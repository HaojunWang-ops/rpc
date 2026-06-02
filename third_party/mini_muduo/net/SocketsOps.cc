#include "SocketsOps.h"
#include "Logging.h"
#include "Types.h"
#include "Endian.h"

#include <fcntl.h>
#include <arpa/inet.h>
#include <sys/uio.h>

using namespace reactor;
using namespace reactor::net;

namespace
{
    #if VALGRIND || defined (NO_ACCEPT4)
    void setNonblockAndCloseOnExec(int sockfd)
    {
        //set O_NONBLOCK
        int flags = ::fcntl(sockfd, F_GETFL, 0);
        if (flags == -1){
            LOG_FATAL << "fcntl error";
        }

        flags |= O_NONBLOCK;

        if (::fcntl(sockfd, F_SETFL, flags) == -1)
        {
            LOG_FATAL << "fcntl error";
        }

        //set FD_CLOEXEC
        flags = ::fcntl(sockfd, F_GETFD, 0);
        if (flags == -1){
            LOG_FATAL << "fcntl error";
        }
        lags |= FD_CLOEXEC;
        if (::fcntl(sockfd, F_SETFD, flags) == -1)
        {
            LOG_FATAL << "fcntl error";
        }
    }
    #endif
}

const struct sockaddr* sockets::sockaddr_cast(const struct sockaddr_in* addr)
{
    return static_cast<const struct sockaddr*> (implicit_cast<const void*>(addr));
}

const struct sockaddr* sockets::sockaddr_cast(const struct sockaddr_in6* addr)
{
    return static_cast<const struct sockaddr*> (implicit_cast<const void*> (addr));
}

struct sockaddr* sockets::sockaddr_cast(struct sockaddr_in6* addr)
{
    return static_cast<struct sockaddr*> (implicit_cast<void*> (addr));
}
struct sockaddr* sockets::sockaddr_cast(struct sockaddr_in* addr)
{
    return static_cast<struct sockaddr*> (implicit_cast<void*> (addr));
}
const struct sockaddr_in* sockets::sockaddr_in_cast(const struct sockaddr* addr)
{
    return static_cast<const struct sockaddr_in*> (implicit_cast<const void*> (addr));
}

const struct sockaddr_in6* sockets::sockaddr_in6_cast(const struct sockaddr* addr)
{
    return static_cast<const struct sockaddr_in6*> (implicit_cast<const void*> (addr));
}

int sockets::createNonblockingOrDie(sa_family_t family)
{
    #if VALGRIND
        int sockfd = ::socket(family, SOCK_STREAM, IPPROTO_TCP);
        if (sockfd < 0)
        {
            LOG_FATAL << "sockets::createNomblockingOrDie error, at socket";
        }
        
        setNonblockAndCloseOnExec(sockfd);
    #else
        int sockfd = ::socket(family, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_TCP);
        if (sockfd < 0)
        {
            LOG_SYSFATAL << "sockets::createNonblockingOrDie error, at socket";
        }
    #endif
        return sockfd;
}

int sockets::connect(int sockfd, const struct sockaddr* addr)
{
    socklen_t addrlen = static_cast <socklen_t> (sizeof (struct sockaddr_in6));
    return ::connect(sockfd, addr, addrlen);
}
void sockets::bindOrDie(int sockfd, const struct sockaddr* addr)
{
    socklen_t addrlen = static_cast <socklen_t> (sizeof (struct sockaddr_in6));
    int ret = ::bind(sockfd, addr, addrlen);
    if (ret < 0)
    {
        LOG_SYSFATAL << "bindOrDie error, at bind";
    }
}
void sockets::listenOrDie(int sockfd)
{
    int ret = ::listen(sockfd, SOMAXCONN);
    if (ret < 0)
    {
        LOG_SYSFATAL << "listenOrDie error, at listen";
    }
}

//accept需要写入，传入参数用sockaddr_in6*,内存足够大
int sockets::accept(int sockfd, struct sockaddr_in6* addr)
{
        socklen_t addrlen = static_cast<socklen_t> (sizeof *addr);
    #if VALGRIND || defined(NO_ACCEPT4)
        int connfd = ::accept(sockfd, sockets::sockaddr_cast(addr), &addrlen);
        setNonblockAndCloseOnExec(connfd);
    #else
        int connfd = ::accept4(sockfd, sockets::sockaddr_cast(addr), &addrlen, SOCK_NONBLOCK | SOCK_CLOEXEC);
    #endif 
        if (connfd < 0)
        {
            int savedErrno = errno;
            if (errno != EAGAIN && errno != EWOULDBLOCK)
            {
                LOG_SYSERR << "accept error";
            }
            switch (savedErrno)
            {
                case EAGAIN:
                case ECONNABORTED:
                case EINTR:
                case EPROTO:
                case EPERM:
                case EMFILE:
                    errno = savedErrno;
                    break;
                
                case EBADF:
                case EFAULT:
                case EINVAL:
                case ENFILE:
                case ENOBUFS:
                case ENOMEM:
                case ENOTSOCK:
                case EOPNOTSUPP:
                    LOG_FATAL << "unexpected error of ::accept " << savedErrno;
                    break;
                
                default:
                    LOG_FATAL << "unknown error of ::accept " << savedErrno;
                    break;
            }
        }
        return connfd;
}

ssize_t sockets::read(int sockfd, void* buf, size_t count)
{
 return ::read(sockfd, buf, count);   
}

ssize_t sockets::readv(int sockfd, const struct iovec* iov, int iovcnt)
{
    return ::readv(sockfd, iov, iovcnt);
}
ssize_t sockets::write(int sockfd, const void* buf, size_t count)
{
    return ::write(sockfd, buf, count);
}

void sockets::close(int sockfd)
{
    if (::close(sockfd) < 0)
    {
        LOG_SYSERR << "sockets::close error";
    }
}
void sockets::shutdownWrite(int sockfd)
{
    if (::shutdown(sockfd, SHUT_WR)< 0)
    {
        LOG_SYSERR << "sockets::shutdownWrite error";
    }
}

//获取IP、port
void sockets::toIpPort(char* buf, size_t size, const struct sockaddr* addr)
{
    if (addr->sa_family == AF_INET)
    {
        sockets::toIp(buf, size, addr);
        const struct sockaddr_in* AF = sockets::sockaddr_in_cast(addr);
        size_t end = strlen(buf);
        assert(size > end);
        uint16_t port = sockets::networkToHost16(AF->sin_port);
        snprintf(buf + end, size - end, ":%hu", port);
        return;
    }
    *buf = '[';
    sockets::toIp(buf + 1, size - 1, addr);
    const struct sockaddr_in6* AF = sockets::sockaddr_in6_cast(addr);
    size_t end = strlen(buf);
    assert(size > end);
    uint16_t port = sockets::networkToHost16(AF->sin6_port);
    snprintf(buf + end, size - end, "]:%hu", port);
}

void sockets::toIp(char* buf, size_t size, const struct sockaddr* addr)
{
    if (addr->sa_family == AF_INET)
    {
        const struct sockaddr_in* AF = sockets::sockaddr_in_cast(addr);
        assert(size >= INET_ADDRSTRLEN);
        socklen_t len = static_cast<socklen_t> (size);
        if (::inet_ntop(AF_INET, &(AF->sin_addr), buf, len) == NULL)
        {
            LOG_SYSERR << "toIP error, at inet_ntop";
        }
        return;
    }
    const struct sockaddr_in6* AF = sockets::sockaddr_in6_cast(addr);
    assert(size >= INET6_ADDRSTRLEN);
    socklen_t len = static_cast<socklen_t> (size);
    if (::inet_ntop(AF_INET6, &(AF->sin6_addr), buf, len) == NULL)
    {
        LOG_SYSERR << "toIp error, at inet_ntop";
    }
}

//设置结构的ip port
void sockets::fromIpPort(const char* ip, uint16_t port, struct sockaddr_in* addr)
{
    addr->sin_family = AF_INET;
    if (::inet_pton(AF_INET, ip, &(addr->sin_addr)) <= 0)
    {
        LOG_SYSERR << "fromIpPort(sockaddr_in) error, at inet_pton";
    }
    addr->sin_port = sockets::hostToNetwork16(port);
}

void sockets::fromIpPort(const char* ip, uint16_t port, struct sockaddr_in6* addr)
{
    addr->sin6_family = AF_INET6;
    if (::inet_pton(AF_INET6, ip, &(addr->sin6_addr)) <= 0)
    {
        LOG_SYSERR << "fromIpPort(sockaddr_in6) error, at inet_pton";
    }
    addr->sin6_port = sockets::hostToNetwork16(port);
}

int sockets::getSocketError(int sockfd)
{
    int optval;
    socklen_t optlen = static_cast<socklen_t> (sizeof(int));
    if (::getsockopt(sockfd, SOL_SOCKET, SO_ERROR, &optval, &optlen) < 0)
    {
        return errno;
    }
    else
    {
        return optval;
    }
}

//只需要sockaddr_in6, sockaddr_in6结构体更大，也能够装得下sockaddr_in
struct sockaddr_in6 sockets::getLocalAddr(int sockfd)
{
    struct sockaddr_in6 addr;
    memZero(&addr, sizeof addr);
    socklen_t len = static_cast<socklen_t> (sizeof addr);
    if (::getsockname(sockfd, sockets::sockaddr_cast(&addr), &len) < 0)
    {
        LOG_SYSERR<< "getLocalAddr error, at getsockname";
    }
    return addr;
}
struct sockaddr_in6 sockets::getPeerAddr(int sockfd)
{
    struct sockaddr_in6 addr;
    memZero(&addr, sizeof addr);
    socklen_t len = static_cast<socklen_t> (sizeof addr);
    if (::getpeername(sockfd, sockets::sockaddr_cast(&addr), &len) < 0)
    {
        LOG_SYSERR << "getPeerAddr error, at getsockname";
    }
    return addr;
}

bool isSelfConnect(int sockfd)
{
    struct sockaddr_in6 localaddr = sockets::getLocalAddr(sockfd);
    struct sockaddr_in6 peeraddr = sockets::getPeerAddr(sockfd);

    if (localaddr.sin6_family == AF_INET)
    {
        struct sockaddr_in* localaddr4 = reinterpret_cast<sockaddr_in*> (&localaddr);
        struct sockaddr_in* peeraddr4 = reinterpret_cast<sockaddr_in*> (&peeraddr);

        if ((localaddr4->sin_port == peeraddr4->sin_port) && 
            (localaddr4->sin_addr.s_addr == peeraddr4->sin_addr.s_addr)){
            return true;
        }
        else
        {
            return false;
        }
    }
    else if(localaddr.sin6_family == AF_INET6)
    {
        return((localaddr.sin6_port == peeraddr.sin6_port) && 
                (memcmp(&localaddr.sin6_addr, &peeraddr.sin6_addr, sizeof localaddr.sin6_addr) == 0));
    }
    else
    {
        return false;
    }
}
