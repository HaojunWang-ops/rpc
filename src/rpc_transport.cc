#include "rpc_transport.h"

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <fcntl.h>
#include <poll.h>

RpcTransport::RpcTransport()
{
}

RpcTransport::~RpcTransport()
{
    close();
}

void RpcTransport::setError(std::string *error, const std::string &msg)
{
    if (error)
    {
        *error = msg;
    }
}

bool RpcTransport::connectTo(const std::string &ip,
                             uint16_t port,
                             std::string *error)
{
    int new_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (new_fd < 0)
    {
        setError(error, "create socket failed: " + std::string(::strerror(errno)));
        return false;
    }

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = ::htons(port);

    if (::inet_pton(AF_INET, ip.c_str(), &(addr.sin_addr)) <= 0)
    {
        setError(error, "inet_pton failed: " + std::string(::strerror(errno)));
        ::close(new_fd);
        return false;
    }

    int timeout_ms = connect_timeout_ms_.load(std::memory_order_acquire);

    if (!connectWithTimeout(new_fd, addr, timeout_ms, error))
    {
        int saved_errno = errno;
        setError(error, "connect failed: " + std::string(::strerror(saved_errno)));
        ::close(new_fd);
        errno = saved_errno;
        return false;
    }

    {
        std::lock_guard<std::mutex> lock(fd_mutex_);

        int old_fd = fd_.exchange(new_fd, std::memory_order_acq_rel);
        if (old_fd >= 0)
        {
            ::close(old_fd);
        }
    }
    return true;
}

bool RpcTransport::connectWithTimeout(int fd, const sockaddr_in& addr, int timeout_ms,std::string* error)
{
    int old_flags = ::fcntl(fd, F_GETFL, 0);
    if (old_flags < 0)
    {
        setError(error, "fcntl F_GETFL failed: " + std::string(::strerror(errno)));
        return false;
    }

    if (::fcntl(fd, F_SETFL, old_flags | O_NONBLOCK) < 0)
    {
        setError(error, "fcntl F_SETFL O_NONBLOCK failed: " + std::string (::strerror(errno)));
        return false;
    }

    int ret = ::connect(fd, reinterpret_cast<const sockaddr*> (&addr), sizeof(addr));

    if (ret == 0)
    {
        if (::fcntl(fd, F_SETFL, old_flags) < 0)
        {
            setError(error, "restore socket flags failed: " + std::string (::strerror(errno)));
            return false;
        }

        return true;
    }

    if (errno != EINPROGRESS)
    {
        setError(error, "connect failed: " + std::string (::strerror(errno)));
        return false;
    }

    struct pollfd pfd;
    pfd.fd = fd;
    pfd.events = POLLOUT;
    pfd.revents = 0;

    while (true)
    {
        ret = ::poll(&pfd, 1, timeout_ms);

        if (ret > 0)
        {
            break;
        }

        if (ret == 0)
        {
            setError(error, "connect failed");
            errno = ETIMEDOUT;
            return false;
        }

        if (errno == EINTR)
        {
            continue;
        }

        setError(error, "poll failed during connect: " + std::string(strerror(errno)));
        return false;
    }

    int socket_error = 0;
    socklen_t len = sizeof(socket_error);

    if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &socket_error, &len) < 0)
    {
        setError(error, "getsockopt SO_ERROR failed: " + std::string(::strerror(errno)));
        return false;
    }

    if (socket_error != 0)
    {
        setError(error, "connect failed: " + std::string(::strerror(socket_error)));
        errno = socket_error;
        return false;
    }
    if (::fcntl(fd, F_SETFL, old_flags) < 0)
    {
        setError(error, "restore socket flags failed: " + std::string(::strerror(errno)));
        return false;
    }

    return true;
}

bool RpcTransport::readN(void *buf, size_t n, const std::atomic<bool> &running)
{
    char *p = reinterpret_cast<char *>(buf);
    size_t left = n;

    while (left > 0)
    {
        if (!running.load(std::memory_order_acquire))
        {
            errno = ECANCELED;
            return false;
        }

        int cur_fd = fd_.load(std::memory_order_acquire);
        if (cur_fd < 0)
        {
            errno = ECANCELED;
            return false;
        }

        ssize_t nr = ::read(cur_fd, p, left);

        if (nr > 0)
        {
            p += nr;
            left -= static_cast<size_t>(nr);
        }
        else if (nr == 0)
        {
            return false;
        }
        else
        {
            if (errno == EINTR)
            {
                continue;
            }

            return false;
        }
    }

    return true;
}

bool RpcTransport::writeN(const void *buf, size_t n, const std::atomic<bool> &running)
{
    const char *p = reinterpret_cast<const char *>(buf);
    size_t left = n;

    while (left > 0)
    {
        if (!running.load(std::memory_order_acquire))
        {
            errno = ECANCELED;
            return false;
        }

        int cur_fd = fd_.load(std::memory_order_acquire);
        if (cur_fd < 0)
        {
            errno = ECANCELED;
            return false;
        }

        ssize_t nw = ::write(cur_fd, p, left);

        if (nw > 0)
        {
            p += nw;
            left -= static_cast<size_t>(nw);
        }
        else if (nw == 0)
        {
            errno = EPIPE;
            return false;
        }
        else
        {
            if (errno == EINTR)
            {
                continue;
            }

            return false;
        }
    }

    return true;
}

void RpcTransport::shutdown()
{
    std::lock_guard<std::mutex> lock(fd_mutex_);

    int cur_fd = fd_.load(std::memory_order_acquire);
    if (cur_fd >= 0)
    {
        ::shutdown(cur_fd, SHUT_RDWR);
    }
}

void RpcTransport::close()
{
    std::lock_guard<std::mutex> lock(fd_mutex_);

    int old_fd = fd_.exchange(-1, std::memory_order_acq_rel);
    if (old_fd >= 0)
    {
        ::close(old_fd);
    }
}

int RpcTransport::fd() const
{
    return fd_.load(std::memory_order_acquire);
}

void RpcTransport::setConnectTimeoutMs(int timeout_ms)
{
    connect_timeout_ms_.store(timeout_ms, std::memory_order_release);
}

int RpcTransport::connectTimeoutMs()
{
    return connect_timeout_ms_.load(std::memory_order_acquire);
}
