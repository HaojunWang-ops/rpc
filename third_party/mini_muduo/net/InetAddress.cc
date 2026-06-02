#include "InetAddress.h"

#include "Logging.h"
#include "Endian.h"
#include "SocketsOps.h"

#include <netdb.h>
#include <netinet/in.h>
#include <memory>
#include <string.h>
#include <assert.h>

#pragma GCC diagnostic ignored "-Wold-style-cast"
static const in_addr_t kInaddrAny = INADDR_ANY;
static const in_addr_t kInaddrLoopbacn = INADDR_LOOPBACK;
#pragma GCC diagnostic error "-Wold-style-cast"

using namespace reactor;
using namespace reactor::net;

static_assert(sizeof(InetAddress) == sizeof(struct sockaddr_in6), 
              "InetAddress is same size as sockaddr_in6");
static_assert(offsetof(sockaddr_in, sin_family) == 0, "sin_family offset 0");
static_assert(offsetof(sockaddr_in6, sin6_family) == 0, "sin6_family offset 0");
static_assert(offsetof(sockaddr_in, sin_port) == 2, "sin_port offset 2");
static_assert(offsetof(sockaddr_in6, sin6_port) == 2, "sin6_port offset 2");

InetAddress::InetAddress(uint16_t portArg, bool loopbackOnly, bool ipv6)
{
    static_assert(offsetof(InetAddress, addr6_) == 0, "addr6_ offset 0");
    static_assert(offsetof(InetAddress, addr_) == 0, "addr_ offset 0");

    if (ipv6)
    {
        memZero(&addr6_, sizeof addr6_);
        addr6_.sin6_family = AF_INET6;
        in6_addr ip = loopbackOnly ? in6addr_loopback : in6addr_any;
        addr6_.sin6_addr = ip;
        addr6_.sin6_port = sockets::hostToNetwork16(portArg);
    }
    else
    {
        memZero(&addr_, sizeof addr_);
        addr_.sin_family = AF_INET;
        in_addr_t ip = loopbackOnly ? kInaddrLoopbacn : kInaddrAny;
        addr_.sin_addr.s_addr = sockets::hostToNetwork32(ip);
        addr_.sin_port = sockets::hostToNetwork16(portArg);
    }
}

InetAddress::InetAddress(StringArg ip, uint16_t portArg, bool ipv6)
{
    if (ipv6 || strchr(ip.c_str(), ':'))
    {
        memZero(&addr6_, sizeof addr6_);
        sockets::fromIpPort(ip.c_str(), portArg, &addr6_);
    }
    else{
        memZero(&addr_, sizeof addr_);
        sockets::fromIpPort(ip.c_str(), portArg, &addr_);
    }
}

string InetAddress::toIpPort() const
{
    char buf[64] = "";
    sockets::toIpPort(buf, sizeof(buf), getSockAddr());
    return buf;
}

string InetAddress::toIp() const
{
    char buf[64] = "";
    sockets::toIp(buf, sizeof buf, getSockAddr());
    return buf;
}

uint32_t InetAddress::ipv4NetEndian() const
{
    assert(family() == AF_INET);
    return addr_.sin_addr.s_addr;
}

uint16_t InetAddress::port() const
{
    return sockets::networkToHost16(portNetEndian());
}


//getaddrinfo() instead of gethostbyname_r()
bool InetAddress::resolve(StringArg hostname, InetAddress* out)
{
    assert(out != NULL);
    struct addrinfo hints;
    memZero(&hints, sizeof hints);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = 0; //hints.ai_socktype = SOCK_STREAM只负责TCP

    struct addrinfo* result = nullptr;
    //将人类可读的主机名和服务名，转换成可供bind或connect等系统调用直接使用的套接字地址结构链表
    int ret = getaddrinfo(hostname.c_str(), nullptr, &hints, &result);
    if (ret != 0)
    {
        LOG_SYSERR << "InetAddress::resolve error " << hostname.c_str() << " " << gai_strerror(ret);
        return false;
    }
    
    //freeaddrinfo来释放addrinfo
    std::unique_ptr<struct addrinfo, decltype(&freeaddrinfo) > addr_ptr(result, freeaddrinfo);

    for (struct addrinfo* rp = addr_ptr.get(); rp!= nullptr; rp = rp->ai_next)
    {
        if (rp->ai_family == AF_INET)
        {
            out->addr_.sin_family =AF_INET;
            out->addr_.sin_addr = reinterpret_cast<struct sockaddr_in*>(rp->ai_addr)->sin_addr;
            return true;
        }
        else if (rp->ai_family == AF_INET6)
        {
            const struct sockaddr_in6* addr = reinterpret_cast<struct sockaddr_in6*> (rp->ai_addr);
            uint16_t oldPort = out->addr6_.sin6_port;

            memZero(out, sizeof out);
            out->addr6_.sin6_family = AF_INET6;
            out->addr6_.sin6_addr = reinterpret_cast<struct sockaddr_in6*>(rp->ai_addr)->sin6_addr;
            out->addr6_.sin6_port = oldPort;
            out->addr6_.sin6_scope_id = addr->sin6_scope_id;
            return true;
        }
    }
    return false;
}


void InetAddress::setScopeId(uint32_t scope_id)
{
    if (family() == AF_INET6)
    {
        addr6_.sin6_scope_id = scope_id;
    }
}