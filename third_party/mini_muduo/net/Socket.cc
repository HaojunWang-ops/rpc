#include "Socket.h"

#include "Logging.h"
#include "InetAddress.h"
#include "SocketsOps.h"

#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdio.h>

using namespace reactor;
using namespace reactor::net;

//socket真正拥有fd，负责close
Socket::~Socket()
{
    sockets::close(sockfd_);
}

bool Socket::getTcpInfo(struct tcp_info* tcpi) const
{
    socklen_t len = sizeof(*tcpi);  //求结构体size
    memZero(tcpi, len);             //清零结构体
    return ::getsockopt(sockfd_, SOL_TCP, TCP_INFO, tcpi, &len);
}

bool Socket::getTcpInfpString(char* buf, int len) const
{
    struct tcp_info tcpi;
    bool ok = getTcpInfo(&tcpi);
    if (ok)
    {
        snprintf(buf, len, "unrecovered=%u "
             "rto=%u ato=%u snd_mss=%u rcv_mss=%u "
             "lost=%u retrans=%u rtt=%u rttvar=%u "
             "sshthresh=%u cwnd=%u total_retrans=%u",
             tcpi.tcpi_retransmits,  // Number of unrecovered [RTO] timeouts
             tcpi.tcpi_rto,          // Retransmit timeout in usec
             tcpi.tcpi_ato,          // Predicted tick of soft clock in usec
             tcpi.tcpi_snd_mss,
             tcpi.tcpi_rcv_mss,
             tcpi.tcpi_lost,         // Lost packets
             tcpi.tcpi_retrans,      // Retransmitted packets out
             tcpi.tcpi_rtt,          // Smoothed round trip time in usec
             tcpi.tcpi_rttvar,       // Medium deviation
             tcpi.tcpi_snd_ssthresh,
             tcpi.tcpi_snd_cwnd,
             tcpi.tcpi_total_retrans);  // Total retransmits for entire connection
    }
    return ok;
}

void Socket::bindAddress(const InetAddress& addr)
{
    sockets::bindOrDie(sockfd_, addr.getSockAddr());
}

void Socket::listen()
{
    sockets::listenOrDie(sockfd_);
}

int Socket::accept(InetAddress* peeraddr)
{
    struct sockaddr_in6 addr;
    memZero(&addr, sizeof addr);
    int connfd = sockets::accept(sockfd_, &addr);
    if (connfd >= 0)
    {
        peeraddr->setSockAddrInet6(addr);
    }
    return connfd;
}

void Socket::shundownWrite()
{
    sockets::shutdownWrite(sockfd_);
}

void Socket::setTcpNoDelay(bool on)
{
#ifdef TCP_NODELAY
    int optval = on ? 1 : 0;
    int ret = ::setsockopt(sockfd_, IPPROTO_TCP, TCP_NODELAY,
                 &optval, static_cast<socklen_t> (sizeof optval));
    if (ret < 0 && on)
    {
        LOG_SYSERR << "TCP_NONDELAY failed";
    }
#else
    if (on)
    {
        LOG_ERROR << "TCP_NONDELAY is not supported";
    }
#endif
}

void Socket::setReuseAddr(bool on)
{
#ifdef SO_REUSEADDR
    int optval = on ? 1 : 0;
    int ret = ::setsockopt(sockfd_, SOL_SOCKET, SO_REUSEADDR,
                 &optval, static_cast<socklen_t> (sizeof optval));
    if (ret < 0 && on)
    {
        LOG_SYSERR << "SO_REUSEADDR failed";
    }
#else
    if (on)
    {
        LOG_ERROR << "SO_REUSEADDR is not supported";
    }
#endif
}


void Socket::setReusePort(bool on)
{
#ifdef SO_REUSEPORT
    int optval = on ? 1 : 0;
    int ret = ::setsockopt(sockfd_, SOL_SOCKET, SO_REUSEADDR,
                 &optval, static_cast<socklen_t> (sizeof optval));
    if (ret < 0 && on)
    {
        LOG_SYSERR << "SO_REUSEPORT failed";
    }
#else
    if (on)
    {
        LOG_ERROR << "SO_REUSEPORT is not supported";
    }
#endif
}

void Socket::setKeepAlive(bool on)
{
#ifdef SO_KEEPALIVE
    int optval = on ? 1 : 0;
    int ret = ::setsockopt(sockfd_, SOL_SOCKET, SO_KEEPALIVE,
                 &optval, static_cast<socklen_t> (sizeof optval));
    if (ret < 0 && on)
    {
        LOG_ERROR << "SO_KEEPALIVE failed";
    }
#else
    if (on)
    {
        LOG_ERROR << "SO_KEEPALIVE is not supported";
    }
#endif
}