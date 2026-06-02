#pragma once

#include "noncopyable.h"

struct tcp_info;

namespace reactor
{
    namespace net
    {
        class InetAddress;

        class Socket : noncopyable
        {
        public:
            explicit Socket(int sockfd)
                : sockfd_ (sockfd)
            {}

            ~Socket();

            int fd() const { return sockfd_; }

            bool getTcpInfo(struct tcp_info*) const;
            bool getTcpInfpString(char* buf, int len) const;

            void bindAddress(const InetAddress& localaddr);

            void listen();

            int accept(InetAddress* peeraddr);

            void shundownWrite();

            void setTcpNoDelay(bool on);
            void setReuseAddr(bool on);
            void setReusePort(bool on);
            void setKeepAlive(bool on);
        
        private:
            const int sockfd_;
        };
    }
}