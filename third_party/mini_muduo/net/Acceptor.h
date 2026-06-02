#pragma once

#include "Channel.h"
#include "Socket.h"

#include <functional>

namespace reactor
{
    namespace net
    {
        class EventLoop;
        class InetAddress;

        class Acceptor : noncopyable
        {
        public:
            typedef std::function<void (int connfd, const InetAddress& addr)> NewConnectionCallback;            

            Acceptor(EventLoop* loop, const InetAddress& listenAddr, bool reuseport);
            ~Acceptor();

            void setNewConnectionCallback(const NewConnectionCallback& cb)
            { newConnectionCallback_ = cb; }

            void listen();

            bool listening() const { return listening_; }

        private:
            void handleRead(Timestamp receivetime);
            EventLoop* loop_;
            Socket acceptSocket_;
            Channel acceptChannel_;
            NewConnectionCallback newConnectionCallback_;
            bool listening_;
            int idleFd_;
        };
    }
}

