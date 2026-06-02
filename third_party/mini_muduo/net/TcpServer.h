#pragma once

#include "Callbacks.h"
#include "TcpConnection.h"
#include "base/noncopyable.h"
#include "InetAddress.h"

#include <map>
#include <memory>

namespace reactor
{
    namespace net
    {
        class Acceptor;
        class EventLoop;
        class EventLoopThreadPool;

        class TcpServer : noncopyable
        {
        public:
            typedef std::function<void(EventLoop*)> ThreadInitCallback;
            enum Option
            {
                kNoReusePort,
                KReusePort,
            };

            TcpServer(EventLoop* loop, 
                      const InetAddress& listenAddr,
                      const string& nameArg,
                      Option option = kNoReusePort);
            ~TcpServer();
            
            const string& ipPort() const { return ipPort_; }
            const string& name() const { return name_; }
            EventLoop* getLoop() const { return loop_; }

            void setThreadNum(int numThreads);

            void setThreadInitCallback(const ThreadInitCallback& cb)
            {
                threadInitCallback_ = cb;
            }


            void start(); //multiple start and thread safe
            
            void setConnectionCallback(const ConnectionCallback& cb)
            { connectionCallback_ = cb; } // not thread safe

            void setMessageCallback(const MessageCallback& cb)
            { messageCallback_ = cb; }    // not thread safe

            void setWriteCallback(const WriteCompleteCallback& cb)
            { writeCompleteCallback_ = cb; }

            void setHightWaterMarkCallback(const HighWaterMarkCallback& cb, size_t highWaterMark)
            {
                highWaterMarkCallback_ = cb;
                highWaterMark_ = highWaterMark;
            }
        private:
            void newConnection(int connfd, const InetAddress& peeraddr);    //给Acceptor的，Acceptor创建新连接后，交给TcpServer处理
            void removeConnection(const TcpConnectionPtr& conn);            //给TcpConnection,TcpConnection销毁时，需要通过TcpServer来remove
            void removeConnectionInLoop(const TcpConnectionPtr& conn);      
            
            typedef std::map<std::string, TcpConnectionPtr> ConnectionMap;

            EventLoop* loop_;
            const std::string ipPort_;
            const std::string name_;

            std::unique_ptr<Acceptor> acceptor_;
            std::unique_ptr<EventLoopThreadPool> threadPool_;

            ConnectionCallback connectionCallback_;                         //TcpConnection的各种回调函数，通过TcpServer来绑定给TcpConnection
            MessageCallback messageCallback_;
            WriteCompleteCallback writeCompleteCallback_;
            HighWaterMarkCallback highWaterMarkCallback_;
            ThreadInitCallback threadInitCallback_;

            size_t highWaterMark_;
            bool started_;
            int nextConnId_;
            ConnectionMap connections_;
        };
    }
}