#pragma once

#include "Callbacks.h"
#include "InetAddress.h"
#include "base/noncopyable.h"
#include "Buffer.h"

#include <memory>

namespace reactor
{
    namespace net
    {
        class Channel;
        class EventLoop;
        class Socket;

        class TcpConnection : noncopyable,
                              public std::enable_shared_from_this<TcpConnection>
        {
        public:
            TcpConnection(EventLoop* loop, const std::string& name, int connfd,
                          const InetAddress& localAddr, const InetAddress& peeraddr);
            ~TcpConnection();

            EventLoop* getLoop() const { return loop_; }
            const std::string& name() { return name_; }
            const InetAddress& localAddress() { return localAddr_; }
            const InetAddress& peerAddress() { return peerAddr_; }
            bool connected() const { return state_ == kConnected; }
            bool disconnected() const { return state_ == kDisconnected; }
            bool disconnecting() const { return state_ == kDisconnecting; }

            void setConnectionCallback(const ConnectionCallback& cb)
            { connectionCallback_ = cb; }

            void setMessageCallback(const MessageCallback& cb)
            { messageCallback_ = cb; }

            void setWriteCompleteCallback(const WriteCompleteCallback& cb)
            { writeCompleteCallback_ = cb;}

            void setHighWaterMarkCallback(const HighWaterMarkCallback& cb, size_t highWaterMark)
            { 
                highwaterMarkCallback_ = cb; 
                highWaterMark_ = highWaterMark;
            }
            

            //TcpServer::removeConnection()
            void setCloseCallback(const CloseCallback& cb)
            { closeCallback_ = cb; } 
           
           
            void send(const std::string& message);
            void send(std::string&& message);
            void send(const void* data, int len);
            void send(const StringPiece& message);
            void send(Buffer* buf);
            void sendInLoop(const StringPiece& message);
            void sendInLoop(const void* data, size_t len);
           
            void shutdown();
            void shutdownInLoop();

            void setTcpNoDelay(bool on);

            void connectEstablished();
            void connectDestroyed();
            
            const char* stateToString() const;
        private:
            enum stateE {kConnecting, kConnected, kDisconnected, kDisconnecting};

            void setState(stateE s) {state_ = s; }

            //给channel设置的回调函数
            void handleRead(Timestamp receiveTime);   
            void handlewrite();
            void handleClose();
            void handleError();

            EventLoop* loop_;
            std::string name_;
            stateE state_;
            
            std::unique_ptr<Socket> socket_;
            std::unique_ptr<Channel> channel_;

            InetAddress localAddr_;
            InetAddress peerAddr_;

            ConnectionCallback connectionCallback_;
            MessageCallback messageCallback_;
            CloseCallback closeCallback_;
            WriteCompleteCallback writeCompleteCallback_;
            HighWaterMarkCallback highwaterMarkCallback_;

            size_t highWaterMark_;
            Buffer inputBuffer_;
            Buffer outputBuffer_;
        
            typedef std::shared_ptr<TcpConnection> TcpConnectionPtr;
        };
    }
}