#include "TcpServer.h"
#include "Acceptor.h"
#include "EventLoop.h"
#include "SocketsOps.h"
#include "EventLoopThreadPool.h"

#include <stdio.h>

namespace reactor
{
    namespace net
    {
        TcpServer::TcpServer(EventLoop *loop,
                             const InetAddress &listenAddr,
                             const string &nameArg,
                             Option option)
            : loop_(loop),
              ipPort_(listenAddr.toIpPort()),
              name_(nameArg),
              acceptor_(new Acceptor(loop, listenAddr, option == KReusePort)),
              threadPool_(std::make_unique<EventLoopThreadPool>(loop, name_)),
              connectionCallback_(defaultConnectionCallback),
              messageCallback_(defaultMessageCallback),
              started_(false),
              nextConnId_(1)
        {
            acceptor_->setNewConnectionCallback([this](int sockfd, const InetAddress &listenAddr)
                                                { this->newConnection(sockfd, listenAddr); });
        }

        TcpServer::~TcpServer()
        {
            loop_->assertInLoopThread();
            LOG_TRACE << "TcpServer::~TcpServer [" << name_ << "] destructing";
            started_ = false;

            // destroy all TcpConnection
            for (auto item : connections_)
            {
                TcpConnectionPtr conn = (item.second);
                item.second.reset();
                EventLoop *ioLoop = conn->getLoop();
                ioLoop->runInLoop([conn]()
                                  { conn->connectDestroyed(); });
            }
        }

        void TcpServer::setThreadNum(int numThreads)
        {
            threadPool_->setThreadNum(numThreads);
        }

        void TcpServer::start()
        {
            if (!started_)
            {
                //初始化线程池
                threadPool_->start(threadInitCallback_);

                assert(!acceptor_->listening());
                loop_->runInLoop([acceptor = acceptor_.get()]()
                                 { acceptor->listen(); });
            }
        }

        // 设置TcpConnection初始化的各个参数
        // 把TcpConnection加到map里面
        // 给TcpConnection绑定回调函数
        // 调用TcpConnection的connectEstablished()
        void TcpServer::newConnection(int connfd, const InetAddress &peerAddr)
        {

            loop_->assertInLoopThread();
            EventLoop *ioLoop = threadPool_->getNextLoop();
            char buf[32];
            snprintf(buf, sizeof buf, "#%d", nextConnId_);
            ++nextConnId_;

            std::string connName = name_ + buf;

            InetAddress localAddress(sockets::getLocalAddr(connfd));
            InetAddress peerAddrress(sockets::getPeerAddr(connfd));

            TcpConnectionPtr conn(new TcpConnection(ioLoop, connName, connfd, localAddress, peerAddrress));
            connections_[connName] = conn;
            conn->setConnectionCallback(connectionCallback_);
            conn->setMessageCallback(messageCallback_);
            conn->setCloseCallback([this](const TcpConnectionPtr &tcpConnectionPtr)
                                   { this->removeConnection(tcpConnectionPtr); });
            conn->setWriteCompleteCallback(writeCompleteCallback_);
            conn->setHighWaterMarkCallback(highWaterMarkCallback_, highWaterMark_);
            ioLoop->runInLoop([conn]()
                              { conn->connectEstablished(); });
            LOG_INFO << "TcpServer::newConnection [" << name_
                     << "] - new connection [" << connName
                     << "] from " << peerAddr.toIpPort();
        }

        //
        void TcpServer::removeConnection(const TcpConnectionPtr &conn)
        {
            loop_->runInLoop([this, conn]()
                             { this->removeConnectionInLoop(conn); });
        }

        void TcpServer::removeConnectionInLoop(const TcpConnectionPtr &conn)
        {
            loop_->assertInLoopThread();

            LOG_INFO << "TcpServer::removeConnectionInLoop [" << name_
                     << "] - connection " << conn->name();

            size_t n = connections_.erase(conn->name()); // 先在TcpServer中把map里面的TcpConnection拿掉
            assert(n == 1);
            (void)n;

            EventLoop *ioLoop = conn->getLoop();
            ioLoop->queueInLoop([this, conn]()
                                { conn->connectDestroyed(); }); // 到线程中去remove channel
        }
    }
}