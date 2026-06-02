#include "Acceptor.h"

#include "Logging.h"
#include "InetAddress.h"
#include "SocketsOps.h"
#include "Channel.h"
#include "EventLoop.h"

#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <assert.h>

namespace reactor
{
    namespace net
    {
        Acceptor::Acceptor(EventLoop* loop, const InetAddress& listenAddr, bool reuseport)
            : loop_ (loop),
              acceptSocket_(sockets::createNonblockingOrDie(listenAddr.family())),
              acceptChannel_(loop_, acceptSocket_.fd()),
              listening_(false),
              idleFd_(::open("/dev/null", O_RDONLY | O_CLOEXEC))
        {
            assert(idleFd_ > 0);
            acceptSocket_.setReusePort(reuseport);
            acceptSocket_.setReuseAddr(true);
            acceptSocket_.bindAddress(listenAddr);
            acceptChannel_.setReadCallback([this](Timestamp receivetime){
                this->handleRead(receivetime);
            });
        }
        
        Acceptor::~Acceptor()
        {
            acceptChannel_.disableAll();
            loop_->removeChannel(&acceptChannel_);
            ::close(idleFd_);    
        }

        void Acceptor::listen()
        {
            loop_->assertInLoopThread();
            listening_ = true;
            acceptSocket_.listen();
            acceptChannel_.enableReading();
        }

        void Acceptor::handleRead(Timestamp receivetime)
        {
            (void) receivetime;
            loop_->assertInLoopThread();
            while (true)
            {
                InetAddress peeraddr;
                int connfd = acceptSocket_.accept(&peeraddr);
                if (connfd >= 0)
                {
                    if (newConnectionCallback_)
                    {
                        newConnectionCallback_(connfd, peeraddr);
                    }
                    else
                    {
                        sockets::close(connfd);
                    }
                }
                else
                {
                    int savedErrno = errno;
                    if (savedErrno == EAGAIN || savedErrno == EWOULDBLOCK)
                    {
                        break;
                    }
                    else if (savedErrno == EMFILE)
                    {
                        ::close(idleFd_);
                        idleFd_ = acceptSocket_.accept(&peeraddr);
                        ::close(idleFd_);
                        idleFd_ = ::open("/dev/null", O_RDONLY | O_CLOEXEC);
                    }
                    else
                    {
                        LOG_SYSERR << "Acceptor::handleReand() failed, at accept";
                    }
                }  
            }
        }
    }
}