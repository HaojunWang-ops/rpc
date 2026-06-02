#include "Channel.h"
#include "EventLoop.h"

#include <poll.h>

namespace reactor
{
    namespace net
    {
        const int Channel::kNoneEvent = 0;
        const int Channel::kReadEvent = POLLIN | POLLPRI;
        const int Channel::kWriteEvent = POLLOUT;

        Channel::Channel(EventLoop *loop, int fd)
            : ownerLoop_(loop),
              fd_(fd),
              index_(-1),
              events_(0),
              revents_(0),
              tied_(false),
              eventHandling_(false),
              addedToLoop_(false)
        {
        }

        //Channel不持有fd，不负责fd的close
        //fd的关闭
        //连接 fd：Socket::~Socket()，由 TcpConnection 持有
        //监听 fd：Socket::~Socket()，由 Acceptor 持有
        //eventfd：EventLoop 析构函数里 close
        //timerfd：TimerQueue 析构函数里 close
        Channel::~Channel()
        {
            assert(!eventHandling_);
        }

        void Channel::tie(const std::shared_ptr<void>& obj)
        {
            tie_ = obj;
            tied_ = true;
        }

        void Channel::update()
        {
            addedToLoop_ = true;
            ownerLoop_->updateChannel(this);
        }

        void Channel::remove()
        {   
            assert(isNoneEvent());
            addedToLoop_ = false;
            ownerLoop_->removeChannel(this);
        }

        void Channel::handleEvent(Timestamp receiveTime)
        {
            std::shared_ptr<void> guard;
            if (tied_)
            {
                guard = tie_.lock();
                if (guard)
                {
                    handleEventWithGuard(receiveTime);
                }
            }
            else //除TcpConnection之外，不需要绑定tie的channel
            {
                handleEventWithGuard(receiveTime);
            }
        }

        /*
        POLLIN      可读
        POLLPRI     有紧急数据或带外数据
        POLLOUT     可写
        POLLHUP     对端挂断
        POLLRDHUP   对端关闭写端/半关闭
        POLLERR     错误
        POLLNVAL    非法 fd
        */


        void Channel::handleEventWithGuard(Timestamp receiveTime)
        {
            eventHandling_ = true;

            //只有HUP且没有可读事件
            //关闭连接
            if ((revents_ & POLLHUP) && !(revents_ & POLLIN))
            {
                if (closeCallback_)
                {
                    closeCallback_();
                }
            }

            if (revents_ & POLLNVAL)
            {
                LOG_WARN << "fd = " << fd_ << "Channel::handleEvent() POLLNVAL";
            }

            if (revents_ & (POLLERR | POLLNVAL))
            {
                if (errorCallback_) errorCallback_();
            }

            if (revents_ & (POLLIN | POLLPRI | POLLRDHUP))
            {
                if (readCallback_) readCallback_(receiveTime);
            }

            if (revents_ & POLLOUT)
            {
                if (writeCallback_) writeCallback_();
            }

            eventHandling_ = false;
        }
    }
}