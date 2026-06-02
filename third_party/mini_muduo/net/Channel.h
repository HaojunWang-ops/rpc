#pragma once

#include "base/Timestamp.h"
#include "base/noncopyable.h"

#include <functional>
#include <memory>

namespace reactor
{
    namespace net
    {
        class EventLoop;

        class Channel : noncopyable
        {
        public:
            typedef std::function<void()> Callback;
            typedef std::function<void(Timestamp)> ReadCallback;
            
            Channel(EventLoop *loop, int fd);
            ~Channel();

            void handleEvent(Timestamp receivetime);
            
            void set_revents(int revents);

            void setReadCallback(ReadCallback cb) { readCallback_ = cb; }
            void setWriteCallback(Callback cb) { writeCallback_ = cb; }
            void setErrorCallback(Callback cb) { errorCallback_ = cb; }
            void setCloseCallback(Callback cb) { closeCallback_ = cb; }
            
            void tie(const std::shared_ptr<void>&);

            void enableReading() { events_ |= kReadEvent; update(); }
            void disableRead() { events_ &= ~kReadEvent; update(); }
            void enableWrting() { events_ |= kWriteEvent; update(); }
            void disableWriting() {events_ &= ~kWriteEvent; update(); }
            void disableAll() {
                events_ = kNoneEvent; 
                update(); 
            }
            bool isWriting() const { return events_ & kWriteEvent; }
            bool isReading() const { return events_ & kReadEvent; }

            int fd() const { return fd_; }
            int index() { return index_; }
            int events() const { return events_; }
            EventLoop *ownerLoop() { return ownerLoop_; }
            void set_index(int index) { index_ = index; }
            void set_revent(int revents) { revents_ = revents; }

            bool isNoneEvent() { return events_ == kNoneEvent; }
            
            void remove();
        private:
            void update();
            void handleEventWithGuard(Timestamp receiveTime);

            static const int kNoneEvent;
            static const int kReadEvent;
            static const int kWriteEvent;            
            
            EventLoop *ownerLoop_;
            const int fd_;
            int index_; // used by Poller
            int events_;
            int revents_;

            //weak_ptr来保证使用时TcpConnection还没有析构
            //使用void类型降低channel与TcpConnection的耦合
            std::weak_ptr<void> tie_;
            bool tied_;
            bool eventHandling_;
            bool addedToLoop_;
            
            ReadCallback readCallback_;
            Callback writeCallback_;
            Callback errorCallback_;
            Callback closeCallback_;
        };
    }
}