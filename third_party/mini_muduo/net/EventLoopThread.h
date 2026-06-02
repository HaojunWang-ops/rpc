#pragma once

#include "Mutex.h"
#include "Thread.h"
#include "Condition.h"
#include "noncopyable.h"

#include <string.h>
#include <string>

namespace reactor
{
    namespace net
    {
        class EventLoop;

        class EventLoopThread : noncopyable
        {
        public:
            typedef std::function<void(EventLoop*)> ThreadInitCallback;

            EventLoopThread(const ThreadInitCallback& cb = ThreadInitCallback(), const std::string& name = std::string());
            ~EventLoopThread();
            EventLoop *startLoop();

        private:
            void threadFunc();

            EventLoop *loop_;
            bool exiting_;
            Thread thread_;
            MutexLock mutex_;
            Condition cond_;
            ThreadInitCallback cb_;
        };
    }
}