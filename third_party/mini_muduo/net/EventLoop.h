#pragma once

#include "Logging.h"
#include "CurrentThread.h"
#include "TimerId.h"
#include "Callbacks.h"
#include <Mutex.h>

#include <vector>
#include <memory>

namespace reactor
{
    namespace net
    {
        class Poller;
        class Channel;
        class TimerQueue;

        class EventLoop
        {
        public:
            typedef std::function<void()> Functor;
            EventLoop();
            ~EventLoop();

            EventLoop(const EventLoop &) = delete;
            EventLoop &operator=(const EventLoop &) = delete;

            void loop();
            void quit();

            Timestamp pollReturnTime() { return pollReturnTime_; }

            void assertInLoopThread() const
            {
                if (!isInLoopThread())
                {
                    abortNotInLoopThread();
                }
            }
            bool isInLoopThread() const
            {
                return threadId_ == CurrentThread::tid();
            }

            void wakeup();
            void updateChannel(Channel *channel);
            void removeChannel(Channel *channel);
            bool hasChannel(Channel* channel);

            void runInLoop(const Functor &cb);
            void queueInLoop(const Functor &cb);

            void cancel(TimerId timerId);

            TimerId runAt(const Timestamp &time, const TimerCallback &cb);
            TimerId runAfter(double delay, const TimerCallback &cb);
            TimerId runEvery(double interval, const TimerCallback &cb);

            EventLoop* getEventLoopOfCurrentThread();
        private:
            void abortNotInLoopThread() const;
            void handleRead();
            void deoPendingFunctors();

            typedef std::vector<Channel *> ChannelList;

            bool looping_;
            bool quit_;
            bool eventHandling_;
            bool callingPendingFunctors_;
            const pid_t threadId_;
            
            Timestamp pollReturnTime_;
            std::unique_ptr<Poller> poller_;
            std::unique_ptr<TimerQueue> timerQueue_;
            
            int wakeupFd_;
            std::unique_ptr<Channel> wakeupChannel_;
            ChannelList activeChannels_;
            
            MutexLock mutex_;
            std::vector<Functor> pendingFunctors_ GUARDED_BY(mutex_);

            Channel* currentActiveChannel;
        };
    }
}