#pragma once

#include "Channel.h"
#include "Timestamp.h"
#include "TimerId.h"
#include "Callbacks.h"

#include <functional>
#include <vector>
#include <set>
#include <utility>

namespace reactor
{
    namespace net
    {
        class Channel;
        class EventLoop;
        class Timer;

        class TimerQueue
        {
        public:
            TimerQueue(EventLoop *loop);
            ~TimerQueue();

            TimerId addTimer(const TimerCallback &callback, Timestamp now, double interval);

            void cancelTimer(TimerId timerId);
        private:
            typedef std::pair<Timestamp, Timer *> Entry;
            typedef std::set<Entry> TimerList;
            typedef std::pair<Timer*, int64_t> ActiveTimer;
            //用set的原因是因为pair支持比较，能够自然放到set里面，不需要写比较器
            //优化可以用unoreded_set 写hash
            //这段功能并不需要set的排序功能
            typedef std::set<ActiveTimer> ActiveTimerSet;

            void addTimerInLoop(Timer *timer);
            void cancelTimerInLoop(TimerId timerId);

            void handleRead(Timestamp receivetime);

            std::vector<Entry> getExpired(Timestamp now);
            void reset(const std::vector<Entry> &expired, Timestamp now);

            bool insert(Timer *timer);

            EventLoop *loop_;
            const int timerfd_;
            Channel timerfdChannel_;

            TimerList timers_;

            ActiveTimerSet activeTimers_;
            bool callingExpiredTimers_;
            ActiveTimerSet cancelingTimers_;
        };
    }
}