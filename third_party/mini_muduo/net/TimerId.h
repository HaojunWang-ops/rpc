#pragma once

namespace reactor
{
    namespace net
    {
        class Timer;

        class TimerId
        {
        public:
            explicit TimerId()
                : value_(nullptr),
                  sequence_(0)
            {
            }
            
            explicit TimerId(Timer *timer, int64_t sequence)
                : value_(timer),
                  sequence_(sequence)
            {
            }

            friend class TimerQueue;
        private:
            Timer* value_;
            const int64_t sequence_;
        };
    }
}