#pragma once

#include "Timestamp.h"
#include "Callbacks.h"
#include <Atomic.h>

#include <functional>

namespace reactor
{
    namespace net
    {
        class Timer
        {
        public:
            Timer(const TimerCallback &callback, Timestamp when, double interval)
                : callback_(callback),
                  expiration_(when),
                  interval_(interval),
                  repeat_(interval > 0.0),
                  sequence_(s_numCreated_.incrementAndGet())
            {
            }

            void run() const
            {
                callback_();
            }

            Timestamp expiration()
            {
                return expiration_;
            }
            double interval()
            {
                return interval_;
            }
            bool repeat()
            {
                return repeat_;
            }

            void restart(Timestamp now);

            int64_t sequence()
            {
                return sequence_;
            }

        private:
            const TimerCallback callback_;
            Timestamp expiration_;
            const double interval_;
            const bool repeat_;

            const int64_t sequence_;

            static AtomicInt64 s_numCreated_;
        };
    }
}