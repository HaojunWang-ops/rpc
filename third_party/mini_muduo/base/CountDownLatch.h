#pragma once

#include "Condition.h"
#include "Mutex.h"

namespace reactor{
    class CountDownLatch : noncopyable
    {
    public:
        explicit CountDownLatch(int count);

        void wait();

        void countDown();

        int getCount() const;
    private:
        mutable MutexLock mutex_;
        Condition condition_ GUARDED_BY(mutex_);
        int count_ GUARDED_BY(mutex_);
    };
}