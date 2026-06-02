#include "Condition.h"

#include <errno.h>

namespace reactor{
    bool Condition::waitForSeconds(double seconds){
        struct timespec absttime;

        clock_gettime(CLOCK_REALTIME, &absttime);

        const int kNanoSecondsPerSeconds = 1000000000;
        int64_t nanoseconds = static_cast<int64_t> (seconds * kNanoSecondsPerSeconds);

        absttime.tv_sec += static_cast<time_t> (( absttime.tv_nsec + nanoseconds) / kNanoSecondsPerSeconds);
        absttime.tv_nsec += static_cast<long> ((absttime.tv_nsec + nanoseconds) % kNanoSecondsPerSeconds);

        MutexLock::UnassignGuard ug(mutex_);
        return ETIMEDOUT == pthread_cond_timedwait(&pcond_, mutex_.getPthreadMutex(), &absttime);
    }
}