#pragma once 

#if defined(__clang__)
    // 如果是 Clang 编译器，启用线程安全分析
    #define THREAD_ANNOTATION_ATTRIBUTE__(x)   __attribute__((x))
#else
    // 如果是 GCC 等其他编译器，直接将宏替换为空，保证顺利编译
    #define THREAD_ANNOTATION_ATTRIBUTE__(x)   
#endif

#define GUARDED_BY(x) THREAD_ANNOTATION_ATTRIBUTE__(guarded_by(x))

#include "CurrentThread.h"
#include "noncopyable.h"

#include <assert.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>

static inline void mcheck_fail_(int rc,
                                const char* call,
                                const char* file,
                                int line,
                                const char* func)
{
    fprintf(stderr, "%s:%d:%s: %s failed, rc = %d\n",
            file, line, func, call, rc);
    abort();
}

#define MCHECK(call)                    \
    do {                                \
        int rc = (call);                \
        if (rc != 0)                    \
            mcheck_fail_(rc, #call, __FILE__, __LINE__, __func__); \
    } while (0)

namespace reactor{
    class MutexLock : noncopyable
    {
    public: 
        MutexLock()
            :holder_ (0)
        {
            MCHECK(pthread_mutex_init(&mutex_, NULL));
        }

        ~MutexLock()
        {
            assert(holder_ == 0);
            MCHECK(pthread_mutex_destroy(&mutex_));
        }

        bool isLockedbyThisThread() const
        {
            return holder_ == CurrentThread::tid();
        }

        void assertLocked() const
        {
            assert(isLockedbyThisThread());
        }

        void lock()
        {
            MCHECK(pthread_mutex_lock(&mutex_));
            assignHolder();
        }

        void unlock()
        {
            unassignHolder();
            MCHECK(pthread_mutex_unlock(&mutex_));
        }

        pthread_mutex_t* getPthreadMutex()
        {
            return &mutex_;
        }
    private:
        friend class Condition;
        
        class UnassignGuard : noncopyable   //RALL, 自动实现holder_的所有权处理
        {
        public:
            explicit UnassignGuard(MutexLock& owner)
                : owner_ (owner)
            {
                owner_.unassignHolder();
            }

            ~UnassignGuard()
            {
                owner_.assignHolder();
            }

        private:
            MutexLock& owner_;    
        };

        
        void unassignHolder()
        {
            holder_ = 0;
        }

        void assignHolder()
        {
            holder_ = CurrentThread::tid();
        }

        pthread_mutex_t mutex_;
        pid_t holder_;
    };


    class MutexLockGuard : noncopyable
    {
    public:
        explicit MutexLockGuard(MutexLock& mutex)
            : mutex_(mutex)
        {
            mutex_.lock();
        }

        ~MutexLockGuard()
        {
            mutex_.unlock();
        }

    private:
        MutexLock& mutex_;
    };
}