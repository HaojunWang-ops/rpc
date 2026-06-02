#pragma once

#include "Mutex.h"

#include <pthread.h>

namespace reactor{

    class Condition : noncopyable
    {
    public:
        explicit Condition(MutexLock& mutex)
            :mutex_ (mutex)
        {
            MCHECK(pthread_cond_init(&pcond_, NULL));
        }

        ~Condition()
        {
            MCHECK(pthread_cond_destroy(&pcond_));
        }

        void wait()
        {
            //pthread_cond_wait的执行步骤
            //1.加入条件变量的等待队列并释放互斥锁(原子操作)
            //2.线程进入休眠(阻塞等待)
            //3.接到唤醒信号(正常唤醒 虚假唤醒 被取消pthread_cancel)
            //4.重新竞争并获取互斥锁(可能会阻塞在互斥锁的等待队列中)

            MutexLock::UnassignGuard ug(mutex_);    //保持mutex_ 和 holder_ 的统一
            MCHECK(pthread_cond_wait(&pcond_, mutex_.getPthreadMutex()));
        }

        bool waitForSeconds(double seconds);

        void notify()
        {
            MCHECK(pthread_cond_signal(&pcond_));
        }

        void notifyAll()
        {
            MCHECK(pthread_cond_broadcast(&pcond_));
        }
    private:
        MutexLock& mutex_;
        pthread_cond_t pcond_;
    };
}