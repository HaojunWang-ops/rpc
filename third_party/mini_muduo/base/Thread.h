#pragma once

#include "Atomic.h"
#include "CountDownLatch.h"
#include "Types.h"

#include <functional>
#include <memory>
#include <pthread.h>

namespace reactor{

    class Thread
    {
    public:
        typedef std::function<void ()> ThreadFunc;

        explicit Thread(ThreadFunc, const string& name = string());
        ~Thread();

        Thread(const Thread&) = delete;
        Thread& operator=(const Thread&) = delete;

        Thread(Thread&&) = delete;
        Thread& operator=(Thread&&) = delete;

        void start();
        int join();

        bool started() const { return started_; }
        pid_t tid() const{ return tid_; }
        const string& name() const  {return name_; }

        static int numCreated() { return numCreated_.get(); }

    private:
        void setDefaultName();

        bool started_;
        bool joined_;
        pthread_t pthreadId_;  //pthread_self(), 线程的句柄
        pid_t tid_;            //syscall(SYS_gettid)
        ThreadFunc func_;
        string name_;
        CountDownLatch latch_;

        static AtomicInt32 numCreated_;
    };
}