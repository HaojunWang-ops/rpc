#pragma once

#include "CountDownLatch.h"
#include "Mutex.h"
#include "Thread.h"
#include "LogStream.h"

#include <atomic>
#include <vector>

namespace reactor
{
    class AsyncLogging : noncopyable
    {
    public:
        AsyncLogging(const string& basename,
                     off_t rollSize,
                     int flushInterval = 3);

        ~AsyncLogging()
        {
            if (running_)
            {
                stop();
            }
        }

        void append(const char* logline, int len);

        void start()
        {
            running_ = true;
            thread_.start();
            latch_.wait();   //等待threadFunc()初始化output
        }

        void stop()
        {
            running_ = false;
            cond_.notify();
            thread_.join();
        }

    private:
        void threadFunc();

        typedef reactor::detail::FixedBuffer<reactor::detail::kLargeBuffer> Buffer;
        typedef std::vector<std::unique_ptr<Buffer>> BufferVector;
        typedef BufferVector::value_type BufferPtr;   //定义BufferVector中元素的类型

        const int flushInterval_;
        std::atomic<bool> running_;
        const string basename_;
        const off_t rollSize_;
        reactor::Thread thread_;
        reactor::CountDownLatch latch_;
        reactor::MutexLock mutex_;
        reactor::Condition cond_ GUARDED_BY(mutex_);
        BufferPtr currentBuffer_ GUARDED_BY(mutex_);
        BufferPtr nextBuffer_ GUARDED_BY(mutex_);
        BufferVector buffers_ GUARDED_BY(mutex_);
    };
}