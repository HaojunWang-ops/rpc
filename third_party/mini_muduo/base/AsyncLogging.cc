#include "AsyncLogging.h"
#include "LogFile.h"
#include "Timestamp.h"

using namespace reactor;

AsyncLogging::AsyncLogging(const string &basename,
                           off_t rollSize,
                           int flushInterval)
    : flushInterval_(flushInterval),
      running_(false),
      basename_(basename),
      rollSize_(rollSize),
      thread_([this]()
              { this->threadFunc(); }, "Logging"),
      latch_(1),
      mutex_(),
      cond_(mutex_),
      currentBuffer_(new Buffer),
      nextBuffer_(new Buffer),
      buffers_()
{
    currentBuffer_->bzero();
    nextBuffer_->bzero();
    buffers_.reserve(16);
}

void AsyncLogging::append(const char *logline, int len)
{
    MutexLockGuard lock(mutex_);
    if (currentBuffer_->avail() > len)
    {
        currentBuffer_->append(logline, len);
    }
    else
    {
        buffers_.push_back(std::move(currentBuffer_));

        if (nextBuffer_)
        {
            currentBuffer_ = std::move(nextBuffer_);
        }
        else
        {
            currentBuffer_.reset(new Buffer);
        }
        currentBuffer_->append(logline, len);
        cond_.notify();
    }
}

void AsyncLogging::threadFunc()
{
    assert(running_ == true);
    LogFile output(basename_, rollSize_, false);
    latch_.countDown();  
    BufferPtr newBuffer1(new Buffer);
    BufferPtr newBuffer2(new Buffer);
    newBuffer1->bzero();
    newBuffer2->bzero();
    BufferVector buffersToWrite;
    buffersToWrite.reserve(16);
    while (running_)
    {
        assert(newBuffer1 && newBuffer1->length() == 0);
        assert(newBuffer2 && newBuffer2->length() == 0);
        assert(buffersToWrite.empty());

        {
            MutexLockGuard lock(mutex_);
            if (buffers_.empty())
            {
                cond_.waitForSeconds(flushInterval_);
            }

            buffersToWrite.swap(buffers_);
            buffersToWrite.push_back(std::move(currentBuffer_));  //不管currentBUffer有没有写完，都要加到buffersToWrite中

            //只会有currentBuffer_ 和 nextBuffer_ 等待赋值
            currentBuffer_ = std::move(newBuffer1);
            if (!nextBuffer_)
            {
                nextBuffer_ = std::move(newBuffer2);
            }
        }

        assert(!buffersToWrite.empty());

        //前端append太多，只保留前两个Buffer
        if (buffersToWrite.size() >= 25)
        {
            char buf[100];
            int n = snprintf(buf, sizeof buf, "Dropped log messages at %s, %zd larger buffers\n",
                     Timestamp::now().toFormattedString().c_str(),
                     buffersToWrite.size() - 2);
            fputs(buf, stderr);
            if (n >= 0)
            {
                size_t len = sizeof buf;
                if (static_cast<size_t>(n) < len)
                {
                    output.append(buf, n);
                }
                else
                {
                    output.append(buf, len - 1);
                }
            }
            buffersToWrite.erase(buffersToWrite.begin() + 2, buffersToWrite.end());
        }

        for (const auto& buffer : buffersToWrite)
        {
            output.append(buffer->data(), buffer->length());
        }
        
        if (buffersToWrite.size() > 2)
        {
            buffersToWrite.resize(2);
        }

        if (!newBuffer1)
        {
            newBuffer1 = std::move(buffersToWrite.back());
            buffersToWrite.pop_back();
            //FixedBuffer的reset函数，将指针拿到FixedBuffer的最前面
            newBuffer1->reset();
        }
        if (!newBuffer2)
        {
            newBuffer2 = std::move(buffersToWrite.back());
            buffersToWrite.pop_back();
            newBuffer2->reset();
        }

        buffersToWrite.clear();
        output.flush();
    }
    output.flush();
}