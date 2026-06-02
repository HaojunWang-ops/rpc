#include "EventLoopThreadPool.h"

#include "EventLoop.h"
#include "EventLoopThread.h"

#include <stdio.h>

using namespace reactor;
using namespace reactor::net;

EventLoopThreadPool::EventLoopThreadPool(EventLoop *baseLoop, const string &nameArg)
    : baseLoop_(baseLoop),
      name_(nameArg),
      started_(false),
      numThreads_(0),
      next_(0)
{
}

EventLoopThreadPool::~EventLoopThreadPool()
{
    // unique_ptr自动析构
    // loop定义在栈上，不需要手动释放
}

void EventLoopThreadPool::start(const ThreadInitCallback& cb)
{
    assert(!started_);
    baseLoop_->assertInLoopThread();
    started_ = true;

    for (int i = 0; i < numThreads_; i++)
    {
        std::string buf;
        buf.resize(name_.size() + 32);
        std::snprintf(buf.data(), buf.size(), "%s%d", name_.c_str(), i);
        auto newLoopThread = std::make_unique<EventLoopThread>(cb, buf.data());
        loops_.push_back(newLoopThread->startLoop());
        threads_.push_back(std::move(newLoopThread));
    }

    if (numThreads_ == 0 && cb)
    {
        cb(baseLoop_);
    }
}
/*
EventLoopThreadPool::start()
    ↓
new EventLoopThread(cb, name)
    ↓
EventLoopThread 构造内部 Thread 成员
    ↓
Thread 保存 threadFunc 和线程名
    ↓
EventLoopThread::startLoop()
    ↓
Thread::start()
    ↓
创建真正的新线程
    ↓
新线程执行 EventLoopThread::threadFunc()
    ↓
在线程里创建 EventLoop loop
    ↓
把 &loop 赋给 loop_
    ↓
通知主线程 startLoop() 可以返回
    ↓
子线程进入 loop.loop()
    ↓
EventLoopThreadPool 保存 EventLoop* 到 loops_
*/
EventLoop* EventLoopThreadPool::getNextLoop()
{
    baseLoop_->assertInLoopThread();
    assert(started_);

    EventLoop* loop = baseLoop_;

    if(!loops_.empty())
    {
        loop = loops_[next_];
        next_++;
        if (next_ == loops_.size())
        {
            next_ = 0;
        }
    }
    return loop;
}

EventLoop* EventLoopThreadPool::getLoopForHash(size_t hashCode)
{
    baseLoop_->assertInLoopThread();
    assert(started_);

    EventLoop* loop = baseLoop_;
    if (!loops_.empty())
    {
        loop = loops_[hashCode % loops_.size()];
    }

    return loop;
}

std::vector<EventLoop*> EventLoopThreadPool::getAllLoops()
{
    baseLoop_->assertInLoopThread();
    assert(started_);

    if (!loops_.empty())
    {
        return loops_;
    }
    else
    {
        return std::vector<EventLoop*> (1, baseLoop_);
    }
}