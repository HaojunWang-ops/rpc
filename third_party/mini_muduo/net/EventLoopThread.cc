#include "EventLoopThread.h"
#include "EventLoop.h"

#include <functional>

namespace reactor
{
    namespace net
    {
        EventLoopThread::EventLoopThread(const ThreadInitCallback &cb, const string &name)
            : loop_(NULL),
              exiting_(false),
              thread_([this]()
                      { this->threadFunc(); }, name),
              mutex_(),
              cond_(mutex_),
              cb_(cb)
        {
        }
        EventLoopThread::~EventLoopThread()
        {
            exiting_ = true;
            loop_->quit();
            thread_.join();
        }

        // use CountDownLatch to simply ?
        EventLoop *EventLoopThread::startLoop()
        {
            assert(!thread_.started());
            thread_.start();

            {
                MutexLockGuard lock(mutex_);
                while (loop_ == NULL)
                {
                    cond_.wait();
                }
            }
            //把loop_给EventLoopThreadPool
            return loop_;
        }


        //给Thread用的，Thread start后运行这个函数
        void EventLoopThread::threadFunc()
        {
            EventLoop loop; //loop已经和当前线程绑定了
            
            if (cb_)
            {
                cb_(&loop);
            }

            {
                MutexLockGuard lock(mutex_);
                loop_ = &loop;
                cond_.notifyAll();
            }
            loop.loop();
        }
    }
}