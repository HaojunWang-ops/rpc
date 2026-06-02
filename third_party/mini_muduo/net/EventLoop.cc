#include "EventLoop.h"
#include "Poller.h"
#include "Channel.h"
#include "CurrentThread.h"
#include "Logging.h"
#include "TimerQueue.h"

#include <signal.h>
#include <assert.h>
#include <sys/eventfd.h>

using namespace reactor;
using namespace reactor::net;

namespace
{
        __thread reactor::net::EventLoop *t_loopInThisThread = NULL;
        const int kPollTimeMs = 1000;

        static int createEventfd()
        {
            int evtfd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
            if (evtfd < 0)
            {
                LOG_SYSERR << "EventLoop::createEventfd error, at eventfd";
                abort();
            }
            return evtfd;
        } 

        #pragma GCC diagnostic ignored "-Wold-style-cast"
        class IgnoreSigPipe
        {
        public:
            IgnoreSigPipe()
            {
                ::signal(SIGPIPE, SIG_IGN);
            }
        };
        #pragma GCC diagnostic error "-Wold-style-cast"

        IgnoreSigPipe initobj;
}
namespace reactor
{
    namespace net
    {
        EventLoop::EventLoop()
            : looping_(false),
              quit_(false),
              eventHandling_(false),
              callingPendingFunctors_(false),
              threadId_(CurrentThread::tid()),
              pollReturnTime_(Timestamp::invalid()),
              poller_(Poller::newDefaultPoller(this)),
              timerQueue_(new TimerQueue(this)),
              wakeupFd_(createEventfd()),
              wakeupChannel_(new Channel(this, wakeupFd_)),
              mutex_(),
              currentActiveChannel(nullptr)
        {
            LOG_DEBUG << "EventLoop created " << this << " in thread " << threadId_;
            if (t_loopInThisThread)
            {
                LOG_FATAL << "Another EventLoop " << t_loopInThisThread
                          << "exits in this thread " << threadId_;
            }
            else
            {
                t_loopInThisThread = this;
            }
            wakeupChannel_->setReadCallback(std::bind(&EventLoop::handleRead, this));
            wakeupChannel_->enableReading();
        }

        EventLoop::~EventLoop()
        {
            assert(!looping_);
            LOG_DEBUG << "EventLoop " << this << " of thread " << threadId_
                << " destructs in thread " << CurrentThread::tid();
            wakeupChannel_->disableAll();
            wakeupChannel_->remove();
            ::close(wakeupFd_);
            t_loopInThisThread = NULL;
            looping_ = false;
        }

        EventLoop* EventLoop::getEventLoopOfCurrentThread()
        {
            return t_loopInThisThread;
        }

        void EventLoop::loop()
        {
            assert(!looping_);
            assertInLoopThread();
            looping_ = true;
            quit_ = false;

            LOG_TRACE << "EventLoop " << this << " start looping";

            while (!quit_)
            {
                activeChannels_.clear();
                pollReturnTime_ = poller_->poll(kPollTimeMs, &activeChannels_);
                eventHandling_ = true;
                for (Channel* channel : activeChannels_)
                {
                    currentActiveChannel = channel;
                    currentActiveChannel->handleEvent(pollReturnTime_);
                }
                currentActiveChannel = nullptr;
                eventHandling_ = false;
                deoPendingFunctors();
            }

            LOG_TRACE << "EventLoop " << this << " stopped";
            looping_ = false;
        }

        void EventLoop::quit()
        {
            quit_ = true;

            if (!isInLoopThread())
            {
                wakeup();
            }
        }

        void EventLoop::runInLoop(const Functor &cb)
        {
            if (isInLoopThread())
            {
                cb();
            }
            else
            {
                queueInLoop(cb);
            }
        }
        void EventLoop::queueInLoop(const Functor &cb)
        {
            {
                MutexLockGuard lock(mutex_);
                pendingFunctors_.push_back(std::move(cb));
            }
            if (!isInLoopThread() || callingPendingFunctors_)
            {
                //3种情况
                //1.其他线程调用queueInLoop，需要唤醒，可能卡在poll()
                //2.自己线程调用，不需要唤醒，接下来会执行到doPendingFunctors()
                //3.callingPendingFunctors，需要唤醒，不唤醒的话，这个任务就只能等待下一次poll()
                wakeup();
            }
        }

        TimerId EventLoop::runAt(const Timestamp &time, const TimerCallback &cb)
        {
            return timerQueue_->addTimer(std::move(cb), time, 0.0);
        }
        TimerId EventLoop::runAfter(double delay, const TimerCallback &cb)
        {
            Timestamp time(addTime(Timestamp::now(), delay));
            return timerQueue_->addTimer(std::move(cb), time, 0.0);
        }
        TimerId EventLoop::runEvery(double interval, const TimerCallback &cb)
        {
            Timestamp time(addTime(Timestamp::now(), interval));
            return timerQueue_->addTimer(std::move(cb), time, interval);
        }

        void EventLoop::updateChannel(Channel* channel)
        {
            assert(channel->ownerLoop() == this);
            assertInLoopThread();
            poller_->updateChannel(channel);
        }

        void EventLoop::removeChannel(Channel* channel)
        {
            assert(channel->ownerLoop() == this);
            assertInLoopThread();
            if (eventHandling_ )
            {
                assert(channel == currentActiveChannel || std::find(activeChannels_.begin(), activeChannels_.end(), channel) == activeChannels_.end());
            }
            poller_->removeChannel(channel);
        }

        bool EventLoop::hasChannel(Channel* channel)
        {
            assertInLoopThread();
            assert(channel->ownerLoop() == this);
            return poller_->hasChannel(channel);
        }

        void EventLoop::abortNotInLoopThread() const
        {
            LOG_FATAL << "EventLoop::abortNotInLoopThread - EventLoop " << this
                      << " was created in threadId_ = " << threadId_
                      << ", current thread id = " << CurrentThread::tid();
        }
        void EventLoop::wakeup()
        {
            uint64_t one = 1;
            ssize_t n = ::write(wakeupFd_, &one, sizeof one);
            if (n != sizeof one)
            {
                LOG_ERROR << "EventLoop::wakeup() writes" << n << "bytes instread of 8";
            }
        }

        void EventLoop::cancel(TimerId timerId)
        {
            return timerQueue_->cancelTimer(timerId);
        }
        void EventLoop::handleRead()
        {
            uint64_t one = 1;
            ssize_t n = ::read(wakeupFd_, &one, sizeof one);
            if (n != sizeof one)
            {
                LOG_ERROR << "EventLoop::wakeup() reads" << n << "bytes instread of 8";
            }
        }

        void EventLoop::deoPendingFunctors()
        {
            callingPendingFunctors_ = true;
            std::vector<Functor> dopendingFunctors;
            {
                MutexLockGuard lock(mutex_);
                swap(pendingFunctors_, dopendingFunctors);
            }
            for (auto it = dopendingFunctors.begin(); it != dopendingFunctors.end(); it++)
            {
                (*it)();
            }
            callingPendingFunctors_ = false;
        }
    }
}