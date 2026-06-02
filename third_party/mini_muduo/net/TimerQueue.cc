#include "TimerQueue.h"
#include "Logging.h"
#include "Timer.h"
#include "EventLoop.h"

#include <sys/timerfd.h>
#include <cstdint>
#include <assert.h>

namespace reactor
{
    namespace net
    {
        namespace detail
        {
            int createTimerfd()
            {
                int timerfd = ::timerfd_create(CLOCK_MONOTONIC, TFD_CLOEXEC | TFD_NONBLOCK);
                if (timerfd < 0)
                {
                    LOG_SYSERR << "createTimerfd() error, at timerfd_create";
                }
                return timerfd;
            }

            struct timespec howMuchTimeFromNow(Timestamp when)
            {
                struct timespec tv;
                int64_t microseconds = when.microSecondsSinceEpoch() - (Timestamp::now()).microSecondsSinceEpoch();
                if (microseconds < 100)
                {
                    microseconds = 100;
                }

                tv.tv_sec = static_cast<time_t>(microseconds / Timestamp::kMicroSecondsPerSecond);
                tv.tv_nsec = static_cast<long>(microseconds % Timestamp::kMicroSecondsPerSecond * 1000);

                return tv;
            }

            void readTimerfd(int timerfd, Timestamp now)
            {
                uint64_t howmany;
                int n = ::read(timerfd, &howmany, sizeof howmany);
                LOG_INFO << now.toFormattedString() << " " << howmany << " in timerfd";
                if (n != sizeof howmany)
                {
                    LOG_SYSERR << "read " << n << " bytes from timerfd";
                }
            }

            void resetTimerfd(int timerfd, Timestamp expiration)
            {
                struct itimerspec newvalue;
                struct itimerspec oldvalue;

                bzero(&newvalue, sizeof newvalue);
                bzero(&oldvalue, sizeof oldvalue);

                newvalue.it_value = howMuchTimeFromNow(expiration);

                int ret = timerfd_settime(timerfd, 0, &newvalue, &oldvalue);
                if (ret < 0)
                {
                    LOG_SYSERR << "resetTimerfd error, at timerfd_settime()";
                }
            }
        }
    }
}

using namespace reactor;
using namespace reactor::net;
using namespace reactor::net::detail;

TimerQueue::TimerQueue(EventLoop *loop)
    : loop_(loop),
      timerfd_(createTimerfd()),
      timerfdChannel_(loop_, timerfd_),
      timers_(),
      callingExpiredTimers_(false)
{
    timerfdChannel_.setReadCallback([this](Timestamp receivetime)
    { 
        handleRead(receivetime); 
    });
    timerfdChannel_.enableReading();
}

TimerQueue::~TimerQueue()
{
    timerfdChannel_.disableAll();
    timerfdChannel_.remove();
    ::close(timerfd_);

    for (auto it = timers_.begin(); it != timers_.end(); it++)
    {
        delete it->second;
    }
}

TimerId TimerQueue::addTimer(const TimerCallback &callback, Timestamp when, double interval)
{
    Timer *timer = new Timer(callback, when, interval);
    loop_->runInLoop([this, timer]()
                     { addTimerInLoop(timer); });
    return TimerId(timer, timer->sequence());
}

void TimerQueue::cancelTimer(TimerId timerId)
{
    loop_->runInLoop([this, timerId](){
        this->cancelTimerInLoop(timerId);
    });
}

void TimerQueue::addTimerInLoop(Timer *timer)
{
    loop_->assertInLoopThread();
    bool earliestChange = insert(timer);

    //如果插入的timer是最早到期的，要设置timerfd
    if (earliestChange)
    {
        resetTimerfd(timerfd_, timer->expiration());
    }
}

void TimerQueue::cancelTimerInLoop(TimerId timerId)
{
    loop_->assertInLoopThread();
    assert(timers_.size() == activeTimers_.size());
    ActiveTimer timer_sequence(timerId.value_, timerId.sequence_);
    auto it = activeTimers_.find(timer_sequence);
    if (it != activeTimers_.end())
    {
        Entry expiration_timer (timer_sequence.first->expiration(), timerId.value_);
        size_t n = timers_.erase(expiration_timer);
        assert(n == 1); (void) n;
        n = activeTimers_.erase(timer_sequence);
        assert(n == 1); (void) n;
        //用delete不够安全
        //同一个timer*同时被activeTimers_ 和 timers_ 拥有
        //所以delete前，一定一定要把两个containter中的timer*给删掉
        delete timer_sequence.first;
    }
    //在处理过期过期timer时，到期的timer调用了cancelTime
    //此时callingExpiredTimers_== true，getExpired()已经把Tiemr从avtiveTimers_中拿掉了
    //cancelingTimers_来处理timer.repeat的情况
    else if (callingExpiredTimers_)
    {
        cancelingTimers_.insert(timer_sequence);
    }

    assert(activeTimers_.size() == timers_.size());
}

void TimerQueue::handleRead(Timestamp receivetime)
{
    (void) receivetime;
    loop_->assertInLoopThread();
    Timestamp now(Timestamp::now());
    readTimerfd(timerfd_, now);

    //设置状态
    //将上一轮的cancelingTimers_清空
    callingExpiredTimers_ = true;
    cancelingTimers_.clear();

    std::vector<Entry> Expired = getExpired(now);
    for (auto it = Expired.begin(); it != Expired.end(); it++)
    {
        it->second->run();
    }

    callingExpiredTimers_ = false;
    reset(Expired, now);
}

std::vector<TimerQueue::Entry> TimerQueue::getExpired(Timestamp now)
{
    std::vector<Entry> Expired;
    //哨兵值：now + 最大的指针地址
    Entry sentry = std::make_pair(now, reinterpret_cast<Timer *>(UINTPTR_MAX));
    auto it = timers_.lower_bound(sentry);
    assert(it == timers_.end() || now < it->first);
    std::copy(timers_.begin(), it, std::back_inserter(Expired));
    timers_.erase(timers_.begin(), it);    //从timers_中拿掉

    for (Entry expiration_timer : Expired)
    {
        Timer* timer = expiration_timer.second;
        ActiveTimer timer_sequence(timer, timer->sequence());
        size_t n = activeTimers_.erase(timer_sequence); //从activeTimers_中拿掉
        assert(n == 1); (void) n;
    }

    return Expired;
}


//reset负责将expired的timer重新设置状态并insert到TimerList中，并且设置timerfd
void TimerQueue::reset(const std::vector<Entry> &expired, Timestamp now)
{
    Timestamp nextexpire;
    for (auto it = expired.begin(); it != expired.end(); it++)
    {
        ActiveTimer timer_sequence (it->second, it->second->sequence());
        if (it->second->repeat() && cancelingTimers_.find(timer_sequence) == cancelingTimers_.end())
        {
            it->second->restart(now);
            insert(it->second);
        }
        else
        {
            delete it->second;
        }
    }

    if (!timers_.empty())
    {
        nextexpire = timers_.begin()->first;
    }
    if (nextexpire.valid())
    {
        resetTimerfd(timerfd_, nextexpire);
    }
}

//insert负责将timer插入到TimerList中，并且返回插入的timer是不是最早到期的
bool TimerQueue::insert(Timer *timer)
{
    bool earliestChange = false;
    if (timers_.empty())
    {
        earliestChange = true;
    }
    auto it = timers_.begin();
    if (it == timers_.end() || timer->expiration() < it->first)
    {
        earliestChange = true;
    }

    {
        std::pair<TimerList::iterator, bool> result = timers_.insert(std::pair<Timestamp, Timer *>(timer->expiration(), timer));
        assert(result.second);
        (void) result;
    }
    {
        std::pair<ActiveTimerSet::iterator, bool> result = activeTimers_.insert(std::pair<Timer*, int64_t> (timer, timer->sequence()));
        assert(result.second);
        (void) result;
    }
    assert(timers_.size() == activeTimers_.size());
    return earliestChange;
}
