#include "EPollPoller.h"
#include "Channel.h"

#include <assert.h>
#include <poll.h>
#include <errno.h>
#include <sys/epoll.h>
#include <unistd.h>

using namespace reactor;
using namespace reactor::net;

static_assert(EPOLLIN == POLLIN, "epoll uses same flag values as poll");
static_assert(EPOLLPRI == POLLPRI, "epoll uses same flag values as poll");
static_assert(EPOLLOUT == POLLOUT, "epoll uses same flag values as poll");
static_assert(EPOLLRDHUP == POLLRDHUP, "epoll uses same flag values as poll");
static_assert(EPOLLERR == POLLERR, "epoll uses same flag values as poll");
static_assert(EPOLLHUP == POLLHUP, "epoll uses same flag values as poll");

namespace
{
    const int kNew = -1;    // 这个 Channel 从来没有加入过 Poller
    const int kAdded = 1;   // 这个 Channel 已经加入 epoll，并且当前正在关注某些事件
    const int kDeleted = 2; // 这个 Channel 还在 Poller::channels_ 里，但已经从 epoll 的监听集合里删掉了
}

EPollPoller::EPollPoller(EventLoop *loop)
    : Poller(loop),//创建子类的时候，先构建基类
      epollfd_(epoll_create1(EPOLL_CLOEXEC)),
      events_(kInitEventListSize)
{
    if (epollfd_ < 0)
    {
        LOG_SYSFATAL << "EPollPoller::EPollPoller()";
    }
}

EPollPoller::~EPollPoller()
{
    LOG_INFO << "EPollPoller destroyed, this=" << this
             << " epollfd=" << epollfd_
             << " tid=" << CurrentThread::tid();
    ::close(epollfd_);
}

Timestamp EPollPoller::poll(int timeoutMs, ChannelList *activeChannels)
{
    int n = epoll_wait(epollfd_, events_.data(), static_cast<int>(events_.size()), timeoutMs);
    int savedErrno = errno;
    Timestamp now(Timestamp::now());
    if (n > 0)
    {
        LOG_TRACE << n << " events happened";
        fillActiveChannels(n, activeChannels);
        if (n == static_cast<int>(events_.size()))
        {
            events_.resize(events_.size() * 2);
        }
    }
    else if (n == 0)
    {
        LOG_TRACE << "nothing happened";
    }
    else
    {
        if (savedErrno != EINTR)
        {
            errno = savedErrno;
                    LOG_SYSERR << "EPollPoller::poll()"
                   << " epollfd = " << epollfd_
                   << " events.size = " << events_.size()
                   << " timeoutMs = " << timeoutMs
                   << " errno = " << savedErrno;
        }
    }
    return now;
}

void EPollPoller::fillActiveChannels(int numEvents, ChannelList *activeChannels) const
{
    for (int i = 0; i < numEvents; i++)
    {
        Channel *channel = static_cast<Channel *>(events_[i].data.ptr);
#ifndef NDEBUG
        int fd = channel->fd();
        auto it = channels_.find(fd);
        assert(it != channels_.end());
        assert(it->second == channel);
#endif
        channel->set_revent(events_[i].events);
        activeChannels->push_back(channel);
    }
}

void EPollPoller::updateChannel(Channel *channel)
{
    EPollPoller::assertInLoopThread();
    LOG_TRACE << "fd = " << channel->fd()
        << " events = " << channel->events() << " index = " << channel->index();
    if (channel->index() == kNew || channel->index() == kDeleted)
    {
        int fd = channel->fd();
        if (channel->index() == kNew)
        {
            channels_[fd] = channel;
        }
        else if (channel->index() == kDeleted)
        {
            assert(channels_.find(fd) != channels_.end());
            assert(channels_[fd] == channel);
        }
        channel->set_index(kAdded);
        update(EPOLL_CTL_ADD, channel);
    }
    else
    {
        int fd = channel->fd();
        (void)fd;
        assert(channels_.find(fd) != channels_.end());
        assert(channels_[fd] == channel);
        assert(channel->index() == kAdded);

        if (channel->isNoneEvent())
        {
            channel->set_index(kDeleted);
            update(EPOLL_CTL_DEL, channel);
        }
        else
        {
            update(EPOLL_CTL_MOD, channel);
        }
    }
}

void EPollPoller::removeChannel(Channel *channel)
{
    EPollPoller::assertInLoopThread();
    int fd = channel->fd();
    assert(channels_.find(fd) != channels_.end());
    assert(channels_[fd] == channel);
    assert(channel->index() == kAdded || channel->index() == kDeleted);
    assert(channel->isNoneEvent());
    size_t n = channels_.erase(fd);
    (void)n;
    assert(n == 1);
    if (channel->index() == kAdded)
    {
        update(EPOLL_CTL_DEL, channel);
    }

    channel->set_index(kNew);
}

void EPollPoller::update(int operation, Channel *channel)
{
    struct epoll_event event;
    bzero(&event, sizeof event);
    event.events = channel->events();
    event.data.ptr = channel;
    int fd = channel->fd();
    LOG_TRACE << "epoll_ctl op = " << operationToString(operation) << " fd = " << fd;
    if (epoll_ctl(epollfd_, operation, fd, &event) < 0)
    {
        if (operation == EPOLL_CTL_DEL)
        {
            LOG_SYSERR << "epoll_ctl op =" << operationToString(operation) << " fd =" << fd;
        }
        else
        {
            LOG_SYSFATAL << "epoll_ctl op =" << operationToString(operation) << " fd =" << fd;
        }
    }
}

const char* EPollPoller::operationToString(int operation)
{
    switch(operation)
    {
        case EPOLL_CTL_ADD:
            return "ADD";
        case EPOLL_CTL_DEL:
            return "DEL";
        case EPOLL_CTL_MOD:
            return "MOD";
        default:
            assert(false && "error op");
            return "Unknown Operation";
    }
} 