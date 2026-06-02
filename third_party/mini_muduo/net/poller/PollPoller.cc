#include "PollPoller.h"
#include "Channel.h"
#include "EventLoop.h"
#include "base/Types.h"

#include <poll.h>
#include <assert.h>
#include <algorithm>

namespace reactor
{
    namespace net
    {
        PollPoller::PollPoller(EventLoop* loop)
            : Poller(loop)
        {
        }

        PollPoller::~PollPoller()
        {
        }
        Timestamp PollPoller::poll(int timeoutMs, Poller::ChannelList *activeChannels)
        {
            Timestamp now(Timestamp::now());
            int ret = ::poll(pollfds_.data(), pollfds_.size(), timeoutMs);
            if (ret > 0)
            {
                fillActiveChannels(ret, activeChannels);
                LOG_INFO << ret << "fds active";
            }
            else if (ret == 0)
            {
                LOG_TRACE << "0 fd active";
            }
            else
            {
                LOG_SYSERR << "Poller::poll error";
            }
            return now;
        }

        void PollPoller::fillActiveChannels(int numevents, Poller::ChannelList *activeChannels) const
        {
            for (auto it = pollfds_.begin(); it != pollfds_.end() && numevents > 0; it++)
            {
                if (it->revents)
                {
                    assert(channels_.find(it->fd) != channels_.end());
                    Channel *channel = (channels_.find(it->fd))->second;
                    assert(channel->fd() == it->fd);
                    channel->set_revent(it->revents);
                    activeChannels->push_back(channel);
                    --numevents;
                }
            }
        }

        void PollPoller::updateChannel(Channel *channel) // 修改pollfds中的事件，不对channel中的事件修改
                                                     // 修改channel中的事件，在enable disable函数 和 fillActivityChannel函数中
        {
            Poller::assertInLoopThread();
            LOG_TRACE << "Poller::updateChannel: " << "fd = " << channel->fd() << " events = " << channel->events();
            int fd = channel->fd();
            if (channels_.find(fd) == channels_.end())
            {
                assert(channel->index() < 0);
                channels_.insert({fd, channel});
                int idx = static_cast<int>(pollfds_.size());
                channel->set_index(idx);
                struct pollfd pfd;
                pfd.fd = fd;
                pfd.events = static_cast<short>(channel->events());
                pfd.revents = 0;
                pollfds_.push_back(pfd);
            }
            else
            {
                assert(channels_.find(fd) != channels_.end());
                assert(channels_[fd] == channel);
                int idx = channel->index();
                assert(0 <= idx && idx < static_cast<int>(pollfds_.size()));
                struct pollfd &pfd = pollfds_[idx];
                assert(pfd.fd == fd || pfd.fd == -fd - 1);
                pfd.events = static_cast<short>(channel->events());
                pfd.revents = 0;
                if (channel->isNoneEvent())
                {
                    pfd.fd = -fd - 1; // poll忽略Pollfds_中负数的fd
                }
                else
                {
                    pfd.fd = fd;
                }
            }
        }

        void PollPoller::removeChannel(Channel *channel)
        {
            Poller::assertInLoopThread();
            LOG_TRACE << "fd = " << channel->fd();
            assert(channels_.find(channel->fd()) != channels_.end());
            assert(channels_[channel->fd()] == channel);
            assert(channel->isNoneEvent());

            int idx = channel->index();
            assert(0 <= idx && idx < static_cast<int>(pollfds_.size()));
            const struct pollfd &pfd = pollfds_[idx];
            assert(pfd.fd == -channel->fd() - 1 && pfd.events == channel->events());
            (void)pfd; // 是否已经disableAll()
            ssize_t n = channels_.erase(channel->fd());
            assert(n == 1);
            (void)n;
            if (implicit_cast<size_t>(idx) == pollfds_.size() - 1)
            {
                pollfds_.pop_back();
            }
            else
            {
                int channelAtEnd = pollfds_.back().fd;
                if (channelAtEnd < 0) // 判断最后一个fd的正负
                {
                    channelAtEnd = -channelAtEnd - 1;
                }
                std::iter_swap(pollfds_.begin() + idx, pollfds_.end() - 1);
                channels_[channelAtEnd]->set_index(idx);
                pollfds_.pop_back();
            }
        }
    }
}