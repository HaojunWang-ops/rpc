#include "Poller.h"
#include "Channel.h"

using namespace reactor;
using namespace reactor::net;

Poller::Poller(EventLoop* loop)
    :owner_loop_(loop)
{   
}

Poller::~Poller() = default;

bool Poller::hasChannel(Channel* channel) const
{
    assertInLoopThread();
    ChannelMap::const_iterator it = channels_.find(channel->fd());
    return it != channels_.end() && it->second == channel;
}