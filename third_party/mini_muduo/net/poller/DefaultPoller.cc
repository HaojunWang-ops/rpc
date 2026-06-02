#include "Poller.h"

#include "EPollPoller.h"
#include "PollPoller.h"

#include <stdlib.h>

using namespace reactor::net;

Poller* Poller::newDefaultPoller(EventLoop* loop)
{
    if (::getenv("REACTOR_USE_POLL"))
    {
        return new PollPoller(loop);
    }
    else
    {
        return new EPollPoller(loop);
    }
}
