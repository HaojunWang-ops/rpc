#pragma once

#include "Timestamp.h"
#include "Logging.h"
#include "EventLoop.h"

#include <vector>
#include <map>
#include <chrono>
#include <poll.h>

namespace reactor
{
    namespace net
    {
        class EventLoop;
        class Channel;

        class Poller : noncopyable
        {
        public:
            typedef std::vector<Channel *> ChannelList;
            Poller(EventLoop *loop); 

            virtual ~Poller();

            virtual Timestamp poll(int timeoutMs, ChannelList *activeChannels) = 0;
            
            virtual void updateChannel(Channel *channel) = 0;

            void assertInLoopThread() const
            {
                owner_loop_->assertInLoopThread();
            }

            virtual void removeChannel(Channel* channel) = 0;
           
            virtual bool hasChannel(Channel* channel) const;

            static Poller* newDefaultPoller(EventLoop* loop);
        protected:
            typedef std::map<int, Channel *> ChannelMap;
            ChannelMap channels_;
        
        private:
            EventLoop *owner_loop_;
        };
    }
}