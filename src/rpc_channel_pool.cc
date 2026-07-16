#include "rpc_channel_pool.h"

RpcChannelPool::RpcChannelPool(std::string ip, uint16_t port, size_t pool_size)
    : ip_(ip), port_(port), pool_size_(pool_size),
      callback_executor_(std::make_unique<CallbackExecutor>())
{
}

RpcChannelPool::~RpcChannelPool()
{
    stop();
}

bool RpcChannelPool::start()
{
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    if (pool_state_ != State::kStopped)
    {
        return false;
    }

    if (pool_size_ == 0)
    {
        return false;
    }

    callback_executor_->start();
    std::shared_ptr<ChannelList> new_channels;
    new_channels = std::make_shared<ChannelList>();
    new_channels->reserve(pool_size_);

    for (size_t i = 0; i < pool_size_; i++)
    {
        auto ch = MyRpcChannel::create(ip_, port_, callback_executor_.get());
        ch->setTimeoutMs(timeout_ms_);

        if (!ch->start())
        {
            for (auto &opened : (*new_channels))
            {
                opened->stop();
            }
            callback_executor_->stop();
            return false;
        }
        new_channels->push_back(std::move(ch));
    }

    {
        std::lock_guard<std::mutex> lock(repair_mutex_);
        std::atomic_store_explicit(
            &channels_snapshot_,
            new_channels,
            std::memory_order_release);
    }

    pool_state_ = State::kRunning;
    
    return true;
}

void RpcChannelPool::stop()
{
    if (callback_executor_->isInWorkerThread())
    {
        LOG_ERROR << "RpcChannelPool::stop() must not be called from CallbackExecutor worker thread";
        return;
    }
    
    std::shared_ptr<ChannelList> old_snapshot;
    {
        std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
        {
            if (pool_state_ != State::kRunning)
            {
                return;
            }

            pool_state_ = State::kStopping;

            std::lock_guard<std::mutex> lock(repair_mutex_);

            old_snapshot = std::atomic_load_explicit(
                &channels_snapshot_,
                std::memory_order_acquire);

            std::atomic_store_explicit(
                &channels_snapshot_,
                std::shared_ptr<ChannelList>{},
                std::memory_order_release);
        }
    }

    for (auto &ch : *old_snapshot)
    {
        if (ch)
        {
            ch->stop();
        }
    }

    {
        std::unique_lock<std::mutex> lock(lifecycle_mutex_);
        lifecycle_cv_.wait(lock, [this](){
            return active_calls_ == 0;
        });
    }

    if (callback_executor_)
    {
        callback_executor_->stop();
    }

    {
        std::lock_guard<std::mutex> lock(lifecycle_mutex_);
        pool_state_ = State::kStopped;
    }

    return;
}

/*
// Weak stop semantics:
// repairDeadChannels() may run concurrently with RpcChannelPool::stop().
// Once stop() begins, running_ becomes false and channels_snapshot_ is cleared.
// A repair operation that has already started may still create a new channel
// outside the repair lock. Before publishing, it must check running_ and
// compare the current snapshot with old_snapshot.
// If publishing succeeds, old channels are stopped.
// If publishing fails, newly created channels are stopped.
// RpcChannelPool::stop() does not wait for concurrent repair operations to exit.
*/
bool RpcChannelPool::repairChannel(size_t index)
{
    std::shared_ptr<MyRpcChannel> old_ch;
    std::shared_ptr<ChannelList> old_snapshot;
    {
        std::lock_guard<std::mutex> lock(repair_mutex_);

        old_snapshot = std::atomic_load_explicit(
            &channels_snapshot_,
            std::memory_order_acquire);

        if (!old_snapshot || index >= old_snapshot->size())
        {
            return false;
        }

        old_ch = (*old_snapshot)[index];

        if (old_ch && old_ch->isAvailable())
        {
            return true;
        }
    }

    auto new_ch = MyRpcChannel::create(ip_, port_, callback_executor_.get());
    new_ch->setTimeoutMs(timeout_ms_);

    if (!new_ch->start())
    {
        return false;
    }

    auto new_channels = std::make_shared<ChannelList>(*old_snapshot);
    (*new_channels)[index] = new_ch;

    std::shared_ptr<ChannelList> new_snapshot = new_channels;

    bool published = false;
    {
        std::lock_guard<std::mutex> lock(repair_mutex_);
        auto now_snapshot = std::atomic_load_explicit(
            &channels_snapshot_,
            std::memory_order_acquire);

        if (now_snapshot == old_snapshot)
        {
            std::atomic_store_explicit(
                &channels_snapshot_,
                new_snapshot,
                std::memory_order_release);

            published = true;
        }
    }
    if (published)
    {
        if (old_ch)
        {
            old_ch->stop();
        }
        return true;
    }
    else
    {
        if (new_ch)
        {
            new_ch->stop();
        }
        return false;
    }
}

bool RpcChannelPool::repairChannelInCopy(ChannelList &new_channels, 
                                        size_t index, 
                                        std::vector<std::shared_ptr<MyRpcChannel>> &channels_to_stop,
                                        std::vector<std::shared_ptr<MyRpcChannel>> &new_channels_stated)
{
    auto old_ch = std::move(new_channels[index]);

    if (old_ch && old_ch->isAvailable())
    {
        new_channels[index] = std::move(old_ch);
        return false;
    } 

    auto new_ch = MyRpcChannel::create(ip_, port_, callback_executor_.get());
    new_ch->setTimeoutMs(timeout_ms_);
    
    if (!new_ch->start())
    {
        new_channels[index] = std::move(old_ch);
        return false;
    }

    new_channels.push_back(new_ch);
    std::swap(new_channels[index], new_channels.back());
    new_channels.pop_back();

    channels_to_stop.push_back(std::move(old_ch));
    new_channels_stated.push_back(std::move(new_ch));
    return true;
}

void RpcChannelPool::repairDeadChannels()
{
    
    std::shared_ptr<ChannelList> old_snapshot;
    {
        std::lock_guard<std::mutex> lock(repair_mutex_);

        old_snapshot = std::atomic_load_explicit(
            &channels_snapshot_,
            std::memory_order_acquire);

        if (!old_snapshot)
        {
            return;
        }
    }
    auto new_snapshot = std::make_shared<ChannelList>(*old_snapshot);

    bool changed = false;
    std::vector<std::shared_ptr<MyRpcChannel>> channels_to_stop;
    std::vector<std::shared_ptr<MyRpcChannel>> new_channels_started;

    for (size_t i = 0; i < (*new_snapshot).size(); i++)
    {
        changed |= repairChannelInCopy(*new_snapshot,
                                         i,
                                         channels_to_stop,
                                         new_channels_started);
    }
#ifdef MYRPC_ENABLE_TEST_HOOKS
    auto hooks = test_hooks_;

    if (changed && hooks && hooks->before_snapshot_publish)
    {
        hooks->before_snapshot_publish(new_channels_started);
    }
#endif
    bool published = false;
    {
        std::lock_guard<std::mutex> lock(repair_mutex_);
        auto now_snapshot = std::atomic_load_explicit(
            &channels_snapshot_,
            std::memory_order_acquire);

        if (old_snapshot == now_snapshot && changed)
        {
            std::atomic_store_explicit(
                &channels_snapshot_,
                new_snapshot,
                std::memory_order_release);
            published = true;
        }
    }

    if (published)
    {
        for (auto& ch : channels_to_stop)
        {
            if (ch)
            {
                ch->stop();
            }
        }
        return;
    }
    else
    {
        for (auto& ch : new_channels_started)
        {
            if (ch)
            {
                ch->stop();
            }
        }
        return;
    }
}
std::shared_ptr<MyRpcChannel> RpcChannelPool::pickChannel()
{
    auto snapshot = std::atomic_load_explicit(
        &channels_snapshot_,
        std::memory_order_acquire);

    if (!snapshot || snapshot->empty())
    {
        return nullptr;
    }

    const size_t n = snapshot->size();

    for (size_t i = 0; i < n; ++i)
    {
        size_t idx = next_.fetch_add(1, std::memory_order_relaxed) % n;

        auto ch = (*snapshot)[idx];
        if (ch && ch->isAvailable())
        {
            return ch;
        }

        if (repairChannel(idx))
        {
            auto new_snapshot = std::atomic_load_explicit(
                &channels_snapshot_,
                std::memory_order_acquire);

            if (new_snapshot && idx < new_snapshot->size())
            {
                auto repaired = (*new_snapshot)[idx];
                if (repaired && repaired->isAvailable())
                {
                    return repaired;
                }
            }
        }
    }

    return nullptr;
}

size_t RpcChannelPool::unavailableCount() const
{
    auto old_snaptshot = std::atomic_load_explicit(
        &channels_snapshot_,
        std::memory_order_acquire);

    if (!old_snaptshot)
    {
        return 0;
    }

    size_t count = 0;

    for (const auto &ch : *old_snaptshot)
    {
        if (!ch || !ch->isAvailable())
        {
            count++;
        }
    }

    return count;
}
void RpcChannelPool::CallMethod(const google::protobuf::MethodDescriptor *method,
                                google::protobuf::RpcController *controller,
                                const google::protobuf::Message *request,
                                google::protobuf::Message *response,
                                google::protobuf::Closure *done)
{
    if (!enterCall())
    {
        if (controller)
        {
            controller->SetFailed("RpcChannel is stopped");

        }
        if(done)
        {
            done->Run();
        }
        return;
    }

    auto ch = pickChannel();

    if (!ch)
    {
        if (controller)
        {
            controller->SetFailed("no available rpc channel");

        }            
        if (done)
        {
            done->Run();
        }

        leaveCall();
        return;
    }

    ch->CallMethod(method, controller, request, response, done);
    leaveCall();
}

bool RpcChannelPool::enterCall()
{
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);

    if (pool_state_ != State::kRunning)
    {
        return false;
    }

    ++active_calls_;
    return true;
}

void RpcChannelPool::leaveCall()
{
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);

    if (active_calls_ > 0)
    {
        --active_calls_;
    }

    lifecycle_cv_.notify_all();
}

void RpcChannelPool::setTimeoutMs(int timeout_ms)
{
    timeout_ms_ = timeout_ms;
}