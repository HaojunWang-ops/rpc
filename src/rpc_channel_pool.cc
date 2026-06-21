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

    std::shared_ptr<ChannelList> new_channels;
    {
        callback_executor_->start();

        std::lock_guard<std::mutex> lock(repair_mutex_);
        auto old_snapshot = std::atomic_load_explicit(
            &channels_snapshot_,
            std::memory_order_acquire);

        if (old_snapshot && !old_snapshot->empty())
        {
            return false;
        }

        new_channels = std::make_shared<ChannelList>();
    }

    if (pool_size_ == 0)
    {
        return false;
    }

    new_channels->reserve(pool_size_);

    for (size_t i = 0; i < pool_size_; i++)
    {
        auto ch = std::make_shared<MyRpcChannel>(ip_, port_, callback_executor_.get());
        if (!ch->start())
        {
            for (auto &opened : (*new_channels))
            {
                opened->stop();
            }
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
    return true;
}

void RpcChannelPool::stop()
{
    std::shared_ptr<ChannelList> old_snapshot;
    {
        std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
        {
            std::lock_guard<std::mutex> lock(repair_mutex_);

            old_snapshot = std::atomic_load_explicit(
                &channels_snapshot_,
                std::memory_order_acquire);

            if (!old_snapshot)
            {
                return;
            }

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

    if (callback_executor_)
    {
        callback_executor_->stop();
    }
}

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

    auto new_ch = std::make_shared<MyRpcChannel>(ip_, port_, callback_executor_.get());
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

    auto new_ch = std::make_shared<MyRpcChannel>(ip_, port_, callback_executor_.get());

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
    auto channel = pickChannel();

    if (channel == nullptr)
    {
        if (controller)
        {
            controller->SetFailed("RpcChannelPool is not started");
        }

        if (done)
        {
            done->Run();
        }

        return;
    }

    channel->CallMethod(method, controller, request, response, done);
}
