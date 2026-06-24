#include "rpc_timeout_manager.h"

RpcTimeoutManager::RpcTimeoutManager(TimeoutCallback cb)
    : on_timeout_(std::move(cb))
{
}

RpcTimeoutManager::~RpcTimeoutManager()
{
    stop();
}

void RpcTimeoutManager::start()
{
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (running_.load(std::memory_order_acquire))
        {
            return;
        }
        running_ .store(true, std::memory_order_release);
        worker_ = std::thread(&RpcTimeoutManager::loop, this);
    }
}

void RpcTimeoutManager::stop()
{
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!running_.load(std::memory_order_acquire))
        {
            return;
        }
        running_.store(false, std::memory_order_release);
        cv_.notify_all();
    }
    if (worker_.joinable())
    {
        worker_.join();
    }
}

void RpcTimeoutManager::add(uint64_t request_id, std::chrono::milliseconds timeout)
{
    std::lock_guard<std::mutex> lock(mutex_);

    struct TimeoutItem item;
    item.request_id = request_id;
    item.deadline = Clock::now() + timeout;
    item.sequence = next_sequence_++;
    
    heap_.push(std::move(item));
    cv_.notify_one();
}

void RpcTimeoutManager::loop()
{
    while (running_.load(std::memory_order_acquire))
    {
        std::vector<uint64_t> expired;
        {
            std::unique_lock<std::mutex> lock(mutex_);

            while (running_ && heap_.empty())
            {
                cv_.wait(lock);
            }

            if (!running_.load(std::memory_order_acquire))
            {
                break;
            }

            while (running_.load(std::memory_order_acquire) && !heap_.empty())
            {
                auto now = Clock::now();
                auto nearest_deadline = heap_.top().deadline;

                if (nearest_deadline > now)
                {
                    cv_.wait_until(lock, nearest_deadline);
                    continue;
                }

                while (!heap_.empty() && heap_.top().deadline <= now)
                {

                    expired.push_back(heap_.top().request_id);
                    heap_.pop();
                }

                break;
            }
        }

        for (uint64_t request_id : expired)
        {
            on_timeout_(request_id);
        }
    }
}