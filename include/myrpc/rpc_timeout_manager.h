#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

class RpcTimeoutManager
{
public:
    using Clock = std::chrono::steady_clock;
    using TimeoutCallback = std::function<void(uint64_t)>;

    explicit RpcTimeoutManager(TimeoutCallback cb);
    ~RpcTimeoutManager();

    void start();
    void stop();

    void add(uint64_t request_id, std::chrono::milliseconds timeout);
private:
    struct TimeoutItem
    {
        uint64_t request_id = 0;
        Clock::time_point deadline;
        uint64_t sequence = 0;
    };

    struct TimeoutItemGreater
    {
        bool operator()(const TimeoutItem& lhs, const TimeoutItem& rhs) const
        {
            if (lhs.deadline != rhs.deadline)
            {
                return lhs.deadline > rhs.deadline;
            }

            return lhs.sequence > rhs.sequence;
        }
    };

    void loop();

    RpcTimeoutManager(const RpcTimeoutManager&) = delete;
    RpcTimeoutManager& operator=(const RpcTimeoutManager&) = delete;
private:
    std::atomic<bool> running_ {false};

    std::mutex lifecycle_mutex_;
    
    std::mutex mutex_;
    std::condition_variable cv_;

    std::priority_queue<
        TimeoutItem,
        std::vector<TimeoutItem>,
        TimeoutItemGreater
    > heap_;

    std::thread worker_;
    TimeoutCallback on_timeout_;

    std::atomic<uint64_t> next_sequence_{0};
};