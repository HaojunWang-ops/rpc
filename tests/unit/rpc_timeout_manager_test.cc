#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

#define private public
#include "rpc_timeout_manager.h"
#undef private

namespace
{
using namespace std::chrono_literals;

bool waitUntil(std::chrono::milliseconds timeout, const std::function<bool()>& pred)
{
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline)
    {
        if (pred())
        {
            return true;
        }
        std::this_thread::sleep_for(2ms);
    }
    return pred();
}

class TimeoutRecorder
{
public:
    void record(uint64_t request_id)
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            request_ids_.push_back(request_id);
        }
        cv_.notify_all();
    }

    bool waitForSize(size_t size, std::chrono::milliseconds timeout)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, timeout, [&] {
            return request_ids_.size() >= size;
        });
    }

    std::vector<uint64_t> requestIds() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return request_ids_;
    }

    size_t size() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return request_ids_.size();
    }

private:
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::vector<uint64_t> request_ids_;
};

size_t heapSize(RpcTimeoutManager& manager)
{
    std::lock_guard<std::mutex> lock(manager.mutex_);
    return manager.heap_.size();
}
}

// timer worker 未运行或已停止时不能接受新 deadline。
TEST(RpcTimeoutManagerTest, AddBeforeStartAndAfterStopShouldBeRejected)
{
    TimeoutRecorder recorder;
    RpcTimeoutManager manager([&recorder](uint64_t request_id) {
        recorder.record(request_id);
    });

    manager.add(1, 0ms);

    EXPECT_FALSE(manager.running_.load(std::memory_order_acquire));
    EXPECT_EQ(heapSize(manager), 0u);
    EXPECT_EQ(recorder.size(), 0u);

    manager.start();
    manager.add(2, 1h);
    EXPECT_EQ(heapSize(manager), 1u);

    manager.stop();
    EXPECT_FALSE(manager.running_.load(std::memory_order_acquire));
    EXPECT_EQ(heapSize(manager), 0u);

    manager.add(3, 0ms);
    EXPECT_EQ(heapSize(manager), 0u);
    EXPECT_FALSE(recorder.waitForSize(1, 50ms));
}

// stop 清空未到期条目，防止旧 timer 在生命周期结束后触发。
TEST(RpcTimeoutManagerTest, StopShouldClearPendingHeap)
{
    TimeoutRecorder recorder;
    RpcTimeoutManager manager([&recorder](uint64_t request_id) {
        recorder.record(request_id);
    });

    manager.start();
    for (uint64_t request_id = 1; request_id <= 16; ++request_id)
    {
        manager.add(request_id, 1h);
    }

    EXPECT_EQ(heapSize(manager), 16u);

    manager.stop();

    EXPECT_FALSE(manager.running_.load(std::memory_order_acquire));
    EXPECT_EQ(heapSize(manager), 0u);
    EXPECT_FALSE(recorder.waitForSize(1, 50ms));
}

// 并发 start/stop 必须串行化，最终不遗留 worker 或 heap 状态。
TEST(RpcTimeoutManagerTest, ConcurrentStartAndStopShouldSerialize)
{
    TimeoutRecorder recorder;
    RpcTimeoutManager manager([&recorder](uint64_t request_id) {
        recorder.record(request_id);
    });

    constexpr int kThreadCount = 8;
    constexpr int kIterations = 100;

    std::atomic<int> ready{0};
    std::atomic<bool> go{false};
    std::vector<std::thread> workers;
    workers.reserve(kThreadCount);

    for (int thread_index = 0; thread_index < kThreadCount; ++thread_index)
    {
        workers.emplace_back([&, thread_index] {
            ready.fetch_add(1, std::memory_order_acq_rel);
            while (!go.load(std::memory_order_acquire))
            {
                std::this_thread::yield();
            }

            for (int i = 0; i < kIterations; ++i)
            {
                if ((thread_index + i) % 2 == 0)
                {
                    manager.start();
                }
                else
                {
                    manager.stop();
                }
            }
        });
    }

    ASSERT_TRUE(waitUntil(1s, [&] {
        return ready.load(std::memory_order_acquire) == kThreadCount;
    }));
    go.store(true, std::memory_order_release);

    for (auto& worker : workers)
    {
        worker.join();
    }

    manager.stop();

    EXPECT_FALSE(manager.running_.load(std::memory_order_acquire));
    EXPECT_FALSE(manager.worker_.joinable());
    EXPECT_EQ(heapSize(manager), 0u);
    EXPECT_EQ(recorder.size(), 0u);
}

// 负 timeout 没有有效 deadline 语义，必须被拒绝。
TEST(RpcTimeoutManagerTest, NegativeTimeoutShouldBeRejected)
{
    TimeoutRecorder recorder;
    RpcTimeoutManager manager([&recorder](uint64_t request_id) {
        recorder.record(request_id);
    });

    manager.start();
    manager.add(1, -1ms);

    EXPECT_EQ(heapSize(manager), 0u);
    EXPECT_FALSE(recorder.waitForSize(1, 50ms));

    manager.stop();
}

// 每个到期 request id 只触发一次 timeout callback。
TEST(RpcTimeoutManagerTest, ExpiredTimeoutsShouldInvokeCallbackOnce)
{
    TimeoutRecorder recorder;
    RpcTimeoutManager manager([&recorder](uint64_t request_id) {
        recorder.record(request_id);
    });

    manager.start();
    manager.add(11, 5ms);
    manager.add(22, 5ms);
    manager.add(33, 5ms);

    ASSERT_TRUE(recorder.waitForSize(3, 2s));

    manager.stop();

    auto request_ids = recorder.requestIds();
    std::sort(request_ids.begin(), request_ids.end());

    const std::vector<uint64_t> expected{11, 22, 33};
    EXPECT_EQ(request_ids, expected);
    EXPECT_EQ(heapSize(manager), 0u);
}

// restart 不得继承上一次运行留下的 deadline。
TEST(RpcTimeoutManagerTest, RestartAfterStopShouldStartWithEmptyHeap)
{
    TimeoutRecorder recorder;
    RpcTimeoutManager manager([&recorder](uint64_t request_id) {
        recorder.record(request_id);
    });

    manager.start();
    manager.add(1, 1h);
    EXPECT_EQ(heapSize(manager), 1u);

    manager.stop();
    EXPECT_EQ(heapSize(manager), 0u);

    manager.start();
    EXPECT_EQ(heapSize(manager), 0u);
    manager.add(2, 5ms);

    ASSERT_TRUE(recorder.waitForSize(1, 2s));
    manager.stop();

    auto request_ids = recorder.requestIds();
    ASSERT_EQ(request_ids.size(), 1u);
    EXPECT_EQ(request_ids[0], 2u);
}
