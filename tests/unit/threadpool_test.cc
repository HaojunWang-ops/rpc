#include "ThreadPool.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <vector>

namespace
{
bool waitUntil(std::chrono::milliseconds timeout, const std::function<bool()>& pred)
{
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline)
    {
        if (pred())
        {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return pred();
}
}

// 未启动线程池不接受任务，调用方不能误以为任务已被执行。
TEST(ThreadPoolTest, SubmitBeforeStartShouldThrow)
{
    ThreadPool pool(2);

    EXPECT_THROW(pool.submit([] {}), std::runtime_error);
}

// 正常运行时每个已提交任务都应被 worker 执行。
TEST(ThreadPoolTest, RunsAllSubmittedTasks)
{
    ThreadPool pool(4);
    pool.start();

    std::atomic<int> completed{0};
    constexpr int kTaskCount = 32;

    for (int i = 0; i < kTaskCount; ++i)
    {
        pool.submit([&completed] {
            completed.fetch_add(1, std::memory_order_relaxed);
        });
    }

    ASSERT_TRUE(waitUntil(std::chrono::seconds(2), [&] {
        return completed.load(std::memory_order_relaxed) == kTaskCount;
    }));

    pool.stop();
    EXPECT_EQ(completed.load(std::memory_order_relaxed), kTaskCount);
}

// stop 需要 drain 已入队任务，重复 stop 不改变已完成结果。
TEST(ThreadPoolTest, StopShouldDrainQueuedTasksAndBeIdempotent)
{
    ThreadPool pool(2);
    pool.start();

    std::atomic<int> completed{0};
    constexpr int kTaskCount = 20;

    for (int i = 0; i < kTaskCount; ++i)
    {
        pool.submit([&completed] {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            completed.fetch_add(1, std::memory_order_relaxed);
        });
    }

    pool.stop();
    pool.stop();

    EXPECT_EQ(completed.load(std::memory_order_relaxed), kTaskCount);
}

// 多个生产者并发 submit 时，队列不能丢失任务。
TEST(ThreadPoolTest, ConcurrentSubmitShouldRunAllTasks)
{
    ThreadPool pool(4);
    pool.start();

    constexpr int kSubmitterCount = 4;
    constexpr int kTasksPerSubmitter = 25;
    constexpr int kTaskCount = kSubmitterCount * kTasksPerSubmitter;

    std::atomic<int> completed{0};
    std::vector<std::thread> submitters;
    submitters.reserve(kSubmitterCount);

    for (int t = 0; t < kSubmitterCount; ++t)
    {
        submitters.emplace_back([&] {
            for (int i = 0; i < kTasksPerSubmitter; ++i)
            {
                pool.submit([&completed] {
                    completed.fetch_add(1, std::memory_order_relaxed);
                });
            }
        });
    }

    for (auto& submitter : submitters)
    {
        submitter.join();
    }

    ASSERT_TRUE(waitUntil(std::chrono::seconds(2), [&] {
        return completed.load(std::memory_order_relaxed) == kTaskCount;
    }));

    pool.stop();
    EXPECT_EQ(completed.load(std::memory_order_relaxed), kTaskCount);
}
