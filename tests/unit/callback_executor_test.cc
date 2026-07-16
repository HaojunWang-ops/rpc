#include "CallbackExecutor.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <thread>

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

// 防止 callback worker 调用 stop() 时 self-join，且 executor 仍可继续执行后续任务。
TEST(CallbackExecutorTest, StopFromWorkerThreadShouldBeIgnoredAndKeepExecutorRunning)
{
    CallbackExecutor executor;
    executor.start();

    std::mutex mutex;
    std::condition_variable cv;
    int completed = 0;
    std::atomic<bool> stop_task_was_in_worker{false};
    std::atomic<bool> followup_task_was_in_worker{false};

    ASSERT_TRUE(executor.post([&] {
        stop_task_was_in_worker.store(executor.isInWorkerThread(),
                                      std::memory_order_release);
        executor.stop();

        {
            std::lock_guard<std::mutex> lock(mutex);
            ++completed;
        }
        cv.notify_all();
    }));

    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(1), [&] {
            return completed == 1;
        }));
    }

    ASSERT_TRUE(executor.post([&] {
        followup_task_was_in_worker.store(executor.isInWorkerThread(),
                                          std::memory_order_release);

        {
            std::lock_guard<std::mutex> lock(mutex);
            ++completed;
        }
        cv.notify_all();
    }));

    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(1), [&] {
            return completed == 2;
        }));
    }

    EXPECT_TRUE(stop_task_was_in_worker.load(std::memory_order_acquire));
    EXPECT_TRUE(followup_task_was_in_worker.load(std::memory_order_acquire));

    executor.stop();

    EXPECT_TRUE(waitUntil(std::chrono::milliseconds(100), [&] {
        return !executor.post([] {});
    }));
}
