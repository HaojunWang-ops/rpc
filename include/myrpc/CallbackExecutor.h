#pragma once

#include "Logging.h"
#include "CountDownLatch.h"

#include <condition_variable>
#include <exception>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <memory>

class CallbackExecutor
{
public:
    CallbackExecutor() = default;

    ~CallbackExecutor()
    {
        stop();
    }

    CallbackExecutor(const CallbackExecutor &) = delete;
    CallbackExecutor &operator=(const CallbackExecutor &) = delete;

    void start()
    {
        std::lock_guard<std::mutex> lock(mutex_);

        if (started_)
        {
            return;
        }

        stopping_ = false;

        auto start_latch = std::make_shared<reactor::CountDownLatch>(1);
        worker_ = std::thread([this, start_latch]()
                              { this->runInLoop(start_latch); });

        start_latch->wait();

        worker_thread_tid_ = worker_.get_id();

        started_ = true;
    }

    void stop()
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);

            if (started_ && worker_thread_tid_ == std::this_thread::get_id())
            {
                LOG_ERROR << "CallbackExecutor::stop() cannot be called from worker thread";
                return;
            }

            if (!started_ || stopping_)
            {
                return;
            }

            stopping_ = true;
        }

        cv_.notify_all();

        if (worker_.joinable())
        {
            worker_.join();
        }

        {
            std::lock_guard<std::mutex> lock(mutex_);
            started_ = false;
            stopping_ = false;
            worker_thread_tid_ = std::thread::id{};
        }
    }

    bool post(std::function<void()> task)
    {
        if (!task)
        {
            return false;
        }

        {
            std::lock_guard<std::mutex> lock(mutex_);

            if (!started_ || stopping_)
            {
                return false;
            }

            tasks_.push(std::move(task));
        }

        cv_.notify_one();
        return true;
    }

    bool isInWorkerThread() const
    {
        return worker_.joinable() && std::this_thread::get_id() == worker_thread_tid_;
    }

private:
    void runInLoop(const std::shared_ptr<reactor::CountDownLatch> &latch)
    {
        latch->countDown();

        while (true)
        {
            std::function<void()> task;

            {
                std::unique_lock<std::mutex> lock(mutex_);

                cv_.wait(lock, [this]()
                         { return stopping_ || !tasks_.empty(); });

                if (stopping_ && tasks_.empty())
                {
                    return;
                }

                task = std::move(tasks_.front());
                tasks_.pop();
            }

            try
            {
                task();
            }
            catch (...)
            {
                LOG_ERROR << "callback task unknown exception.";
            }
        }
    }

private:
    std::mutex mutex_;
    std::condition_variable cv_;
    std::queue<std::function<void()>> tasks_;

    bool started_ = false;
    bool stopping_ = false;

    std::thread worker_;

    std::thread::id worker_thread_tid_ = std::thread::id{};
};