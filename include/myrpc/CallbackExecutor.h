#pragma once

#include "Logging.h"

#include <condition_variable>
#include <exception>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>

class CallbackExecutor
{
public:
    CallbackExecutor() = default;

    ~CallbackExecutor()
    {
        stop();
    }

    CallbackExecutor(const CallbackExecutor&) = delete;
    CallbackExecutor& operator=(const CallbackExecutor&) = delete;

    void start()
    {
        std::lock_guard<std::mutex> lock(mutex_);

        if (started_)
        {
            return;
        }

        stopping_ = false;

        worker_ = std::thread([this]() {
            this->runInLoop();
        });

        started_ = true;
    }

    void stop()
    {
        if (worker_.joinable() &&
            worker_.get_id() == std::this_thread::get_id())
        {
            std::terminate();
        }

        {
            std::lock_guard<std::mutex> lock(mutex_);

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

private:
    void runInLoop()
    {
        while (true)
        {
            std::function<void()> task;

            {
                std::unique_lock<std::mutex> lock(mutex_);

                cv_.wait(lock, [this]() {
                    return stopping_ || !tasks_.empty();
                });

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
};