#pragma once

#include <thread>
#include <queue>
#include <vector>
#include <functional>
#include <mutex>
#include <condition_variable>
#include <atomic>

class ThreadPool
{
public:
    explicit ThreadPool(size_t thread_num);
    ~ThreadPool();

    void start();
    void stop();

    void submit(std::function<void()> task);

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;
    
private:
    void workerLoop();

private:
    std::vector<std::thread> threads_;
    std::queue<std::function<void()> > tasks_;
    std::mutex mutex_;
    std::condition_variable cond_;

    size_t num_;
    bool started_ = false;
    bool stop_ = false;
};