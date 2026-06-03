#include "ThreadPool.h"

#include <assert.h>
#include <type_traits>
#include <stdexcept>
#include <iostream>

ThreadPool::ThreadPool(size_t thread_num)
    : num_(thread_num)
{  
}

ThreadPool::~ThreadPool()
{
    stop();
}

void ThreadPool::start()
{
    {
        std::unique_lock<std::mutex> lock(mutex_);
        if (started_)
        {
            return;
        }
        started_ = true;
        stop_ = false;
    }
    for (int i = 0; i < num_; i++)
    {
        threads_.emplace_back([this](){
            workerLoop();
        });
    }
}

void ThreadPool::workerLoop()
{
    while (true)
    {
        std::function<void()> task;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cond_.wait(lock, [this](){
                return (stop_ || !tasks_.empty());
            });

            if (stop_ && tasks_.empty())
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
        catch (const std::exception& e)
        {
            std::cerr << "ThreadPool task exception: " << e.what() << "\n";
        }
        catch (...)
        {
            std::cerr << "ThreadPool task unknown exception\n";
        }
    }
}
void ThreadPool::stop()
{
    {
        std::unique_lock<std::mutex> lock(mutex_);

        if (started_ != true || stop_ == true)
        {
            return;
        }

        stop_ = true;
        
    }

    cond_.notify_all();
    
    for (auto &t : threads_)
    {
        if (t.joinable())
        {
            t.join();
        }
    }

    threads_.clear();

    {
        std::unique_lock<std::mutex> lock(mutex_);
        started_ = false;
    }
}

void ThreadPool::submit(std::function<void()> task)
{
    {
        std::unique_lock<std::mutex> lock(mutex_);
        if (!started_)
        {
            throw std::runtime_error("enqueue on unstarted_ ThreadPool");
        }

        if (stop_)
        {
            throw std::runtime_error("enqueue on stopped ThreadPool");
        }
        tasks_.emplace(std::move(task));
    }
    cond_.notify_one();
}


