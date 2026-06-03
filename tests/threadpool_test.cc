#include "../include/myrpc/ThreadPool.h"

#include <chrono>
#include <iostream>
#include <thread>

int main()
{
    ThreadPool pool(4);
    pool.start();

    for (int i = 0; i < 10; ++i)
    {
        pool.submit([i]() {
            std::cout << "task " << i
                      << " run in thread "
                      << std::this_thread::get_id()
                      << std::endl;

            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        });
    }

    pool.stop();
    return 0;
}