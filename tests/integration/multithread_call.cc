#include "rpc_channel_pool.h"
#include "rpc_controller.h"
#include "tcpserver.h"
#include "user.pb.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <vector>

namespace
{
std::string buildLoginResponseBody(const myrpc::RpcHeader&,
                                   const std::string&)
{
    demo::LoginResponse response;
    response.set_code(0);
    response.set_message("login success");
    response.set_success(true);
    return response.SerializeAsString();
}
}

// 多线程同步调用共享 pool 时，所有调用必须返回而非因连接竞争挂起。
TEST(MultithreadCallTest, ConcurrentSyncCallsShouldAllReturn)
{
    constexpr int kThreadCount = 8;
    constexpr int kCallsPerThread = 25;
    constexpr int kRequestCount = kThreadCount * kCallsPerThread;

    ControlledTcpServer server(0, buildLoginResponseBody);
    ASSERT_TRUE(server.start());

    RpcChannelPool pool("127.0.0.1", server.port(), 4);
    ASSERT_TRUE(pool.start());

    demo::UserService_Stub stub(&pool);

    std::atomic<int> success_count{0};
    std::atomic<int> fail_count{0};
    std::vector<std::future<void>> workers;
    workers.reserve(kThreadCount);

    for (int t = 0; t < kThreadCount; ++t)
    {
        workers.push_back(std::async(std::launch::async, [&] {
            for (int i = 0; i < kCallsPerThread; ++i)
            {
                demo::LoginRequest request;
                demo::LoginResponse response;
                SimpleRpcController controller;

                request.set_name("haojun");
                request.set_password("123456");

                stub.Login(&controller, &request, &response, nullptr);

                if (!controller.Failed() && response.success())
                {
                    success_count.fetch_add(1, std::memory_order_relaxed);
                }
                else
                {
                    fail_count.fetch_add(1, std::memory_order_relaxed);
                }
            }
        }));
    }

    for (auto& worker : workers)
    {
        ASSERT_EQ(worker.wait_for(std::chrono::seconds(3)), std::future_status::ready);
        worker.get();
    }

    EXPECT_EQ(success_count.load(std::memory_order_relaxed), kRequestCount);
    EXPECT_EQ(fail_count.load(std::memory_order_relaxed), 0);
    EXPECT_TRUE(server.waitForTotalRequests(kRequestCount, std::chrono::seconds(1)));

    pool.stop();
    server.stop();
}
