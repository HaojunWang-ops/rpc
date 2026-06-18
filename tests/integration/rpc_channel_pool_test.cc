#include "tcpserver.h"
#include "rpc_channel_pool.h"
#include "user.pb.h"

#include <gtest/gtest.h>
#include <string>
#include <chrono>
#include <future>

std::string buildEmptyResponseBody(const myrpc::RpcHeader& req_header,
                const std::string& request_body)
{
    std::string response_body("");
    return response_body;
}

static bool waitUntil(std::chrono::milliseconds timeout,
                     std::function<bool()> pred)
{
    auto deadline = std::chrono::steady_clock::now() + timeout;

    while (std::chrono::steady_clock::now() < deadline)
    {
        if (pred())
        {
            return true;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

   return pred();     
}

static bool doOneLoginCall(std::shared_ptr<MyRpcChannel> ch)
{
    if (!ch)
    {
        return false;
    }

    demo::LoginRequest req;
    demo::LoginResponse resp;
    SimpleRpcController controller;

    req.set_name("tom");
    req.set_password("1234");

    demo::UserService_Stub stub(ch.get());

    stub.Login(&controller, &req, &resp, nullptr);

    return !controller.Failed();
}

TEST(RpcChannelPoolTest, StartShouldCreateFixedConnections)
{
        
    ControlledTcpServer server(0, buildEmptyResponseBody);
    ASSERT_TRUE(server.start());

    RpcChannelPool pool("127.0.0.1", server.port(), 4);
    ASSERT_TRUE(pool.start());

    ASSERT_TRUE(server.waitForAcceptCount(4, std::chrono::seconds(1)));
    ASSERT_TRUE(server.waitForActiveConnections(4, std::chrono::seconds(1)));

    pool.stop();
    server.stop();
}

TEST(RpcChannelPoolTest, RequestsShouldBeDistributedAcrossConnections)
{
    ControlledTcpServer server(0, buildEmptyResponseBody);
    ASSERT_TRUE(server.start());

    RpcChannelPool pool("127.0.0.1", server.port(), 4);
    ASSERT_TRUE(pool.start());
    ASSERT_TRUE(server.waitForAcceptCount(4, std::chrono::seconds(1)));

    for (int i = 0; i < 8; ++i)
    {
        demo::LoginRequest req;
        demo::LoginResponse resp;
        SimpleRpcController controller;

        req.set_name("haojun");
        req.set_password("123");

        auto ch = pool.pickChannel();
        ASSERT_TRUE(ch);

        demo::UserService_Stub stub(ch.get());
        stub.Login(&controller, &req, &resp, nullptr);

        ASSERT_FALSE(controller.Failed()) << controller.ErrorText();
    }

    ASSERT_TRUE(server.waitForTotalRequests(8, std::chrono::seconds(1)));

    for (size_t i = 0; i < 4; ++i)
    {
        EXPECT_GE(server.requestCountOf(i), 1);
    }

    pool.stop();
    server.stop();
}

TEST(RpcChannelPoolTest, RepairShouldReplaceClosedConnection)
{
    ControlledTcpServer server(0, buildEmptyResponseBody);
    ASSERT_TRUE(server.start());

    RpcChannelPool pool("127.0.0.1", server.port(), 3);
    ASSERT_TRUE(pool.start());

    ASSERT_TRUE(server.waitForAcceptCount(3, std::chrono::seconds(1)));
    size_t old_accept_count = server.acceptCount();

    server.closeConnection(0);

    // 给 reader 线程机会感知连接断开
    ASSERT_TRUE(server.waitForActiveConnections(2, std::chrono::seconds(1)));

    waitUntil(std::chrono::seconds(2), [&](){
        return  pool.unavailableCount() >= 1;
    });

    pool.repairDeadChannels();

    ASSERT_TRUE(server.waitForNewConnectionAfter(old_accept_count, std::chrono::seconds(1)));
    ASSERT_TRUE(server.waitForActiveConnections(3, std::chrono::seconds(1)));

    pool.stop();
    server.stop();
}

TEST(RpcChannelPoolTest, StopShouldCloseAllConnections)
{
    ControlledTcpServer server(0, buildEmptyResponseBody);
    ASSERT_TRUE(server.start());

    RpcChannelPool pool("127.0.0.1", server.port(), 4);
    ASSERT_TRUE(pool.start());

    ASSERT_TRUE(server.waitForAcceptCount(4, std::chrono::seconds(1)));
    ASSERT_TRUE(server.waitForActiveConnections(4, std::chrono::seconds(1)));

    pool.stop();

    ASSERT_TRUE(server.waitForActiveConnections(0, std::chrono::seconds(1)));

    EXPECT_EQ(server.activeCount(), 0);

    server.stop();
}

TEST(RpcChannelPoolTest, ConcurrentPickAndRepairShouldNotCrash)
{
    ControlledTcpServer server(0, buildEmptyResponseBody);
    ASSERT_TRUE(server.start());

    RpcChannelPool pool("127.0.0.1", server.port(), 4);
    ASSERT_TRUE(pool.start());

    ASSERT_TRUE(server.waitForAcceptCount(4, std::chrono::seconds(1)));
    ASSERT_TRUE(server.waitForActiveConnections(4, std::chrono::seconds(1)));

    std::atomic<bool> stop_background{false};
    std::atomic<int> success_count{0};
    std::atomic<int> fail_count{0};

    auto repair_worker = std::async(std::launch::async, [&] {
        while (!stop_background.load(std::memory_order_acquire))
        {
            pool.repairDeadChannels();
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
    });

    auto breaker_worker = std::async(std::launch::async, [&] {
        for (int i = 0; i < 20; ++i)
        {
            if (stop_background.load(std::memory_order_acquire))
            {
                break;
            }

            server.closeOneConnection();
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    });

    constexpr int kThreadCount = 8;
    constexpr int kCallsPerThread = 30;

    std::vector<std::future<void>> workers;
    workers.reserve(kThreadCount);

    for (int t = 0; t < kThreadCount; ++t)
    {
        workers.push_back(std::async(std::launch::async, [&] {
            for (int i = 0; i < kCallsPerThread; ++i)
            {
                auto ch = pool.pickChannel();

                bool ok = doOneLoginCall(ch);

                if (ok)
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

    for (auto& f : workers)
    {
        ASSERT_EQ(f.wait_for(std::chrono::seconds(2)), std::future_status::ready)
            << "worker thread seems blocked; CallMethod may not return after connection loss";
        f.get();
    }

    stop_background.store(true, std::memory_order_release);

    ASSERT_EQ(breaker_worker.wait_for(std::chrono::seconds(1)), std::future_status::ready);
    breaker_worker.get();

    ASSERT_EQ(repair_worker.wait_for(std::chrono::seconds(1)), std::future_status::ready);
    repair_worker.get();

    EXPECT_EQ(success_count.load() + fail_count.load(),
              kThreadCount * kCallsPerThread);

    pool.stop();
    ASSERT_TRUE(server.waitForActiveConnections(0, std::chrono::seconds(2)));

    server.stop();
}

TEST(RpcChannelPoolTest, DoubleStartAndDoubleStopShouldBeSafe)
{
    ControlledTcpServer server(0, buildEmptyResponseBody);
    ASSERT_TRUE(server.start());

    RpcChannelPool pool("127.0.0.1", server.port(), 3);

    ASSERT_TRUE(pool.start());

    ASSERT_TRUE(server.waitForAcceptCount(3, std::chrono::seconds(1)));
    ASSERT_TRUE(server.waitForActiveConnections(3, std::chrono::seconds(1)));

    size_t accept_after_first_start = server.acceptCount();

    bool second_start = pool.start();

    EXPECT_FALSE(second_start);

    EXPECT_FALSE(waitUntil(std::chrono::milliseconds(300), [&] {
        return server.acceptCount() > accept_after_first_start;
    }));

    EXPECT_EQ(server.acceptCount(), accept_after_first_start);
    EXPECT_EQ(server.activeCount(), 3);

    pool.stop();

    ASSERT_TRUE(server.waitForActiveConnections(0, std::chrono::seconds(1)));
    
    pool.stop();

    EXPECT_EQ(server.activeCount(), 0);

    server.stop();
}
