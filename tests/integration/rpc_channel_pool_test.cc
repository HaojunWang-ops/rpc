#include "tcpserver.h"
#include "rpc_closure.h"
#include "rpc_channel_pool.h"
#include "user.pb.h"

#include <gtest/gtest.h>
#include <string>
#include <chrono>
#include <future>
#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

std::string buildEmptyResponseBody(const myrpc::RpcHeader& req_header,
                const std::string& request_body)
{
    std::string response_body("");
    return response_body;
}

std::string buildLoginResponseBody(const myrpc::RpcHeader&,
                                   const std::string&)
{
    demo::LoginResponse response;
    response.set_code(0);
    response.set_message("login success");
    response.set_success(true);
    return response.SerializeAsString();
}

demo::LoginRequest makeLoginRequest()
{
    demo::LoginRequest req;
    req.set_name("haojun");
    req.set_password("123");
    return req;
}

const google::protobuf::MethodDescriptor* loginMethod()
{
    return demo::UserService::descriptor()->FindMethodByName("Login");
}

struct AsyncCallState
{
    std::mutex mutex;
    std::condition_variable cv;
    int done_count = 0;
    bool controller_failed = false;
    std::string error_text;
};

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

static bool doOnePoolLoginCall(RpcChannelPool& pool)
{
    demo::LoginRequest req = makeLoginRequest();
    demo::LoginResponse resp;
    SimpleRpcController controller;

    demo::UserService_Stub stub(&pool);

    stub.Login(&controller, &req, &resp, nullptr);

    return !controller.Failed();
}

// start 成功后应建立与 pool_size 相同数量的持久连接。
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

// 轮询选择 channel 时，请求应分布到 pool 中的多个连接。
TEST(RpcChannelPoolTest, RequestsShouldBeDistributedAcrossConnections)
{
    ControlledTcpServer server(0, buildLoginResponseBody);
    ASSERT_TRUE(server.start());

    RpcChannelPool pool("127.0.0.1", server.port(), 4);
    ASSERT_TRUE(pool.start());
    ASSERT_TRUE(server.waitForAcceptCount(4, std::chrono::seconds(1)));

    for (int i = 0; i < 8; ++i)
    {
        demo::LoginRequest req = makeLoginRequest();
        demo::LoginResponse resp;
        SimpleRpcController controller;

        demo::UserService_Stub stub(&pool);
        stub.Login(&controller, &req, &resp, nullptr);

        ASSERT_FALSE(controller.Failed()) << controller.ErrorText();
        ASSERT_TRUE(resp.success());
    }

    ASSERT_TRUE(server.waitForTotalRequests(8, std::chrono::seconds(1)));

    for (size_t i = 0; i < 4; ++i)
    {
        EXPECT_GE(server.requestCountOf(i), 1);
    }

    pool.stop();
    server.stop();
}

// reader 感知断连后，repair 应替换不可用 channel 并恢复可用连接数。
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

// pool stop 必须关闭 snapshot 中的全部连接。
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

// 断连、repair 与多线程提交并行时，提交路径不能阻塞或崩溃。
TEST(RpcChannelPoolTest, ConcurrentCallAndRepairShouldNotCrash)
{
    // 同时制造断连、repair 和多线程提交，验证提交路径不会因 snapshot 替换而阻塞或崩溃。
    ControlledTcpServer server(0, buildLoginResponseBody);
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
                bool ok = doOnePoolLoginCall(pool);

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

// stop 后的同步和异步提交都要失败，异步 done 仍只能执行一次。
TEST(RpcChannelPoolTest, CallAfterStopShouldFailAndCallDoneOnce)
{
    ControlledTcpServer server(0, buildLoginResponseBody);
    ASSERT_TRUE(server.start());

    RpcChannelPool pool("127.0.0.1", server.port(), 1);
    ASSERT_TRUE(pool.start());
    ASSERT_TRUE(server.waitForAcceptCount(1, std::chrono::seconds(1)));

    pool.stop();

    demo::UserService_Stub stub(&pool);
    auto request = std::make_shared<demo::LoginRequest>(makeLoginRequest());
    auto response = std::make_shared<demo::LoginResponse>();
    auto controller = std::make_shared<SimpleRpcController>();
    auto state = std::make_shared<AsyncCallState>();

    auto* done = SendResponseClosure(
        [request, response, controller, state] {
            std::lock_guard<std::mutex> lock(state->mutex);
            ++state->done_count;
            state->controller_failed = controller->Failed();
            state->error_text = controller->ErrorText();
            state->cv.notify_one();
        });

    stub.Login(controller.get(), request.get(), response.get(), done);

    {
        std::unique_lock<std::mutex> lock(state->mutex);
        ASSERT_TRUE(state->cv.wait_for(lock, std::chrono::seconds(1), [&] {
            return state->done_count == 1;
        }));
    }

    EXPECT_EQ(state->done_count, 1);
    EXPECT_TRUE(state->controller_failed);
    EXPECT_FALSE(state->error_text.empty());

    auto request_without_controller =
        std::make_shared<demo::LoginRequest>(makeLoginRequest());
    auto response_without_controller =
        std::make_shared<demo::LoginResponse>();
    auto no_controller_state = std::make_shared<AsyncCallState>();

    auto* no_controller_done = SendResponseClosure(
        [request_without_controller, response_without_controller,
         no_controller_state] {
            std::lock_guard<std::mutex> lock(no_controller_state->mutex);
            ++no_controller_state->done_count;
            no_controller_state->cv.notify_one();
        });

    pool.CallMethod(loginMethod(),
                    nullptr,
                    request_without_controller.get(),
                    response_without_controller.get(),
                    no_controller_done);

    {
        std::unique_lock<std::mutex> lock(no_controller_state->mutex);
        ASSERT_TRUE(no_controller_state->cv.wait_for(
            lock, std::chrono::seconds(1), [&] {
                return no_controller_state->done_count == 1;
            }));
    }

    EXPECT_EQ(no_controller_state->done_count, 1);

    server.stop();
}

// 对端关闭后 pool 应识别无可用连接，并且 stop 不依赖对端继续读写。
TEST(RpcChannelPoolTest, NoAvailableChannelShouldFailAndStopShouldNotBlock)
{
    ControlledTcpServer server(0, buildLoginResponseBody);
    ASSERT_TRUE(server.start());

    RpcChannelPool pool("127.0.0.1", server.port(), 1);
    ASSERT_TRUE(pool.start());
    ASSERT_TRUE(server.waitForActiveConnections(1, std::chrono::seconds(1)));

    server.stop();
    ASSERT_TRUE(waitUntil(std::chrono::seconds(2), [&] {
        return pool.unavailableCount() >= 1;
    }));

    demo::UserService_Stub stub(&pool);
    demo::LoginRequest req = makeLoginRequest();
    demo::LoginResponse resp;
    SimpleRpcController controller;

    stub.Login(&controller, &req, &resp, nullptr);

    EXPECT_TRUE(controller.Failed());
    EXPECT_FALSE(controller.ErrorText().empty());

    auto stop_future = std::async(std::launch::async, [&] {
        pool.stop();
    });

    ASSERT_EQ(stop_future.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    stop_future.get();
}

// stop 面对 in-flight 异步请求时，必须 drain 每个 done 且不重复执行。
TEST(RpcChannelPoolTest, StopWhileAsyncCallsInFlightShouldDrainDoneOnce)
{
    constexpr int kPoolSize = 4;
    constexpr int kRequestCount = 20;

    // 服务端先不返回响应，确保 stop() 面对的是仍在 pending 表中的异步请求。
    std::atomic<bool> release_response{false};
    ControlledTcpServer server(0, [&](const myrpc::RpcHeader& header,
                                      const std::string& body) {
        while (!release_response.load(std::memory_order_acquire))
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        return buildLoginResponseBody(header, body);
    });
    ASSERT_TRUE(server.start());

    RpcChannelPool pool("127.0.0.1", server.port(), kPoolSize);
    ASSERT_TRUE(pool.start());
    ASSERT_TRUE(server.waitForAcceptCount(kPoolSize, std::chrono::seconds(1)));

    demo::UserService_Stub stub(&pool);

    std::mutex mutex;
    std::condition_variable cv;
    int done_count = 0;
    int failed_count = 0;

    for (int i = 0; i < kRequestCount; ++i)
    {
        auto request = std::make_shared<demo::LoginRequest>(makeLoginRequest());
        auto response = std::make_shared<demo::LoginResponse>();
        auto controller = std::make_shared<SimpleRpcController>();

        auto* done = SendResponseClosure(
            [request, response, controller, &mutex, &cv,
             &done_count, &failed_count] {
                std::lock_guard<std::mutex> lock(mutex);
                ++done_count;
                if (controller->Failed())
                {
                    ++failed_count;
                }
                cv.notify_all();
            });

        stub.Login(controller.get(), request.get(), response.get(), done);
    }

    ASSERT_TRUE(server.waitForTotalRequests(kPoolSize, std::chrono::seconds(1)));

    auto stop_future = std::async(std::launch::async, [&] {
        pool.stop();
    });

    auto stop_status = stop_future.wait_for(std::chrono::seconds(3));
    if (stop_status != std::future_status::ready)
    {
        release_response.store(true, std::memory_order_release);
    }
    ASSERT_EQ(stop_status, std::future_status::ready);
    stop_future.get();

    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(1), [&] {
            return done_count == kRequestCount;
        }));
    }

    EXPECT_EQ(done_count, kRequestCount);
    EXPECT_EQ(failed_count, kRequestCount);

    release_response.store(true, std::memory_order_release);
    server.stop();
}

// callback worker 内 stop 被拒绝以避免 self-join，pool 应继续可用。
TEST(RpcChannelPoolTest, StopFromCallbackWorkerShouldBeIgnoredAndKeepPoolRunning)
{
    ControlledTcpServer server(0, buildLoginResponseBody);
    ASSERT_TRUE(server.start());

    RpcChannelPool pool("127.0.0.1", server.port(), 1);
    ASSERT_TRUE(pool.start());
    ASSERT_TRUE(server.waitForAcceptCount(1, std::chrono::seconds(1)));

    demo::UserService_Stub stub(&pool);
    auto request = std::make_shared<demo::LoginRequest>(makeLoginRequest());
    auto response = std::make_shared<demo::LoginResponse>();
    auto controller = std::make_shared<SimpleRpcController>();

    std::mutex mutex;
    std::condition_variable cv;
    bool done_ran = false;
    bool callback_saw_failure = false;

    auto* done = SendResponseClosure(
        [&pool, request, response, controller, &mutex, &cv,
         &done_ran, &callback_saw_failure] {
            pool.stop();

            std::lock_guard<std::mutex> lock(mutex);
            done_ran = true;
            callback_saw_failure = controller->Failed();
            cv.notify_all();
        });

    stub.Login(controller.get(), request.get(), response.get(), done);

    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(1), [&] {
            return done_ran;
        }));
    }

    EXPECT_FALSE(callback_saw_failure);
    EXPECT_TRUE(doOnePoolLoginCall(pool));

    pool.stop();
    ASSERT_TRUE(server.waitForActiveConnections(0, std::chrono::seconds(1)));
    server.stop();
}

// 时间驱动地并发 repair/stop，检查连接和后台 repair 线程均能收束。
TEST(RpcChannelPoolTest, ConcurrentRepairAndStopShouldNotLeakConnections)
{
    ControlledTcpServer server(0, buildLoginResponseBody);
    ASSERT_TRUE(server.start());

    RpcChannelPool pool("127.0.0.1", server.port(), 4);
    ASSERT_TRUE(pool.start());
    ASSERT_TRUE(server.waitForAcceptCount(4, std::chrono::seconds(1)));

    // 该循环是时间驱动的广覆盖检查；精确的发布/停止交错由 race test 覆盖。
    std::atomic<bool> keep_repairing{true};
    auto repair_worker = std::async(std::launch::async, [&] {
        while (keep_repairing.load(std::memory_order_acquire))
        {
            pool.repairDeadChannels();
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    });

    for (int i = 0; i < 20; ++i)
    {
        server.closeOneConnection();
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }

    auto stop_future = std::async(std::launch::async, [&] {
        pool.stop();
    });

    ASSERT_EQ(stop_future.wait_for(std::chrono::seconds(3)),
              std::future_status::ready);
    stop_future.get();

    keep_repairing.store(false, std::memory_order_release);
    ASSERT_EQ(repair_worker.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    repair_worker.get();

    ASSERT_TRUE(server.waitForActiveConnections(0, std::chrono::seconds(2)));
    server.stop();
}

// 重复 start/stop 不应重复创建资源、泄漏连接或阻塞调用方。
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
