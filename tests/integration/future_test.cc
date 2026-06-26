#include "CallbackExecutor.h"
#include "tcpserver.h"
#include "rpc_closure.h"
#include "rpc_channel.h"
#include "rpc_channel_pool.h"
#include "user.pb.h"

#include <gtest/gtest.h>
#include <string>
#include <chrono>
#include <functional>
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

TEST(RpcFutureTest, ChannelFutureShouldResolveSuccessfulResponse)
{
    ControlledTcpServer server(0, buildLoginResponseBody);
    ASSERT_TRUE(server.start());

    CallbackExecutor callback_executor;
    callback_executor.start();

    auto channel = std::make_shared<MyRpcChannel>(
        "127.0.0.1", server.port(), &callback_executor);
    ASSERT_TRUE(channel->start());
    ASSERT_TRUE(server.waitForAcceptCount(1, std::chrono::seconds(1)));

    auto future = channel->CallMethodFuture<demo::LoginResponse>(
        loginMethod(),
        makeLoginRequest());

    ASSERT_EQ(future.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);

    auto result = future.get();
    EXPECT_TRUE(result.ok) << result.error_text;
    EXPECT_EQ(result.error_code, myrpc::RPC_OK);
    EXPECT_TRUE(result.response.success());
    EXPECT_EQ(result.response.message(), "login success");
    EXPECT_TRUE(server.waitForTotalRequests(1, std::chrono::seconds(1)));

    channel->stop();
    callback_executor.stop();
    server.stop();
}

TEST(RpcFutureTest, ChannelFutureTimeoutShouldResolveError)
{
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

    CallbackExecutor callback_executor;
    callback_executor.start();

    auto channel = std::make_shared<MyRpcChannel>(
        "127.0.0.1", server.port(), &callback_executor);
    channel->setTimeoutMs(50);
    ASSERT_TRUE(channel->start());
    ASSERT_TRUE(server.waitForAcceptCount(1, std::chrono::seconds(1)));

    auto future = channel->CallMethodFuture<demo::LoginResponse>(
        loginMethod(),
        makeLoginRequest());

    auto status = future.wait_for(std::chrono::seconds(2));
    if (status != std::future_status::ready)
    {
        release_response.store(true, std::memory_order_release);
        channel->stop();
        callback_executor.stop();
        server.stop();
        FAIL() << "future did not resolve after rpc timeout";
    }

    auto result = future.get();
    EXPECT_FALSE(result.ok);
    EXPECT_NE(result.error_code, myrpc::RPC_OK);
    EXPECT_FALSE(result.error_text.empty());

    release_response.store(true, std::memory_order_release);
    channel->stop();
    callback_executor.stop();
    server.stop();
}

TEST(RpcFutureTest, ChannelFutureConcurrentCallsShouldAllComplete)
{
    ControlledTcpServer server(0, buildLoginResponseBody);
    ASSERT_TRUE(server.start());

    CallbackExecutor callback_executor;
    callback_executor.start();

    auto channel = std::make_shared<MyRpcChannel>(
        "127.0.0.1", server.port(), &callback_executor);
    ASSERT_TRUE(channel->start());
    ASSERT_TRUE(server.waitForAcceptCount(1, std::chrono::seconds(1)));

    constexpr int kThreadCount = 8;
    constexpr int kCallsPerThread = 12;
    constexpr int kTotalCalls = kThreadCount * kCallsPerThread;

    std::atomic<int> success_count{0};
    std::atomic<int> fail_count{0};
    std::atomic<int> timeout_count{0};

    std::vector<std::future<void>> workers;
    workers.reserve(kThreadCount);

    for (int t = 0; t < kThreadCount; ++t)
    {
        workers.push_back(std::async(std::launch::async, [&, t] {
            for (int i = 0; i < kCallsPerThread; ++i)
            {
                auto request = makeLoginRequest();
                request.set_name("future-" + std::to_string(t) + "-" +
                                 std::to_string(i));

                auto future = channel->CallMethodFuture<demo::LoginResponse>(
                    loginMethod(),
                    request);

                if (future.wait_for(std::chrono::seconds(3)) !=
                    std::future_status::ready)
                {
                    timeout_count.fetch_add(1, std::memory_order_relaxed);
                    continue;
                }

                auto result = future.get();
                if (result.ok && result.response.success())
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
        ASSERT_EQ(worker.wait_for(std::chrono::seconds(5)),
                  std::future_status::ready);
        worker.get();
    }

    EXPECT_EQ(timeout_count.load(std::memory_order_relaxed), 0);
    EXPECT_EQ(fail_count.load(std::memory_order_relaxed), 0);
    EXPECT_EQ(success_count.load(std::memory_order_relaxed), kTotalCalls);
    EXPECT_TRUE(server.waitForTotalRequests(kTotalCalls,
                                            std::chrono::seconds(1)));

    channel->stop();
    callback_executor.stop();
    server.stop();
}

TEST(RpcFutureTest, ChannelStopShouldResolveInFlightFutures)
{
    constexpr int kRequestCount = 8;

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

    CallbackExecutor callback_executor;
    callback_executor.start();

    auto channel = std::make_shared<MyRpcChannel>(
        "127.0.0.1", server.port(), &callback_executor);
    ASSERT_TRUE(channel->start());
    ASSERT_TRUE(server.waitForAcceptCount(1, std::chrono::seconds(1)));

    std::vector<std::future<RpcFutureResult<demo::LoginResponse>>> futures;
    futures.reserve(kRequestCount);

    for (int i = 0; i < kRequestCount; ++i)
    {
        futures.push_back(channel->CallMethodFuture<demo::LoginResponse>(
            loginMethod(),
            makeLoginRequest()));
    }

    ASSERT_TRUE(server.waitForTotalRequests(1, std::chrono::seconds(1)));

    auto stop_future = std::async(std::launch::async, [&] {
        channel->stop();
    });

    auto stop_status = stop_future.wait_for(std::chrono::seconds(2));
    if (stop_status != std::future_status::ready)
    {
        release_response.store(true, std::memory_order_release);
        callback_executor.stop();
        server.stop();
        FAIL() << "channel stop blocked with future calls in flight";
    }
    stop_future.get();

    for (auto& future : futures)
    {
        ASSERT_EQ(future.wait_for(std::chrono::seconds(1)),
                  std::future_status::ready);

        auto result = future.get();
        EXPECT_FALSE(result.ok);
        EXPECT_FALSE(result.error_text.empty());
    }

    release_response.store(true, std::memory_order_release);
    callback_executor.stop();
    server.stop();
}

TEST(RpcFutureTest, PoolFutureShouldResolveSuccessfulResponse)
{
    ControlledTcpServer server(0, buildLoginResponseBody);
    ASSERT_TRUE(server.start());

    RpcChannelPool pool("127.0.0.1", server.port(), 2);
    ASSERT_TRUE(pool.start());
    ASSERT_TRUE(server.waitForAcceptCount(2, std::chrono::seconds(1)));

    auto future = pool.CallMethodFuture<demo::LoginResponse>(
        loginMethod(),
        makeLoginRequest());

    ASSERT_EQ(future.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);

    auto result = future.get();
    EXPECT_TRUE(result.ok) << result.error_text;
    EXPECT_EQ(result.error_code, myrpc::RPC_OK);
    EXPECT_TRUE(result.response.success());
    EXPECT_TRUE(server.waitForTotalRequests(1, std::chrono::seconds(1)));

    pool.stop();
    server.stop();
}

TEST(RpcFutureTest, PoolFutureAfterStopShouldReturnReadyError)
{
    ControlledTcpServer server(0, buildLoginResponseBody);
    ASSERT_TRUE(server.start());

    RpcChannelPool pool("127.0.0.1", server.port(), 1);
    ASSERT_TRUE(pool.start());
    ASSERT_TRUE(server.waitForAcceptCount(1, std::chrono::seconds(1)));

    pool.stop();

    auto future = pool.CallMethodFuture<demo::LoginResponse>(
        loginMethod(),
        makeLoginRequest());

    ASSERT_EQ(future.wait_for(std::chrono::seconds(1)),
              std::future_status::ready);

    auto result = future.get();
    EXPECT_FALSE(result.ok);
    EXPECT_FALSE(result.error_text.empty());

    server.stop();
}
