#include "CallbackExecutor.h"
#include "rpc_channel.h"
#include "rpc_closure.h"
#include "rpc_controller.h"
#include "tcpserver.h"
#include "user.pb.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
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

demo::LoginRequest makeLoginRequest(int index = 0)
{
    demo::LoginRequest request;
    request.set_name("haojun-" + std::to_string(index));
    request.set_password("123456");
    return request;
}

struct CompletionState
{
    std::mutex mutex;
    std::condition_variable cv;
    int done_count = 0;
    int failed_count = 0;
    int success_count = 0;

    void record(const std::shared_ptr<SimpleRpcController>& controller,
                const std::shared_ptr<demo::LoginResponse>& response)
    {
        std::lock_guard<std::mutex> lock(mutex);
        ++done_count;
        if (controller->Failed())
        {
            ++failed_count;
        }
        if (response->success())
        {
            ++success_count;
        }
        cv.notify_all();
    }
};

bool waitForDoneCount(CompletionState& state,
                      int expected,
                      std::chrono::milliseconds timeout)
{
    std::unique_lock<std::mutex> lock(state.mutex);
    return state.cv.wait_for(lock, timeout, [&] {
        return state.done_count == expected;
    });
}

int doneCountOf(CompletionState& state)
{
    std::lock_guard<std::mutex> lock(state.mutex);
    return state.done_count;
}

bool startChannel(const std::shared_ptr<MyRpcChannel>& channel,
                  int timeout_ms)
{
    channel->setTimeoutMs(timeout_ms);
    return channel->start();
}
}

TEST(RpcChannelTimeoutTest, SyncCallShouldTimeoutAndReturnWithoutBlocking)
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

    auto channel = MyRpcChannel::create(
        "127.0.0.1", server.port(), &callback_executor);
    ASSERT_TRUE(startChannel(channel, 50));
    ASSERT_TRUE(server.waitForAcceptCount(1, std::chrono::seconds(1)));

    demo::UserService_Stub stub(channel.get());
    auto future = std::async(std::launch::async, [&] {
        demo::LoginRequest request = makeLoginRequest();
        demo::LoginResponse response;
        SimpleRpcController controller;

        stub.Login(&controller, &request, &response, nullptr);

        return std::make_pair(controller.Failed(), controller.ErrorText());
    });

    auto status = future.wait_for(std::chrono::seconds(2));
    if (status != std::future_status::ready)
    {
        channel->stop();
        release_response.store(true, std::memory_order_release);
        FAIL() << "sync CallMethod did not return after rpc timeout";
    }

    auto result = future.get();
    EXPECT_TRUE(result.first);
    EXPECT_FALSE(result.second.empty());

    release_response.store(true, std::memory_order_release);
    channel->stop();
    callback_executor.stop();
    server.stop();
}

TEST(RpcChannelTimeoutTest, AsyncTimeoutsShouldCompleteEveryPendingCallExactlyOnce)
{
    constexpr int kRequestCount = 32;

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

    auto channel = MyRpcChannel::create(
        "127.0.0.1", server.port(), &callback_executor);
    ASSERT_TRUE(startChannel(channel, 50));
    ASSERT_TRUE(server.waitForAcceptCount(1, std::chrono::seconds(1)));

    demo::UserService_Stub stub(channel.get());
    auto state = std::make_shared<CompletionState>();

    for (int i = 0; i < kRequestCount; ++i)
    {
        auto request = std::make_shared<demo::LoginRequest>(makeLoginRequest(i));
        auto response = std::make_shared<demo::LoginResponse>();
        auto controller = std::make_shared<SimpleRpcController>();

        auto* done = SendResponseClosure(
            [state, controller, response, request] {
                state->record(controller, response);
            });

        stub.Login(controller.get(), request.get(), response.get(), done);
    }

    if (!waitForDoneCount(*state, kRequestCount, std::chrono::seconds(3)))
    {
        channel->stop();
        release_response.store(true, std::memory_order_release);
        FAIL() << "not all async pending calls completed after rpc timeout";
    }

    {
        std::lock_guard<std::mutex> lock(state->mutex);
        EXPECT_EQ(state->done_count, kRequestCount);
        EXPECT_EQ(state->failed_count, kRequestCount);
        EXPECT_EQ(state->success_count, 0);
    }

    release_response.store(true, std::memory_order_release);
    ASSERT_TRUE(server.waitForTotalRequests(kRequestCount,
                                            std::chrono::seconds(1)));

    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    EXPECT_EQ(doneCountOf(*state), kRequestCount);

    channel->stop();
    callback_executor.stop();
    server.stop();
}

TEST(RpcChannelTimeoutTest, SuccessfulCallsShouldNotRunDoneAgainWhenTimersExpire)
{
    constexpr int kRequestCount = 8;

    ControlledTcpServer server(0, buildLoginResponseBody);
    ASSERT_TRUE(server.start());

    CallbackExecutor callback_executor;
    callback_executor.start();

    auto channel = MyRpcChannel::create(
        "127.0.0.1", server.port(), &callback_executor);
    ASSERT_TRUE(startChannel(channel, 500));
    ASSERT_TRUE(server.waitForAcceptCount(1, std::chrono::seconds(1)));

    demo::UserService_Stub stub(channel.get());
    auto state = std::make_shared<CompletionState>();

    for (int i = 0; i < kRequestCount; ++i)
    {
        auto request = std::make_shared<demo::LoginRequest>(makeLoginRequest(i));
        auto response = std::make_shared<demo::LoginResponse>();
        auto controller = std::make_shared<SimpleRpcController>();

        auto* done = SendResponseClosure(
            [state, controller, response, request] {
                state->record(controller, response);
            });

        stub.Login(controller.get(), request.get(), response.get(), done);
    }

    ASSERT_TRUE(waitForDoneCount(*state, kRequestCount,
                                 std::chrono::seconds(3)));

    {
        std::lock_guard<std::mutex> lock(state->mutex);
        EXPECT_EQ(state->done_count, kRequestCount);
        EXPECT_EQ(state->failed_count, 0);
        EXPECT_EQ(state->success_count, kRequestCount);
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(650));
    EXPECT_EQ(doneCountOf(*state), kRequestCount);

    channel->stop();
    callback_executor.stop();
    server.stop();
}

TEST(RpcChannelTimeoutTest, ConcurrentAsyncSubmitAndStopShouldNotLoseDone)
{
    constexpr int kThreadCount = 4;
    constexpr int kCallsPerThread = 20;
    constexpr int kRequestCount = kThreadCount * kCallsPerThread;

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

    auto channel = MyRpcChannel::create(
        "127.0.0.1", server.port(), &callback_executor);
    ASSERT_TRUE(startChannel(channel, 80));
    ASSERT_TRUE(server.waitForAcceptCount(1, std::chrono::seconds(1)));

    demo::UserService_Stub stub(channel.get());
    auto state = std::make_shared<CompletionState>();

    std::vector<std::future<void>> workers;
    workers.reserve(kThreadCount);

    for (int t = 0; t < kThreadCount; ++t)
    {
        workers.push_back(std::async(std::launch::async, [&, t] {
            for (int i = 0; i < kCallsPerThread; ++i)
            {
                int request_index = t * kCallsPerThread + i;
                auto request =
                    std::make_shared<demo::LoginRequest>(
                        makeLoginRequest(request_index));
                auto response = std::make_shared<demo::LoginResponse>();
                auto controller = std::make_shared<SimpleRpcController>();

                auto* done = SendResponseClosure(
                    [state, controller, response, request] {
                        state->record(controller, response);
                    });

                stub.Login(controller.get(), request.get(), response.get(), done);
            }
        }));
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    auto stop_future = std::async(std::launch::async, [&] {
        channel->stop();
    });

    for (auto& worker : workers)
    {
        auto status = worker.wait_for(std::chrono::seconds(2));
        if (status != std::future_status::ready)
        {
            release_response.store(true, std::memory_order_release);
            channel->stop();
            FAIL() << "async submitter blocked while channel was stopping";
        }
        worker.get();
    }

    ASSERT_EQ(stop_future.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    stop_future.get();

    if (!waitForDoneCount(*state, kRequestCount, std::chrono::seconds(2)))
    {
        release_response.store(true, std::memory_order_release);
        FAIL() << "not every async CallMethod attempt ran done exactly once";
    }

    EXPECT_EQ(doneCountOf(*state), kRequestCount);

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    EXPECT_EQ(doneCountOf(*state), kRequestCount);

    release_response.store(true, std::memory_order_release);
    callback_executor.stop();
    server.stop();
}
