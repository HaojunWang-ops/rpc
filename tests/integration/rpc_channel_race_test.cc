#include "tcpserver.h"
#include "rpc_closure.h"
#include "rpc_channel_pool.h"
#include "user.pb.h"
#include "rpc_controller.h"
#include "rpc_channel.h"

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

class LambdaClosure final : public google::protobuf::Closure
{
public:
    explicit LambdaClosure(std::function<void()> fn)
        :fn_ (std::move(fn))
    {
    }

    void Run() override
    {
        fn_();
    }
private:
    std::function<void()> fn_;
};

bool startChannel(const std::shared_ptr<MyRpcChannel>& channel, int timeout_ms)
{
    channel->setTimeoutMs(timeout_ms);
    return channel->start();
}

std::string buildLoginResponse(const myrpc::RpcHeader&,
                                const std::string&)
{
    demo::LoginResponse response;
    response.set_code(0);
    response.set_message("ok");
    response.set_success(true);
    return response.SerializeAsString();
}

class PendingTakeRaceState
{
public:
    using Path = MyRpcChannel::PendingTakePath;

    void beforeTake(Path path)
    {
        std::unique_lock<std::mutex> lock(mutex_);

        if (path == Path::kResponse)
        {
            response_arrived_ = true;
        }
        if (path == Path::kTimeout)
        {
            timeout_arrived_ = true;
        }

        cv_.notify_all();
        cv_.wait(lock, [&]{
            return released_;
        });
    }

    void afterTake(Path path, bool taken)
    {
        std::unique_lock<std::mutex> lock(mutex_);

        ++attempt_count_;

        if (taken)
        {
            ++success_count_;
            if (path == Path::kResponse)
            {
                ++response_success_count_;
            }
            if (path == Path::kTimeout)
            {
                ++timeout_success_count_;
            }
        }

        cv_.notify_all();
    }

    bool waitUtilBothArrived(std::chrono::milliseconds timeout)
    {
        std::unique_lock<std::mutex> lock(mutex_);

        return cv_.wait_for(lock, timeout, [&]{
            return timeout_arrived_ && response_arrived_;
        });
    }

    void release()
    {
        std::unique_lock<std::mutex> lock(mutex_);
        released_ = true;
        cv_.notify_all();
    }

    bool waitUtilBothAttempted(std::chrono::milliseconds timeout)
    {
        std::unique_lock<std::mutex> lock(mutex_);

        return cv_.wait_for(lock, timeout, [&]{
            return attempt_count_ == 2;
        });
    }

    int successCount() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return success_count_;
    }

    int timeoutSuccessCount() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return timeout_success_count_;
    }

    int responseSuccessCount() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return response_success_count_;
    }

private:
    mutable std::mutex mutex_;
    std::condition_variable cv_;

    bool timeout_arrived_ = false;
    bool response_arrived_ = false;
    bool released_ = false;

    int attempt_count_ = 0;
    int success_count_ = 0;
    int timeout_success_count_ = 0;
    int response_success_count_ = 0;
};

// 固定 pending 已接受与 stop 竞争的窗口，验证 timeout 注册失败路径不会重复完成。
TEST(RpcChannelRaceTest, StopAfterPendingAcceptShouldCompleteCallExactlyOnce)
{
    using namespace std::chrono_literals;

    ControlledTcpServer server(0, [](
        const myrpc::RpcHeader&,
        const std::string&)
    {
        return std::string{};
    });

    ASSERT_TRUE(server.start());

    CallbackExecutor callback_executor;
    callback_executor.start();

    auto channel = MyRpcChannel::create("127.0.0.1", server.port(), &callback_executor);

    ASSERT_TRUE(startChannel(channel, 80));

    std::promise<void> reached_pending_promise;
    auto reached_pending_ = reached_pending_promise.get_future();

    std::promise<void> release_submit_promise;
    auto release_submit = release_submit_promise.get_future().share();

    auto hooks = std::make_shared<MyRpcChannel::RpcChannelTestHooks>();

    hooks->after_pending_added = [&](uint64_t){
        // 固定在 pending 已接受、timeout 尚未注册的窗口，让 stop 先执行。
        reached_pending_promise.set_value();

        release_submit.wait();
    };

    channel->setTestHooksForTest(hooks);

    demo::UserService_Stub stub(channel.get());

    demo::LoginRequest request;
    demo::LoginResponse response;
    SimpleRpcController controller;

    request.set_name("race-test");

    std::atomic<int> done_count{0};

    std::promise<void> first_done_promise;
    auto first_done = first_done_promise.get_future();

    LambdaClosure done([&]{
        const int old = done_count.fetch_add(1, std::memory_order_acq_rel);

        if (old == 0)
        {
            first_done_promise.set_value();
        }
    });

    auto submit_future = std::async(std::launch::async, [&]{
        stub.Login(&controller, &request, &response, &done);
    });

    ASSERT_EQ(reached_pending_.wait_for(1s), std::future_status::ready);

    channel->stop();

    release_submit_promise.set_value();

    ASSERT_EQ(submit_future.wait_for(1s), std::future_status::ready);

    submit_future.get();

    ASSERT_EQ(first_done.wait_for(1s), std::future_status::ready);

    callback_executor.stop();

    EXPECT_EQ(done_count.load(std::memory_order_acquire), 1);

    EXPECT_TRUE(controller.Failed());
    EXPECT_EQ(channel->pendingSizeForTest(), 0);
}

// 固定 response 和 timeout 同时争夺 take() 的交错，验证唯一完成权。
TEST(RpcChannelRaceTest, TimeoutAndResponseRaceShouldCompleteExactlyOnce)
{
    using namespace std::chrono_literals;

    ControlledTcpServer server(0, buildLoginResponse);

    ASSERT_TRUE(server.start());

    CallbackExecutor callback_executor;
    callback_executor.start();

    auto race_state = std::make_shared<PendingTakeRaceState>();
    
    auto hooks = std::make_shared<MyRpcChannel::RpcChannelTestHooks>();

    hooks->before_pending_take = [race_state](MyRpcChannel::PendingTakePath path, uint64_t){
        // reader 和 timeout 都在 take 前汇合，再同时竞争唯一完成权。
        race_state->beforeTake(path);
    };
    hooks->after_pending_take = [race_state](MyRpcChannel::PendingTakePath path, uint64_t ,bool taken){
        race_state->afterTake(path, taken);
    };

    auto channel = MyRpcChannel::create("127.0.0.1", server.port(), &callback_executor);
    channel->setTestHooksForTest(hooks);

    ASSERT_TRUE(startChannel(channel, 80));

    demo::UserService_Stub stub(channel.get());

    demo::LoginRequest request;
    demo::LoginResponse response;
    SimpleRpcController controller;

    request.set_name("timeout-reponse-race");

    std::mutex done_mutex;
    std::condition_variable done_cv;
    int done_count = 0;

    LambdaClosure done([&]{
        {
            std::lock_guard<std::mutex> lock(done_mutex);
            ++done_count;  
        }
        done_cv.notify_all();
    });

    stub.Login(&controller, &request, &response, &done);

    ASSERT_TRUE(race_state->waitUtilBothArrived(2s));
    
    // 两条路径都已到达同步点后才释放，避免测试依赖 sleep 猜测调度时序。
    race_state->release();

    ASSERT_TRUE(race_state->waitUtilBothAttempted(2s));
    
    {
        std::unique_lock<std::mutex> lock(done_mutex);

        done_cv.wait_for(lock, 2s, [&]{
            return done_count >= 1;
        });
    }

    callback_executor.stop();

    EXPECT_EQ(race_state->successCount(), 1);
    EXPECT_EQ(race_state->timeoutSuccessCount() + race_state->responseSuccessCount(), 1);
    EXPECT_EQ(done_count, 1);
    EXPECT_EQ(channel->pendingSizeForTest(), 0);

    if (race_state->timeoutSuccessCount() == 1)
    {
        EXPECT_TRUE(controller.Failed());
        EXPECT_FALSE(controller.error_text().empty());
    }
    else if (race_state->responseSuccessCount() == 1)
    {
        EXPECT_FALSE(controller.Failed());
        EXPECT_TRUE(response.success());
    }
}
