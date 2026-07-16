#pragma once

#include "rpc_channel.h"
#include "rpc_controller.h"
#include "CallbackExecutor.h"
#include "rpc_future.h"

#include <google/protobuf/service.h>

#include <future>
#include <atomic>
#include <cstddef>
#include <memory>
#include <string>
#include <vector>

class RpcChannelPool : public google::protobuf::RpcChannel
{
public:
    using ChannelPtr = std::shared_ptr<MyRpcChannel>;
    using ChannelList = std::vector<ChannelPtr>;

public:
#ifdef MYRPC_ENABLE_TEST_HOOKS
    struct TestHooks
    {
        std::function<void(const std::vector<std::shared_ptr<MyRpcChannel>>)> before_snapshot_publish; 
    };

    void setTestHookForTest(std::shared_ptr<TestHooks> hooks)
    {
        test_hooks_ = hooks;        
    }

    size_t snapshotSizeForTest() const
    {
        auto snapshot = std::atomic_load_explicit(&channels_snapshot_, std::memory_order_acquire);
        return snapshot ? snapshot->size() : 0;
    }

    std::shared_ptr<ChannelList> snapshotForTest() const
    {
        return std::atomic_load_explicit(&channels_snapshot_, std::memory_order_acquire);
    }
private:
    std::shared_ptr<TestHooks> test_hooks_;
#endif

public:
    enum class State
    {
        kStopped,
        kRunning,
        kStopping
    };

    /*
    kStopped:
        snapshot 为空
        callback executor 停止
        不接受新调用

    kRunning:
        snapshot 已发布
        callback executor 正在运行
        接受新 CallMethod

    kStopping:
        不接受新调用
        snapshot 已摘除
        正在 stop channels
        callback executor 仍然活着，用来执行 stop 过程中产生的回调
    */
    RpcChannelPool(std::string ip, uint16_t port, size_t pool_size);

    ~RpcChannelPool();

    bool start();

    void stop();

    void CallMethod(const google::protobuf::MethodDescriptor *method,
                    google::protobuf::RpcController *controller,
                    const google::protobuf::Message *request,
                    google::protobuf::Message *response,
                    google::protobuf::Closure *done) override;

    void repairDeadChannels();

    size_t unavailableCount() const;

    void setTimeoutMs(int timeout_ms);
private:
    std::string ip_;
    uint16_t port_;
    size_t pool_size_;

    std::condition_variable lifecycle_cv_;
    std::mutex lifecycle_mutex_; // 保护 start() 和 stop() 串行化执行

    std::mutex repair_mutex_; // 保护channels_ 和 next_


    std::shared_ptr<ChannelList> channels_snapshot_;
    std::atomic<size_t> next_{0};

    std::unique_ptr<CallbackExecutor> callback_executor_;

    State pool_state_{State::kStopped};

    int active_calls_{0};

    int timeout_ms_ = 3000;
private:
    bool repairChannel(size_t index);
    bool repairChannelInCopy(ChannelList &new_channels,
                             size_t index,
                             std::vector<std::shared_ptr<MyRpcChannel>> &channels_to_stop,
                             std::vector<std::shared_ptr<MyRpcChannel>> &new_channels_stared);

    std::shared_ptr<MyRpcChannel> pickChannel();

    bool enterCall();
    void leaveCall();

public:
    template <typename Response, typename Request>
    std::future<RpcFutureResult<Response>> CallMethodFuture(
        const google::protobuf::MethodDescriptor *method,
        const Request &request)
    {
        using State = FutureCallState<Request, Response>;

        auto state = std::make_shared<State>();
        state->request = request;

        auto future = state->promise.get_future();
        auto* done = new FutureClosure<Request, Response> (state);

        if (!enterCall())
        {
            state->controller.SetFailed("RpcChannel is stopped");
            if (done)
            {
                done->Run();
            }
            return future;
        }

        auto ch = pickChannel();

        if (!ch)
        {

            state->controller.SetFailed("no available rpc channel");
            if (done)
            {
                done->Run();
            }

            leaveCall();
            return future;
        }

        ch->CallMethod(method, &state->controller, &state->request, &state->response, done);
        leaveCall();
        return future;
    }
};
