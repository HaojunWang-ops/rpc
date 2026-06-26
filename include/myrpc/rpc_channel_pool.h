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
    enum class State
    {
        kStopped,
        KRunning,
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

    bool isRunning() const;

private:
    std::string ip_;
    uint16_t port_;
    size_t pool_size_;

    std::condition_variable lifecycle_cv_;
    std::mutex lifecycle_mutex_; // 保护 start() 和 stop() 串行化执行

    std::mutex repair_mutex_; // 保护channels_ 和 next_

    using ChannelPtr = std::shared_ptr<MyRpcChannel>;
    using ChannelList = std::vector<ChannelPtr>;

    std::shared_ptr<ChannelList> channels_snapshot_;
    std::atomic<size_t> next_{0};

    std::unique_ptr<CallbackExecutor> callback_executor_;

    State pool_state_{State::kStopped};

    int active_calls_{0};

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

// Threading model:
// 1. CallMethod() can be called concurrently.
// 2. pickChannel() is lock-free with atomic snapshot.
// 3. repairDeadChannels() is serialized by repair_mutex_.
// 4. channels_snapshot_ is immutable after publication.
// 5. Do not modify the vector in-place after publishing it.

/*
RpcChannelPool owns CallbackExecutor.
RpcChannelPool owns channels.
MyRpcChannel does not own CallbackExecutor.
MyRpcChannel only holds a non-owning pointer/reference to CallbackExecutor.
RpcChannelPool stops all channels before stopping CallbackExecutor.



CallbackExecutor lifecycle rule:

CallbackExecutor is owned by RpcChannelPool or a higher-level RpcClient/RpcRuntime.
MyRpcChannel only posts completion callbacks to it and never stops it.

CallbackExecutor::stop() must be called by the owner thread after all channels
have been stopped. User callbacks must not call CallbackExecutor::stop().
If a callback wants to shut down the client or pool, it should request shutdown
and let the owner perform the actual stop outside the callback worker thread.

规则：
1. RpcChannelPool owns CallbackExecutor，使用 unique_ptr。
2. RpcChannelPool owns all MyRpcChannel snapshots。
3. MyRpcChannel 使用 CallbackExecutor*，不拥有 executor。
4. MyRpcChannel 不对外暴露。
5. RpcChannelPool 自己继承 RpcChannel，对外提供 CallMethod。
6. stop() 后 pool 拒绝所有新调用，并 inline 执行错误 done。
7. channel stop 发生在 executor stop 之前。
8. executor stop drain 已投递 callback。
9. repair 不在锁内 connect/start/stop。
10. response frame 有最大大小限制。
*/