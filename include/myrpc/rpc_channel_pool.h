#pragma once

#include "rpc_channel.h"
#include "rpc_controller.h"
#include "CallbackExecutor.h"

#include <google/protobuf/service.h>

#include <atomic>
#include <cstddef>
#include <memory>
#include <string>
#include <vector>

class RpcChannelPool : public google::protobuf::RpcChannel
{
public:
    RpcChannelPool(std::string ip, uint16_t port, size_t pool_size);

    ~RpcChannelPool();

    bool start();

    void stop();

    void CallMethod(const google::protobuf::MethodDescriptor* method,
                    google::protobuf::RpcController* controller,
                    const google::protobuf::Message* request,
                    google::protobuf::Message* response,
                    google::protobuf::Closure* done) override;
    std::shared_ptr<MyRpcChannel> pickChannel();

    void repairDeadChannels();

    size_t unavailableCount() const;
private:
    std::string ip_;
    uint16_t port_;
    size_t pool_size_;

    std::mutex lifecycle_mutex_; //保护 start() 和 stop() 串行化执行
    std::mutex repair_mutex_; //保护channels_ 和 next_

    using ChannelPtr = std::shared_ptr<MyRpcChannel>;
    using ChannelList = std::vector<ChannelPtr>;

    std::shared_ptr<ChannelList> channels_snapshot_;
    std::atomic<size_t> next_{0};

    std::unique_ptr<CallbackExecutor> callback_executor_;
private:
    bool repairChannel(size_t index);
    bool repairChannelInCopy(ChannelList& new_channels,
                                size_t index,
                                std::vector<std::shared_ptr<MyRpcChannel> >& channels_to_stop,
                                std::vector<std::shared_ptr<MyRpcChannel> >& new_channels_stared);
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
*/

/*
CallbackExecutor lifecycle rule:

CallbackExecutor is owned by RpcChannelPool or a higher-level RpcClient/RpcRuntime.
MyRpcChannel only posts completion callbacks to it and never stops it.

CallbackExecutor::stop() must be called by the owner thread after all channels
have been stopped. User callbacks must not call CallbackExecutor::stop().
If a callback wants to shut down the client or pool, it should request shutdown
and let the owner perform the actual stop outside the callback worker thread.
*/