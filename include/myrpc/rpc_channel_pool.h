#pragma once

#include "rpc_channel.h"
#include "rpc_controller.h"

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

private:
    bool repairChannel(size_t index);
    bool repairChannelInCopy(ChannelList& new_channels,
                                size_t index,
                                std::vector<std::shared_ptr<MyRpcChannel> >& channels_to_stop,
                                std::vector<std::shared_ptr<MyRpcChannel> >& new_channels_stared,
                                std::vector<size_t>& new_indexs);
};


// Threading model:
// 1. CallMethod() can be called concurrently.
// 2. pickChannel() is lock-free with atomic snapshot.
// 3. repairDeadChannels() is serialized by repair_mutex_.
// 4. channels_snapshot_ is immutable after publication.
// 5. Do not modify the vector in-place after publishing it.