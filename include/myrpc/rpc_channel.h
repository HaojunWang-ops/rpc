#pragma once

#include <google/protobuf/service.h>
#include <string>
#include <cstdint>

class RpcChannel : public google::protobuf::RpcChannel {
public:
    RpcChannel(std::string ip, std::uint16_t port);

    void CallMethod(const google::protobuf::MethodDescriptor* method,
                    google::protobuf::RpcController* controller,
                    const google::protobuf::Message* request,
                    google::protobuf::Message* response,
                    google::protobuf::Closure* done) override;

private:
    std::string ip_;
    std::uint16_t port_;
};