#pragma once

#include "rpc_header.pb.h"

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>

#include <cstdint>
#include <string>

class RpcCodec
{
public:
    struct FrameMeta
    {
        uint32_t total_size = 0;   
        uint32_t header_size = 0;
        uint32_t body_size = 0;
    };

    static bool encodeRequestFrame(uint64_t request_id,
                                   const google::protobuf::MethodDescriptor* method,
                                   const google::protobuf::Message* request,
                                   std::string* out,
                                   std::string* error);

    static bool decodeFrameMeta(uint32_t net_total_size,
                                uint32_t net_header_size,
                                FrameMeta* meta,
                                std::string* error);

    static bool decodeResponseHeader(const std::string& header_str,
                                     const FrameMeta& meta,
                                     myrpc::RpcResponseHeader* header,
                                     std::string* error);

    static bool decodeResponseBody(const std::string& body,
                                   google::protobuf::Message* response,
                                   std::string* error);
};