#include "rpc_codec.h"

#include <arpa/inet.h>

#include <limits>

namespace
{
static constexpr uint32_t kHeaderSizeFieldBytes = 4;

static constexpr uint32_t kMaxResponseFrameSize = 64 * 1024 * 1024;
static constexpr uint32_t kMaxResponseHeaderSize = 1024 * 1024;  
void setError(std::string* error, const std::string& msg)
{
    if (error)
    {
        *error = msg;
    }
}
}

bool RpcCodec::encodeRequestFrame(uint64_t request_id,
                                  const google::protobuf::MethodDescriptor* method,
                                  const google::protobuf::Message* request,
                                  std::string* out,
                                  std::string* error)
{
    if (!method)
    {
        setError(error, "method is null");
        return false;
    }

    if (!request)
    {
        setError(error, "request is null");
        return false;
    }

    if (!out)
    {
        setError(error, "output buffer is null");
        return false;
    }

    std::string args;
    if (!request->SerializeToString(&args))
    {
        setError(error, "request SerializeToString failed");
        return false;
    }

    if (args.size() > std::numeric_limits<uint32_t>::max())
    {
        setError(error, "request body too large");
        return false;
    }

    myrpc::RpcHeader header;
    header.set_request_id(request_id);
    header.set_service_name(method->service()->full_name());
    header.set_method_name(method->name());
    header.set_args_size(static_cast<uint32_t>(args.size()));

    std::string header_str;
    if (!header.SerializeToString(&header_str))
    {
        setError(error, "request header SerializeToString failed");
        return false;
    }

    if (header_str.size() > std::numeric_limits<uint32_t>::max())
    {
        setError(error, "request header too large");
        return false;
    }

    const uint64_t total_size64 =
        static_cast<uint64_t>(kHeaderSizeFieldBytes) +
        static_cast<uint64_t>(header_str.size()) +
        static_cast<uint64_t>(args.size());

    if (total_size64 > std::numeric_limits<uint32_t>::max())
    {
        setError(error, "request frame too large");
        return false;
    }

    const uint32_t header_size = static_cast<uint32_t>(header_str.size());
    const uint32_t total_size = static_cast<uint32_t>(total_size64);

    const uint32_t net_total_size = ::htonl(total_size);
    const uint32_t net_header_size = ::htonl(header_size);

    out->clear();
    out->reserve(sizeof(net_total_size) + total_size);

    out->append(reinterpret_cast<const char*>(&net_total_size), sizeof(net_total_size));
    out->append(reinterpret_cast<const char*>(&net_header_size), sizeof(net_header_size));
    out->append(header_str);
    out->append(args);

    return true;
}

//将reponse的total_size和header_size解码出来
bool RpcCodec::decodeFrameMeta(uint32_t net_total_size,
                               uint32_t net_header_size,
                               FrameMeta* meta,
                               std::string* error)
{
    if (!meta)
    {
        setError(error, "frame meta is null");
        return false;
    }

    const uint32_t total_size = ::ntohl(net_total_size);
    const uint32_t header_size = ::ntohl(net_header_size);

    if (total_size < kHeaderSizeFieldBytes)
    {
        setError(error, "incorrect response frame: total_size too small");
        return false;
    }

    if (total_size > kMaxResponseFrameSize)
    {
        setError(error, "incorrect response frame: total_size too large");
        return false;
    }

    if (header_size > total_size - kHeaderSizeFieldBytes)
    {
        setError(error, "incorrect response frame: header_size too large");
        return false;
    }

    if (header_size > kMaxResponseHeaderSize)
    {
        setError(error, "invalid response frame:header too large");
        return false;
    }
    meta->total_size = total_size;
    meta->header_size = header_size;
    meta->body_size = total_size - kHeaderSizeFieldBytes - header_size;

    return true;
}

//将response header从std::string解码
bool RpcCodec::decodeResponseHeader(const std::string& header_str,
                                    const FrameMeta& meta,
                                    myrpc::RpcResponseHeader* header,
                                    std::string* error)
{
    if (!header)
    {
        setError(error, "response header is null");
        return false;
    }

    if (header_str.size() != meta.header_size)
    {
        setError(error, "response header size mismatch");
        return false;
    }

    if (!header->ParseFromString(header_str))
    {
        setError(error, "response header ParseFromString failed");
        return false;
    }

    if (header->response_size() != meta.body_size)
    {
        setError(error, "incorrect response frame: body size mismatch");
        return false;
    }

    return true;
}

//将reponse body从std::string解码
bool RpcCodec::decodeResponseBody(const std::string& body,
                                  google::protobuf::Message* response,
                                  std::string* error)
{
    if (!response)
    {
        setError(error, "response is null");
        return false;
    }

    if (!response->ParseFromString(body))
    {
        setError(error, "response ParseFromString failed");
        return false;
    }

    return true;
}