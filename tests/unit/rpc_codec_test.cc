#include "rpc_codec.h"

#include "user.pb.h"

#include <arpa/inet.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <string>

namespace
{
const google::protobuf::MethodDescriptor* loginMethod()
{
    return demo::UserService::descriptor()->FindMethodByName("Login");
}

demo::LoginRequest makeLoginRequest()
{
    demo::LoginRequest request;
    request.set_name("haojun");
    request.set_password("123456");
    return request;
}
}

TEST(RpcCodecTest, EncodeRequestFrameShouldProduceExpectedWireFields)
{
    auto request = makeLoginRequest();
    std::string frame;
    std::string error;

    ASSERT_TRUE(RpcCodec::encodeRequestFrame(
        42, loginMethod(), &request, &frame, &error)) << error;

    ASSERT_GE(frame.size(), 8u);

    uint32_t net_total_size = 0;
    uint32_t net_header_size = 0;
    std::memcpy(&net_total_size, frame.data(), sizeof(net_total_size));
    std::memcpy(&net_header_size, frame.data() + 4, sizeof(net_header_size));

    const uint32_t total_size = ::ntohl(net_total_size);
    const uint32_t header_size = ::ntohl(net_header_size);

    ASSERT_EQ(frame.size(), static_cast<size_t>(4 + total_size));
    ASSERT_GE(total_size, 4u + header_size);

    myrpc::RpcHeader header;
    ASSERT_TRUE(header.ParseFromArray(frame.data() + 8, header_size));

    EXPECT_EQ(header.request_id(), 42u);
    EXPECT_EQ(header.service_name(), "demo.UserService");
    EXPECT_EQ(header.method_name(), "Login");
    EXPECT_EQ(header.args_size(), request.ByteSizeLong());
}

TEST(RpcCodecTest, EncodeRequestFrameShouldRejectNullInputs)
{
    auto request = makeLoginRequest();
    std::string frame;
    std::string error;

    EXPECT_FALSE(RpcCodec::encodeRequestFrame(
        1, nullptr, &request, &frame, &error));
    EXPECT_FALSE(error.empty());

    EXPECT_FALSE(RpcCodec::encodeRequestFrame(
        1, loginMethod(), nullptr, &frame, &error));
    EXPECT_FALSE(error.empty());

    EXPECT_FALSE(RpcCodec::encodeRequestFrame(
        1, loginMethod(), &request, nullptr, &error));
    EXPECT_FALSE(error.empty());
}

TEST(RpcCodecTest, DecodeFrameMetaShouldValidateSizes)
{
    RpcCodec::FrameMeta meta;
    std::string error;

    EXPECT_FALSE(RpcCodec::decodeFrameMeta(::htonl(3), ::htonl(0), &meta, &error));
    EXPECT_FALSE(error.empty());

    EXPECT_FALSE(RpcCodec::decodeFrameMeta(::htonl(4), ::htonl(1), &meta, &error));
    EXPECT_FALSE(error.empty());

    ASSERT_TRUE(RpcCodec::decodeFrameMeta(::htonl(12), ::htonl(5), &meta, &error));
    EXPECT_EQ(meta.total_size, 12u);
    EXPECT_EQ(meta.header_size, 5u);
    EXPECT_EQ(meta.body_size, 3u);
}

TEST(RpcCodecTest, DecodeFrameMetaShouldRejectOversizedResponseFrames)
{
    constexpr uint32_t kMaxResponseFrameSize = 64 * 1024 * 1024;
    constexpr uint32_t kMaxResponseHeaderSize = 1024 * 1024;

    RpcCodec::FrameMeta meta;
    std::string error;

    EXPECT_FALSE(RpcCodec::decodeFrameMeta(
        ::htonl(kMaxResponseFrameSize + 1), ::htonl(0), &meta, &error));
    EXPECT_FALSE(error.empty());

    error.clear();
    EXPECT_FALSE(RpcCodec::decodeFrameMeta(
        ::htonl(4 + kMaxResponseHeaderSize + 1),
        ::htonl(kMaxResponseHeaderSize + 1),
        &meta,
        &error));
    EXPECT_FALSE(error.empty());
}

TEST(RpcCodecTest, DecodeResponseHeaderShouldValidateHeaderAndBodySize)
{
    myrpc::RpcResponseHeader response_header;
    response_header.set_request_id(7);
    response_header.set_error_code(myrpc::RPC_OK);
    response_header.set_response_size(3);

    std::string header_str;
    ASSERT_TRUE(response_header.SerializeToString(&header_str));

    RpcCodec::FrameMeta meta;
    meta.header_size = static_cast<uint32_t>(header_str.size());
    meta.body_size = 3;

    myrpc::RpcResponseHeader decoded;
    std::string error;
    ASSERT_TRUE(RpcCodec::decodeResponseHeader(header_str, meta, &decoded, &error));
    EXPECT_EQ(decoded.request_id(), 7u);

    RpcCodec::FrameMeta bad_body_meta = meta;
    bad_body_meta.body_size = 2;
    EXPECT_FALSE(RpcCodec::decodeResponseHeader(
        header_str, bad_body_meta, &decoded, &error));
    EXPECT_FALSE(error.empty());

    EXPECT_FALSE(RpcCodec::decodeResponseHeader(
        "not a protobuf", meta, &decoded, &error));
    EXPECT_FALSE(error.empty());

    EXPECT_FALSE(RpcCodec::decodeResponseHeader(
        header_str, meta, nullptr, &error));
    EXPECT_FALSE(error.empty());
}

TEST(RpcCodecTest, DecodeResponseBodyShouldParseMessage)
{
    demo::LoginResponse response;
    response.set_code(0);
    response.set_message("ok");
    response.set_success(true);

    std::string body;
    ASSERT_TRUE(response.SerializeToString(&body));

    demo::LoginResponse decoded;
    std::string error;
    ASSERT_TRUE(RpcCodec::decodeResponseBody(body, &decoded, &error));

    EXPECT_EQ(decoded.code(), 0);
    EXPECT_EQ(decoded.message(), "ok");
    EXPECT_TRUE(decoded.success());

    EXPECT_FALSE(RpcCodec::decodeResponseBody(body, nullptr, &error));
    EXPECT_FALSE(error.empty());

    EXPECT_FALSE(RpcCodec::decodeResponseBody("not a protobuf", &decoded, &error));
    EXPECT_FALSE(error.empty());
}
