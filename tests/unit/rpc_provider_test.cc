#include "ThreadPool.h"
#include "net/Buffer.h"
#include "net/EventLoop.h"
#include "rpc_closure.h"
#include "rpc_controller.h"
#include "rpc_header.pb.h"

#include <atomic>
#include <string>
#include <unordered_map>

// The harness sets TcpConnection state directly without running EventLoop.
#define private public
#include "net/TcpConnection.h"
#undef private

#include "net/TcpServer.h"

// Existing provider tests use private access as their unit-test seam.
#define private public
#include "rpc_provider.h"
#undef private

#include "user.pb.h"

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <memory>
#include <utility>

namespace
{
class TestUserService final : public demo::UserService
{
public:
    void Login(google::protobuf::RpcController* controller,
               const demo::LoginRequest* request,
               demo::LoginResponse* response,
               google::protobuf::Closure* done) override
    {
        (void)controller;
        response->set_code(0);
        response->set_message("login:" + request->name());
        response->set_success(true);

        if (done != nullptr)
        {
            done->Run();
        }
    }

    void Register(google::protobuf::RpcController* controller,
                  const demo::RegisterRequest* request,
                  demo::RegisterResponse* response,
                  google::protobuf::Closure* done) override
    {
        (void)controller;
        response->set_code(0);
        response->set_message("register:" + request->name());
        response->set_success(true);

        if (done != nullptr)
        {
            done->Run();
        }
    }
};

class ProviderConnectionHarness
{
public:
    ProviderConnectionHarness()
    {
        int fds[2] = {-1, -1};
        if (::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0)
        {
            return;
        }

        peer_fd_ = fds[1];
        timeval timeout{};
        timeout.tv_sec = 1;
        ::setsockopt(peer_fd_,
                     SOL_SOCKET,
                     SO_RCVTIMEO,
                     &timeout,
                     sizeof(timeout));

        conn_ = std::make_shared<reactor::net::TcpConnection>(
            &loop_,
            "rpc_provider_unit_test",
            fds[0],
            reactor::net::InetAddress(0),
            reactor::net::InetAddress(0));
        conn_->setState(reactor::net::TcpConnection::kConnected);
    }

    ~ProviderConnectionHarness()
    {
        if (conn_)
        {
            conn_->setState(reactor::net::TcpConnection::kDisconnected);
            conn_.reset();
        }

        if (peer_fd_ >= 0)
        {
            ::close(peer_fd_);
        }
    }

    bool valid() const
    {
        return conn_ != nullptr && peer_fd_ >= 0;
    }

    const reactor::net::TcpConnectionPtr& conn() const
    {
        return conn_;
    }

    int peerFd() const
    {
        return peer_fd_;
    }

private:
    reactor::net::EventLoop loop_;
    int peer_fd_ = -1;
    reactor::net::TcpConnectionPtr conn_;
};

const google::protobuf::MethodDescriptor* findMethod(
    const RpcProvider::ServiceInfo& service_info,
    const std::string& name)
{
    auto it = service_info.method_map.find(name);
    if (it == service_info.method_map.end())
    {
        return nullptr;
    }
    return it->second;
}

bool readAll(int fd, void* data, size_t size)
{
    char* p = static_cast<char*>(data);
    size_t left = size;

    while (left > 0)
    {
        ssize_t n = ::read(fd, p, left);
        if (n > 0)
        {
            p += n;
            left -= static_cast<size_t>(n);
            continue;
        }

        if (n < 0 && errno == EINTR)
        {
            continue;
        }

        return false;
    }

    return true;
}

bool readRpcResponse(int fd,
                     myrpc::RpcResponseHeader* response_header,
                     std::string* response_body)
{
    uint32_t net_total_size = 0;
    uint32_t net_header_size = 0;

    if (!readAll(fd, &net_total_size, sizeof(net_total_size)) ||
        !readAll(fd, &net_header_size, sizeof(net_header_size)))
    {
        return false;
    }

    const uint32_t total_size = ::ntohl(net_total_size);
    const uint32_t header_size = ::ntohl(net_header_size);
    if (total_size < 4 || header_size > total_size - 4)
    {
        return false;
    }

    std::string header_str(header_size, '\0');
    std::string body(total_size - 4 - header_size, '\0');

    if (!readAll(fd, header_str.data(), header_str.size()) ||
        !readAll(fd, body.data(), body.size()))
    {
        return false;
    }

    if (!response_header->ParseFromString(header_str))
    {
        return false;
    }

    *response_body = std::move(body);
    return true;
}

std::string makeRequestFrame(const myrpc::RpcHeader& header,
                             const std::string& body)
{
    std::string header_str = header.SerializeAsString();
    const uint32_t header_size =
        static_cast<uint32_t>(header_str.size());
    const uint32_t net_header_size = ::htonl(header_size);

    std::string frame;
    frame.append(reinterpret_cast<const char*>(&net_header_size),
                 sizeof(net_header_size));
    frame.append(header_str);
    frame.append(body);
    return frame;
}

void expectBadRequestResponse(int fd,
                              uint64_t expected_request_id,
                              const std::string& expected_error_text)
{
    myrpc::RpcResponseHeader response_header;
    std::string response_body;
    ASSERT_TRUE(readRpcResponse(fd, &response_header, &response_body));

    EXPECT_EQ(response_header.request_id(), expected_request_id);
    EXPECT_EQ(response_header.error_code(), myrpc::RPC_BAD_REQUEST);
    EXPECT_EQ(response_header.error_text(), expected_error_text);
    EXPECT_EQ(response_header.response_size(), 0u);
    EXPECT_TRUE(response_body.empty());
}
}

TEST(RpcProviderTest, ConstructedProviderStartsStoppedAndEmpty)
{
    RpcProvider provider(1);

    EXPECT_FALSE(provider.running_.load(std::memory_order_acquire));
    EXPECT_EQ(provider.threadNum_, 1u);
    EXPECT_TRUE(provider.service_map_.empty());
}

TEST(RpcProviderTest, NotifyServiceBeforeRunRegistersServiceAndMethods)
{
    RpcProvider provider(1);
    TestUserService service;

    provider.NotifyService(&service);

    auto service_it = provider.service_map_.find("demo.UserService");
    ASSERT_NE(service_it, provider.service_map_.end());
    EXPECT_EQ(service_it->second.service, &service);
    EXPECT_EQ(service_it->second.method_map.size(), 2u);
    EXPECT_EQ(findMethod(service_it->second, "Login"),
              demo::UserService::descriptor()->FindMethodByName("Login"));
    EXPECT_EQ(findMethod(service_it->second, "Register"),
              demo::UserService::descriptor()->FindMethodByName("Register"));
}

TEST(RpcProviderTest, NotifyNullServiceDoesNotMutateRegistry)
{
    RpcProvider provider(1);

    provider.NotifyService(nullptr);

    EXPECT_TRUE(provider.service_map_.empty());
}

TEST(RpcProviderTest, NotifyDuplicateServiceDoesNotReplaceExistingService)
{
    RpcProvider provider(1);
    TestUserService first_service;
    TestUserService second_service;

    provider.NotifyService(&first_service);
    provider.NotifyService(&second_service);

    auto service_it = provider.service_map_.find("demo.UserService");
    ASSERT_NE(service_it, provider.service_map_.end());
    EXPECT_EQ(provider.service_map_.size(), 1u);
    EXPECT_EQ(service_it->second.service, &first_service);
    EXPECT_EQ(service_it->second.method_map.size(), 2u);
}

TEST(RpcProviderTest, RegisteredMethodDescriptorDispatchesToService)
{
    RpcProvider provider(1);
    TestUserService service;
    provider.NotifyService(&service);

    auto service_it = provider.service_map_.find("demo.UserService");
    ASSERT_NE(service_it, provider.service_map_.end());

    const google::protobuf::MethodDescriptor* method =
        findMethod(service_it->second, "Login");
    ASSERT_NE(method, nullptr);

    demo::LoginRequest request;
    request.set_name("haojun");
    request.set_password("123456");

    demo::LoginResponse response;
    bool done_called = false;
    auto done = SendResponseClosure([&done_called] {
        done_called = true;
    });

    service_it->second.service->CallMethod(method, nullptr, &request, &response, done);

    EXPECT_TRUE(done_called);
    EXPECT_EQ(response.code(), 0);
    EXPECT_EQ(response.message(), "login:haojun");
    EXPECT_TRUE(response.success());
}

TEST(RpcProviderTest, NotifyServiceAfterRunStartedDoesNotMutateRegistry)
{
    RpcProvider provider(1);
    TestUserService service;

    provider.running_.store(true, std::memory_order_release);
    provider.NotifyService(&service);

    EXPECT_TRUE(provider.service_map_.empty());
}

TEST(RpcProviderTest, DoRpcTaskRejectsHeaderSizeLargerThanFrame)
{
    RpcProvider provider(1);
    ProviderConnectionHarness harness;
    ASSERT_TRUE(harness.valid());

    const uint32_t header_size = ::htonl(100);
    std::string frame;
    frame.append(reinterpret_cast<const char*>(&header_size),
                 sizeof(header_size));

    provider.doRpcTask(harness.conn(), std::move(frame));

    expectBadRequestResponse(harness.peerFd(),
                             0,
                             "total_size < 4 + header_size");
}

TEST(RpcProviderTest, DoRpcTaskRejectsHeaderSizeAboveLimit)
{
    RpcProvider provider(1);
    ProviderConnectionHarness harness;
    ASSERT_TRUE(harness.valid());

    constexpr uint32_t kHeaderSizeLimit = 1024 * 1024;
    const uint32_t header_size = kHeaderSizeLimit + 1;
    const uint32_t net_header_size = ::htonl(header_size);

    std::string frame;
    frame.append(reinterpret_cast<const char*>(&net_header_size),
                 sizeof(net_header_size));
    frame.append(header_size, '\0');

    provider.doRpcTask(harness.conn(), std::move(frame));

    expectBadRequestResponse(harness.peerFd(),
                             0,
                             "header_size is too large");
}

TEST(RpcProviderTest, DoRpcTaskRejectsFrameAboveLimit)
{
    RpcProvider provider(1);
    ProviderConnectionHarness harness;
    ASSERT_TRUE(harness.valid());

    constexpr size_t kFrameSizeLimit = 64 * 1024 * 1024;
    std::string frame(kFrameSizeLimit + 1, '\0');

    provider.doRpcTask(harness.conn(), std::move(frame));

    expectBadRequestResponse(harness.peerFd(),
                             0,
                             "total_size is too large");
}

TEST(RpcProviderTest, DoRpcTaskRejectsArgsSizeMismatchAndKeepsRequestId)
{
    RpcProvider provider(1);
    ProviderConnectionHarness harness;
    ASSERT_TRUE(harness.valid());

    myrpc::RpcHeader header;
    header.set_request_id(42);
    header.set_service_name("demo.UserService");
    header.set_method_name("Login");
    header.set_args_size(10);

    std::string frame = makeRequestFrame(header, "");

    provider.doRpcTask(harness.conn(), std::move(frame));

    expectBadRequestResponse(harness.peerFd(),
                             42,
                             "total_size != 4 + header_size + args_size");
}
