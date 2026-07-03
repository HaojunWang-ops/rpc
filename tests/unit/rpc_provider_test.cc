#include "ThreadPool.h"
#include "net/Buffer.h"
#include "net/EventLoop.h"
#include "net/TcpConnection.h"
#include "net/TcpServer.h"
#include "rpc_closure.h"
#include "rpc_controller.h"

#include <atomic>
#include <string>
#include <unordered_map>

#define private public
#include "rpc_provider.h"
#undef private

#include "user.pb.h"

#include <gtest/gtest.h>

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
