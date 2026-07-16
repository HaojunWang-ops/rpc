#include "rpc_channel_pool.h"
#include "rpc_controller.h"
#include "tcpserver.h"
#include "user.pb.h"

#include <gtest/gtest.h>

#include <chrono>

namespace
{
std::string buildLoginResponseBody(const myrpc::RpcHeader&,
                                   const std::string&)
{
    demo::LoginResponse response;
    response.set_code(0);
    response.set_message("login success");
    response.set_success(true);
    return response.SerializeAsString();
}
}

// 最小端到端基线：生成的 stub、channel 编码、服务端分发和 response 解码都可用。
TEST(NormalRpcTest, SyncLoginShouldReturnResponse)
{
    ControlledTcpServer server(0, buildLoginResponseBody);
    ASSERT_TRUE(server.start());

    RpcChannelPool pool("127.0.0.1", server.port(), 1);
    ASSERT_TRUE(pool.start());

    demo::UserService_Stub stub(&pool);
    demo::LoginRequest request;
    demo::LoginResponse response;
    SimpleRpcController controller;

    request.set_name("haojun");
    request.set_password("123456");

    stub.Login(&controller, &request, &response, nullptr);

    ASSERT_FALSE(controller.Failed()) << controller.ErrorText();
    EXPECT_TRUE(response.success());
    EXPECT_EQ(response.code(), 0);
    EXPECT_EQ(response.message(), "login success");
    EXPECT_TRUE(server.waitForTotalRequests(1, std::chrono::seconds(1)));

    pool.stop();
    server.stop();
}
