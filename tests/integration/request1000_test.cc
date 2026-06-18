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

TEST(Request1000Test, SyncLoginRequestsShouldAllSucceed)
{
    constexpr int kRequestCount = 1000;

    ControlledTcpServer server(0, buildLoginResponseBody);
    ASSERT_TRUE(server.start());

    RpcChannelPool pool("127.0.0.1", server.port(), 4);
    ASSERT_TRUE(pool.start());

    demo::UserService_Stub stub(&pool);

    for (int i = 0; i < kRequestCount; ++i)
    {
        demo::LoginRequest request;
        demo::LoginResponse response;
        SimpleRpcController controller;

        request.set_name("haojun");
        request.set_password("123456");

        stub.Login(&controller, &request, &response, nullptr);

        ASSERT_FALSE(controller.Failed()) << controller.ErrorText();
        ASSERT_TRUE(response.success());
    }

    EXPECT_TRUE(server.waitForTotalRequests(kRequestCount, std::chrono::seconds(1)));

    pool.stop();
    server.stop();
}
