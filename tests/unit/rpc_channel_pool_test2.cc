#include "tcpserver.h"
#include "rpc_channel_pool.h"
#include "user.pb.h"

#include <gtest/gtest.h>
#include <string>
#include <chrono>

std::string func(const myrpc::RpcHeader& req_header,
                const std::string& request_body)
{
    std::string response_body("\0");
    return response_body;
}

TEST(RpcChannelPoolTest, RequestsShouldBeDistributedAcrossConnections)
{
    ControlledTcpServer server(18081, func);
    ASSERT_TRUE(server.start());

    RpcChannelPool pool("127.0.0.1", 18081, 4);
    ASSERT_TRUE(pool.start());
    ASSERT_TRUE(server.waitForAcceptCount(4, std::chrono::seconds(1)));

    for (int i = 0; i < 8; ++i)
    {
        demo::LoginRequest req;
        demo::LoginResponse resp;
        SimpleRpcController controller;

        req.set_name("haojun");
        req.set_password("123");

        auto ch = pool.pickChannel();
        ASSERT_TRUE(ch);

        demo::UserService_Stub stub(ch.get());
        stub.Login(&controller, &req, &resp, nullptr);

        ASSERT_FALSE(controller.Failed()) << controller.ErrorText();
    }

    ASSERT_TRUE(server.waitForTotalRequests(8, std::chrono::seconds(1)));

    for (size_t i = 0; i < 4; ++i)
    {
        EXPECT_GE(server.requestCountOf(i), 1);
    }

    pool.stop();
    server.stop();
}