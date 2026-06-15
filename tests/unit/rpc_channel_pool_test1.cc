#include "tcpserver.h"
#include "rpc_channel_pool.h"

#include <gtest/gtest.h>
#include <string>
#include <chrono>

std::string func(const myrpc::RpcHeader& req_header,
                const std::string& request_body)
{
}
TEST(RpcChannelPoolTest, StartShouldCreateFixedConnections)
{
        
    ControlledTcpServer server(18080, func);
    ASSERT_TRUE(server.start());

    RpcChannelPool pool("127.0.0.1", 18080, 4);
    ASSERT_TRUE(pool.start());

    ASSERT_TRUE(server.waitForAcceptCount(4, std::chrono::seconds(1)));
    ASSERT_TRUE(server.waitForActiveConnections(4, std::chrono::seconds(1)));

    pool.stop();
    server.stop();
}