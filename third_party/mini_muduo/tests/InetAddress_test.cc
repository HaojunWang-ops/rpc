#include <gtest/gtest.h>
#include "InetAddress.h"

using namespace reactor;
using namespace reactor::net;

TEST(InetAddress, InetAddress)
{
    InetAddress addr0(1234);

    EXPECT_EQ(addr0.toIp(), string("0.0.0.0"));
    EXPECT_EQ(addr0.toIpPort(), string("0.0.0.0:1234"));
    EXPECT_EQ(addr0.port(), 1234);

    InetAddress addr1(4321, true);
    EXPECT_EQ(addr1.toIp(), string("127.0.0.1"));
    EXPECT_EQ(addr1.toIpPort(), string("127.0.0.1:4321"));
    EXPECT_EQ(addr1.port(), 4321);

    InetAddress addr2("1.2.3.4", 8888);
    EXPECT_EQ(addr2.toIp(), string("1.2.3.4"));
    EXPECT_EQ(addr2.toIpPort(), string("1.2.3.4:8888"));
    EXPECT_EQ(addr2.port(), 8888);

    InetAddress addr3("255.254.253.252", 65535);
    EXPECT_EQ(addr3.toIp(), string("255.254.253.252"));
    EXPECT_EQ(addr3.toIpPort(), string("255.254.253.252:65535"));
    EXPECT_EQ(addr3.port(), 65535);
}