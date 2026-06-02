#include <gtest/gtest.h>
#include "net/Buffer.h"

using namespace reactor;
using namespace reactor::net;

TEST(BufferTest, AppendAndRetrieveAll)
{
    Buffer buf;
    
    EXPECT_EQ(buf.readableBytes(), 0);

    const char* msg = "hello";
    buf.append(msg, 5);

    EXPECT_EQ(buf.readableBytes(), 5);
    EXPECT_EQ(buf.writeableBytes(), 1024 - 5);
    EXPECT_EQ(buf.retrieveAsString(), "hello");
    EXPECT_EQ(buf.readableBytes(), 0);
}

TEST(BufferTest, RetrieveParital)
{
    Buffer buf;

    std::string msg = "hello world!";
    buf.append(msg.data(), msg.size());

    buf.retrieve(6);

    EXPECT_EQ(buf.readableBytes(), 6);
    EXPECT_EQ(buf.retrieveAsString(), "world!");
}

TEST(BufferTest, AppendLargeData)
{
    Buffer buf;

    std::string data(100000, 'x');
    buf.append(data.data(), data.size());

    EXPECT_EQ(buf.readableBytes(), data.size());
    EXPECT_EQ(buf.writeableBytes(), 0);
    EXPECT_EQ(buf.retrieveAsString(), data);
}