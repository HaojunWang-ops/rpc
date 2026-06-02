#include <gtest/gtest.h>
#include "base/LogStream.h"

using namespace reactor;

TEST(LogStream, Integer)
{
    LogStream os;
    os << 123;

    EXPECT_EQ(os.buffer().toString(), "123");
}

TEST(LogStreamTest, NegativeInteger)
{
    LogStream os;
    os << -123;

    EXPECT_EQ(os.buffer().toString(), "-123");
}

TEST(LogStreamTest, String)
{
    LogStream os;
    os << "hello";

    EXPECT_EQ(os.buffer().toString(), "hello");
}

TEST(LogStreamTest, Double)
{
    LogStream os;
    os << 3.14;

    EXPECT_EQ(os.buffer().toString(), "3.14");
}

TEST(LogStreamTest, pointer)
{
    LogStream os;

    const void* p = reinterpret_cast<const void*>(0x7f8a1c3b5d4e);
    os << p;

    EXPECT_EQ(os.buffer().toString(), "0x7F8A1C3B5D4E");
}
