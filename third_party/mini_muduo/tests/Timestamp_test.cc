#include <gtest/gtest.h>

#include "base/Timestamp.h"

using namespace reactor;

TEST(TimestampTest, Invalid)
{
    Timestamp ts = Timestamp::invalid();

    EXPECT_EQ(ts.microSecondsSinceEpoch(), 0);
}

TEST(TimestampTest, Now)
{
    Timestamp now = Timestamp::now();

    EXPECT_GT(now.microSecondsSinceEpoch(), 0);
    EXPECT_GT(now.secondsSinceEpoch(), 0);
}

TEST(TimestampTest, ConstructFromMicroseconds)
{
    Timestamp ts(123456789);

    EXPECT_EQ(ts.microSecondsSinceEpoch(), 123456789);
    EXPECT_EQ(ts.secondsSinceEpoch(), 123);
}

TEST(TimestampTest, TimeDifference)
{
    Timestamp t1(1000000);  // 1 second
    Timestamp t2(2500000);  // 2.5 seconds

    EXPECT_DOUBLE_EQ(timeDifference(t2, t1), 1.5);
    EXPECT_DOUBLE_EQ(timeDifference(t1, t2), -1.5);
}

TEST(TimestampTest, AddTime)
{
    Timestamp t1(1000000);  // 1 second
    Timestamp t2 = addTime(t1, 2.5);

    EXPECT_EQ(t2.microSecondsSinceEpoch(), 3500000);
}

TEST(TimestampTest, ToString)
{
    Timestamp ts(123456789);

    EXPECT_EQ(ts.toString(), "123.456789");
}

TEST(TimestampTest, ToFormattedString)
{
    Timestamp ts(0);

    std::string str = ts.toFormattedString(false);

    EXPECT_FALSE(str.empty());
}