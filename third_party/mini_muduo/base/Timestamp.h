#pragma once

#include <boost/operators.hpp>
#include <string>
#include <cstring>

namespace reactor
{

    class Timestamp
    {
    public:
        Timestamp()
            : microSecondsSinceEpoch_(0)
        {
        }

        explicit Timestamp(int64_t microSecondsSinceEpoch)
            : microSecondsSinceEpoch_(microSecondsSinceEpoch)
        {
        }

        void swap(Timestamp &that)
        {
            std::swap(microSecondsSinceEpoch_, that.microSecondsSinceEpoch_);
        }

        std::string toString() const;
        std::string toFormattedString(bool showMicroseconds = true) const;

        bool valid() const { return microSecondsSinceEpoch_ > 0; }

        int64_t microSecondsSinceEpoch() const { return microSecondsSinceEpoch_; }
        time_t secondsSinceEpoch() const
        {
            return static_cast<time_t>(microSecondsSinceEpoch_ / kMicroSecondsPerSecond);
        }

        static Timestamp now();
        static Timestamp invalid() // construct an invalid TimeStamp
        {
            return Timestamp();
        }

        static Timestamp fromUninxTime(time_t t)
        {
            return fromUnixTime(t, 0);
        }
        static Timestamp fromUnixTime(time_t t, int microseconds) // change unixtime to Timestamp
        {
            return Timestamp(static_cast<int64_t>(t) * kMicroSecondsPerSecond + microseconds);
        }

        static const int kMicroSecondsPerSecond = (1000 * 1000);

    private:
        int64_t microSecondsSinceEpoch_;
    };

    inline bool operator<(Timestamp lhs, Timestamp rhs)
    {
        return lhs.microSecondsSinceEpoch() < rhs.microSecondsSinceEpoch();
    }
    inline bool operator==(Timestamp lhs, Timestamp rhs)
    {
        return lhs.microSecondsSinceEpoch() == rhs.microSecondsSinceEpoch();
    }
    inline bool operator!=(const Timestamp &lhs, const Timestamp &rhs) { return !(lhs == rhs); }
    inline bool operator>(const Timestamp &lhs, const Timestamp &rhs) { return rhs < lhs; }
    inline bool operator<=(const Timestamp &lhs, const Timestamp &rhs) { return !(rhs < lhs); }
    inline bool operator>=(const Timestamp &lhs, const Timestamp &rhs) { return !(lhs < rhs); }

    // get the difference of two TimeStamps ,reslut in seconds

    inline double timeDifference(Timestamp high, Timestamp low)
    {
        int64_t diff = high.microSecondsSinceEpoch() - low.microSecondsSinceEpoch();
        return static_cast<double>(diff) / Timestamp::kMicroSecondsPerSecond;
    }

    // add seconds to TimeStamp
    inline Timestamp addTime(Timestamp timestamp, double seconds)
    {
        int64_t delta = static_cast<int64_t>(seconds * Timestamp::kMicroSecondsPerSecond);
        return Timestamp(timestamp.microSecondsSinceEpoch() + delta);
    }
}
