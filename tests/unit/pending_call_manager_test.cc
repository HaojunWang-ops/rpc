#include "pending_call_manager.h"

#include <gtest/gtest.h>

#include <atomic>
#include <future>
#include <memory>
#include <thread>
#include <vector>

namespace
{
std::shared_ptr<PendingCall> makeCall()
{
    return std::make_shared<PendingCall>();
}
}

TEST(PendingCallManagerTest, RejectsAddBeforeAccepting)
{
    PendingCallManager manager;

    EXPECT_EQ(manager.add(1, makeCall()),
              PendingCallManager::AddResult::kNotAccepting);
}

TEST(PendingCallManagerTest, AddTakeAndDuplicateSemantics)
{
    PendingCallManager manager;
    manager.resetForStart();

    auto call = makeCall();

    EXPECT_EQ(manager.add(1, call), PendingCallManager::AddResult::kOk);
    EXPECT_EQ(manager.add(1, makeCall()),
              PendingCallManager::AddResult::kDuplicate);

    EXPECT_EQ(manager.take(1), call);
    EXPECT_EQ(manager.take(1), nullptr);
}

TEST(PendingCallManagerTest, FailAllStopsAcceptingAndReturnsPendingCalls)
{
    PendingCallManager manager;
    manager.resetForStart();

    auto first = makeCall();
    auto second = makeCall();

    ASSERT_EQ(manager.add(1, first), PendingCallManager::AddResult::kOk);
    ASSERT_EQ(manager.add(2, second), PendingCallManager::AddResult::kOk);

    auto failed = manager.failAllAndStopAccepting();

    ASSERT_EQ(failed.size(), 2u);
    EXPECT_EQ(failed[1], first);
    EXPECT_EQ(failed[2], second);
    EXPECT_EQ(manager.take(1), nullptr);
    EXPECT_EQ(manager.add(3, makeCall()),
              PendingCallManager::AddResult::kNotAccepting);
}

TEST(PendingCallManagerTest, ResetForStartClearsOldPendingAndAcceptsAgain)
{
    PendingCallManager manager;
    manager.resetForStart();

    ASSERT_EQ(manager.add(1, makeCall()), PendingCallManager::AddResult::kOk);

    manager.resetForStart();

    EXPECT_EQ(manager.take(1), nullptr);
    EXPECT_EQ(manager.add(1, makeCall()), PendingCallManager::AddResult::kOk);
}

TEST(PendingCallManagerTest, ConcurrentAddTakeAndFailAllShouldBeSafe)
{
    PendingCallManager manager;
    manager.resetForStart();

    constexpr int kThreadCount = 4;
    constexpr int kCallsPerThread = 100;

    std::vector<std::future<void>> workers;
    workers.reserve(kThreadCount);

    for (int t = 0; t < kThreadCount; ++t)
    {
        workers.push_back(std::async(std::launch::async, [&, t] {
            for (int i = 0; i < kCallsPerThread; ++i)
            {
                const uint64_t request_id =
                    static_cast<uint64_t>(t * kCallsPerThread + i + 1);
                auto result = manager.add(request_id, makeCall());
                if (result == PendingCallManager::AddResult::kOk && i % 2 == 0)
                {
                    manager.take(request_id);
                }
            }
        }));
    }

    for (auto& worker : workers)
    {
        worker.get();
    }

    auto failed = manager.failAllAndStopAccepting();
    EXPECT_EQ(failed.size(),
              static_cast<size_t>(kThreadCount * kCallsPerThread / 2));
    EXPECT_EQ(manager.add(100000, makeCall()),
              PendingCallManager::AddResult::kNotAccepting);
}
