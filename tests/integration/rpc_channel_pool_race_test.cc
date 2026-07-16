#include "tcpserver.h"
#include "rpc_closure.h"
#include "rpc_channel_pool.h"
#include "user.pb.h"
#include "rpc_controller.h"
#include "rpc_channel.h"

#include <gtest/gtest.h>
#include <string>
#include <chrono>
#include <future>
#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

class LambdaClosure final : public google::protobuf::Closure
{
public:
    explicit LambdaClosure(std::function<void()> fn)
        : fn_(std::move(fn))
    {
    }

    void Run() override
    {
        fn_();
    }
private:
    std::function<void()> fn_;
};

std::string buildLoginResponse(const myrpc::RpcHeader&,
                                const std::string&)
{
    demo::LoginResponse response;
    response.set_code(0);
    response.set_success(true);
    response.set_message("OK");
    return response.SerializeAsString();
}

TEST(RpcChannelPoolRaceTest, RepairPausedBeforePublishThenStopShouldNotRepublishChannel)
{
    using namespace std::chrono_literals;

    ControlledTcpServer server(0, buildLoginResponse);
    ASSERT_TRUE(server.start());

    auto pool = std::make_shared<RpcChannelPool>("127.0.0.1", server.port(), 1);
    ASSERT_TRUE(pool->start());

    auto old_snapshot = pool->snapshotForTest();
    ASSERT_TRUE(old_snapshot);
    EXPECT_EQ(pool->snapshotSizeForTest(), 1u);

    auto old_channel = old_snapshot->at(0);
    ASSERT_TRUE(old_channel);
    ASSERT_TRUE(old_channel->isAvailable());

    old_channel->stop();

    std::promise<void> reached_publish_point_promise;
    auto reached_publish_point = reached_publish_point_promise.get_future();

    std::promise<void> release_repair_promise;
    auto release_repair = release_repair_promise.get_future().share();

    std::vector<std::shared_ptr<MyRpcChannel>> replacements;
    
    auto hooks = std::make_shared<RpcChannelPool::TestHooks>();
    hooks->before_snapshot_publish = [&](const std::vector<std::shared_ptr<MyRpcChannel>>& new_channels_started){
        replacements = new_channels_started;
        
        reached_publish_point_promise.set_value();

        release_repair.wait();
    };

    pool->setTestHookForTest(hooks);

    auto repair_future = std::async(std::launch::async, [pool]{
        pool->repairDeadChannels();
    });

    const auto reached_status = reached_publish_point.wait_for(2s);

    if (reached_status != std::future_status::ready)
    {
        release_repair_promise.set_value();
        repair_future.wait();

        FAIL() << "repair did not reach the publish hook";
    }

    ASSERT_FALSE(replacements.empty());

    pool->stop();

    EXPECT_EQ(pool->snapshotSizeForTest(), 0u);
    ASSERT_FALSE(old_channel->isAvailable());

    release_repair_promise.set_value();

    EXPECT_EQ(repair_future.wait_for(2s), std::future_status::ready);
    repair_future.get();

    EXPECT_EQ(pool->snapshotSizeForTest(), 0u);

    for (const auto& replacement : replacements)
    {
        ASSERT_TRUE(replacement);
        ASSERT_FALSE(replacement->isAvailable());
    }
}