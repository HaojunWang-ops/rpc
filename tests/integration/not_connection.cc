#include "rpc_channel.h"
#include "rpc_closure.h"
#include "rpc_controller.h"
#include "user.pb.h"
#include "CallbackExecutor.h"

#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>

namespace
{
struct AsyncState
{
    std::mutex mutex;
    std::condition_variable cv;
    int done_count = 0;
    bool controller_failed = false;
    std::string error_text;
};
}

// 未 start 的 channel 拒绝异步调用时也必须执行一次 done。
TEST(NotConnectionTest, AsyncCallBeforeStartShouldFailAndCallDoneOnce)
{
    CallbackExecutor callbackexecutor;
    callbackexecutor.start();
    auto channel = MyRpcChannel::create("127.0.0.1", 1, &callbackexecutor);
    demo::UserService_Stub stub(channel.get());

    auto request = std::make_shared<demo::LoginRequest>();
    auto response = std::make_shared<demo::LoginResponse>();
    auto controller = std::make_shared<SimpleRpcController>();
    auto state = std::make_shared<AsyncState>();

    request->set_name("Tom");
    request->set_password("123456");

    google::protobuf::Closure* done = SendResponseClosure(
        [request, response, controller, state] {
            std::lock_guard<std::mutex> lock(state->mutex);
            ++state->done_count;
            state->controller_failed = controller->Failed();
            state->error_text = controller->ErrorText();
            state->cv.notify_one();
        });

    stub.Login(controller.get(), request.get(), response.get(), done);

    std::unique_lock<std::mutex> lock(state->mutex);
    ASSERT_TRUE(state->cv.wait_for(lock, std::chrono::seconds(1), [&] {
        return state->done_count == 1;
    }));

    EXPECT_EQ(state->done_count, 1);
    EXPECT_TRUE(state->controller_failed);
    EXPECT_FALSE(state->error_text.empty());
}

// 未 start 的同步调用必须快速失败，不能等待不存在的 reader/timeout worker。
TEST(NotConnectionTest, SyncCallBeforeStartShouldFailWithoutBlocking)
{
    CallbackExecutor callbackexecutor;
    callbackexecutor.start();
    auto channel = MyRpcChannel::create("127.0.0.1", 1, &callbackexecutor);
    demo::UserService_Stub stub(channel.get());

    demo::LoginRequest request;
    demo::LoginResponse response;
    SimpleRpcController controller;

    request.set_name("Tom");
    request.set_password("123456");

    stub.Login(&controller, &request, &response, nullptr);

    EXPECT_TRUE(controller.Failed());
    EXPECT_FALSE(controller.ErrorText().empty());
}
