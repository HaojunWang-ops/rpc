#include "user.pb.h"
#include "rpc_controller.h"
#include "rpc_channel.h"
#include "Logging.h"
#include "rpc_closure.h"

#include <gtest/gtest.h>
#include <google/protobuf/stubs/common.h>
#include <iostream>
#include <thread>
#include <chrono>
#include <mutex>
#include <condition_variable>

struct AsyncState
{
    std::mutex mutex;
    std::condition_variable cv;

    bool done_called = false;
    bool controller_failed = false;
    std::string error_text;
};

TEST (NotConnectionTest, RpcChannelNotStartAndPendingCallShouldeBeDone)
{
    RpcChannel channel("127.0.0.1", 8000);
    // no channel.start()

    demo::UserService_Stub stub(&channel);

    auto request = std::make_shared<demo::LoginRequest>();
    auto response = std::make_shared<demo::LoginResponse>();
    auto controller = std::make_shared<SimpleRpcController>();
    auto state = std::make_shared<AsyncState>();

    request->set_name("Tom");
    request->set_password("123456");

    google::protobuf::Closure *done = SendResponseClosure(
        [request, response, controller, state]()
        {
            std::lock_guard<std::mutex> lock(state->mutex);

            state->done_called = true;
            state->controller_failed = controller->Failed();
            state->error_text = controller->error_text();

            state->cv.notify_one();
        });

    stub.Login(controller.get(), request.get(), response.get(), done);

    std::unique_lock<std::mutex> lock(state->mutex);
    bool ok = state->cv.wait_for(lock, std::chrono::seconds(3),[&](){
        return state->done_called == true;
    });

    ASSERT_TRUE(ok) << "done was not called";
    EXPECT_TRUE(state->controller_failed);
    EXPECT_FALSE(state->error_text.empty());
}