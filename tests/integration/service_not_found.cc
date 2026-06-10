#include "rpc_channel.h"
#include "rpc_controller.h"
#include "ghost_service.pb.h"
#include "rpc_closure.h"

#include <gtest/gtest.h>
#include <condition_variable>
#include <mutex>
#include <memory>
#include <chrono>

struct AsyncState
{
    std::mutex mutex;
    std::condition_variable cv;

    bool done_called = false;
    bool controller_failed = false;
    std::string error_text;
};

TEST(ServerErrorTest, ServiceNotFoundShouldCallDoneAndSetControllerFailed)
{
    RpcChannel channel("127.0.0.1", 8000);
    ASSERT_TRUE(channel.start());

    ghost::GhostService_Stub stub(&channel);

    auto request = std::make_shared<ghost::GhostRequest>();
    auto response = std::make_shared<ghost::GhostResponse>();
    auto controller = std::make_shared<SimpleRpcController>();
    auto state = std::make_shared<AsyncState>();

    request->set_name("Tom");

    google::protobuf::Closure* done = SendResponseClosure(
        [request, response, controller, state]()
        {
            std::lock_guard<std::mutex> lock(state->mutex);

            state->done_called = true;
            state->controller_failed = controller->Failed();
            state->error_text = controller->error_text();

            state->cv.notify_one();
        }
    );

    stub.GhostCall(controller.get(), request.get(), response.get(), done);

    std::unique_lock<std::mutex> lock(state->mutex);
    bool ok = state->cv.wait_for(
        lock, 
        std::chrono::seconds(2),
        [&state]()
        {
            return state->done_called;
        }
    );

    ASSERT_TRUE(ok) << "done was not called";
    EXPECT_TRUE(state->controller_failed);
    EXPECT_FALSE(state->error_text.empty());
}

