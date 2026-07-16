#include "rpc_channel_pool.h"
#include "rpc_closure.h"
#include "rpc_controller.h"
#include "tcpserver.h"
#include "user.pb.h"

#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>

namespace
{
std::string buildLoginResponseBody(const myrpc::RpcHeader&,
                                   const std::string&)
{
    demo::LoginResponse response;
    response.set_code(0);
    response.set_message("login success");
    response.set_success(true);
    return response.SerializeAsString();
}
}

TEST(BatchAsyncTest, AllAsyncCallsShouldCompleteExactlyOnce)
{
    constexpr int kRequestCount = 100;

    ControlledTcpServer server(0, buildLoginResponseBody);
    ASSERT_TRUE(server.start());

    RpcChannelPool pool("127.0.0.1", server.port(), 2);
    ASSERT_TRUE(pool.start());

    demo::UserService_Stub stub(&pool);

    std::mutex mutex;
    std::condition_variable cv;
    int done_count = 0;
    int failed_count = 0;
    int success_response_count = 0;

    for (int i = 0; i < kRequestCount; ++i)
    {
        auto request = std::make_shared<demo::LoginRequest>();
        auto response = std::make_shared<demo::LoginResponse>();
        auto controller = std::make_shared<SimpleRpcController>();

        request->set_name("haojun");
        request->set_password("123456");

        google::protobuf::Closure* done = SendResponseClosure(
            [request, response, controller, &mutex, &cv,
             &done_count, &failed_count, &success_response_count] {
                std::lock_guard<std::mutex> lock(mutex);
                ++done_count;
                if (controller->Failed())
                {
                    ++failed_count;
                }
                if (response->success())
                {
                    ++success_response_count;
                }
                cv.notify_all();
            });

        stub.Login(controller.get(), request.get(), response.get(), done);
    }

    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(3), [&] {
            return done_count == kRequestCount;
        }));
    }

    EXPECT_EQ(done_count, kRequestCount);
    EXPECT_EQ(failed_count, 0);
    EXPECT_EQ(success_response_count, kRequestCount);
    EXPECT_TRUE(server.waitForTotalRequests(kRequestCount, std::chrono::seconds(1)));

    pool.stop();
    server.stop();
}

/*测试意图：
发送异步请求， 所有回调都应该complete exactly once，并且都成功完成。
*/