#include "rpc_channel_pool.h"
#include "rpc_closure.h"
#include "rpc_controller.h"
#include "tcpserver.h"
#include "user.pb.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>

namespace
{
std::string buildLoginResponseBody(const myrpc::RpcHeader&,
                                   const std::string&)
{
    demo::LoginResponse response;
    response.set_code(0);
    response.set_message("ok");
    response.set_success(true);
    return response.SerializeAsString();
}
}

TEST(ConnectionLostTest, AllPendingCallsShouldFailAndCallDoneOnce)
{
    constexpr int kPoolSize = 4;
    constexpr int kRequestCount = 20;

    std::atomic<bool> release_response{false};
    ControlledTcpServer server(0, [&](const myrpc::RpcHeader& header,
                                      const std::string& body) {
        while (!release_response.load(std::memory_order_acquire))
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        return buildLoginResponseBody(header, body);
    });
    ASSERT_TRUE(server.start());

    RpcChannelPool pool("127.0.0.1", server.port(), kPoolSize);
    ASSERT_TRUE(pool.start());
    ASSERT_TRUE(server.waitForAcceptCount(kPoolSize, std::chrono::seconds(1)));

    demo::UserService_Stub stub(&pool);

    std::mutex mutex;
    std::condition_variable cv;
    int done_count = 0;
    int failed_count = 0;

    for (int i = 0; i < kRequestCount; ++i)
    {
        auto request = std::make_shared<demo::LoginRequest>();
        auto response = std::make_shared<demo::LoginResponse>();
        auto controller = std::make_shared<SimpleRpcController>();

        request->set_name("Tom");
        request->set_password("123456");

        auto* done = SendResponseClosure(
            [request, response, controller, &mutex, &cv, &done_count, &failed_count] {
                std::lock_guard<std::mutex> lock(mutex);
                ++done_count;
                if (controller->Failed())
                {
                    ++failed_count;
                }
                cv.notify_all();
            });

        stub.Login(controller.get(), request.get(), response.get(), done);
    }

    //handler阻塞住，tcpserver只能拿到每个线程首个连接
    ASSERT_TRUE(server.waitForTotalRequests(kPoolSize, std::chrono::seconds(1)));

    for (size_t conn_id : server.connectionIds())
    {
        server.closeConnection(conn_id);
    }
    release_response.store(true, std::memory_order_release);

    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(3), [&] {
            return done_count == kRequestCount;
        }));
    }

    EXPECT_EQ(done_count, kRequestCount);
    EXPECT_EQ(failed_count, kRequestCount);

    pool.stop();
    server.stop();
}

/*
测试意图：
1. 客户端请求已经发出
2. 服务端 handler 卡住，不返回 response
3. 测试线程先关闭连接
4. 再 release_response = true
5. 服务端 handler 返回 response body
6. TcpServer 尝试发送 response
7. 但连接已经关闭，发送失败或者直接放弃
8. 客户端 reader 已经读到 EOF / error
9. 客户端 pending 全部 fail
*/