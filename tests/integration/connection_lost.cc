#include "rpc_channel.h"
#include "rpc_controller.h"
#include "rpc_closure.h"
#include "user.pb.h"

#include <gtest/gtest.h>
#include <condition_variable>
#include <mutex>
#include <memory>
#include <chrono>
#include <sys/socket.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>

TEST(ConnectionLostTest, AllPendingCallShouldFailedAndDoneShouldeBeCalled)
{
    const int kRequestCount = 100;
    const int port = 18000;

    std::thread server_thread([&](){
        int listenfd = ::socket(AF_INET, SOCK_STREAM, 0);

        int opt = 1;
        ::setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        struct sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = ::htons(port);
        addr.sin_addr.s_addr = ::htonl(INADDR_LOOPBACK);

        ASSERT_EQ(::bind(listenfd, reinterpret_cast<sockaddr*> (&addr), sizeof(addr)), 0);
        ASSERT_EQ(::listen(listenfd, 128), 0);

        int connfd = ::accept(listenfd, nullptr, nullptr);
        ASSERT_GE(connfd, 0);

        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        ::close(connfd);
        ::close(listenfd);
    });

    RpcChannel channel("127.0.0.1", 18000);
    ASSERT_TRUE(channel.start());
    demo::UserService_Stub stub(&channel);

    std::mutex mutex;
    std::condition_variable cv;
    int done_count = 0;
    int failed_count = 0;

    for (int i = 0; i < kRequestCount; i++)
    {
        auto request = std::make_shared<demo::LoginRequest>();
        auto response = std::make_shared<demo::LoginResponse>();
        auto controller = std::make_shared<SimpleRpcController>();

        request->set_name("Tom");
        request->set_password("123456");

        auto* done = SendResponseClosure(
            [request, response, controller, &mutex, &cv, &done_count, &failed_count, kRequestCount]()
        {
            std::lock_guard<std::mutex> lock(mutex);

            ++done_count;
            if (controller->Failed())
            {
                ++failed_count;
            }

            if (done_count == kRequestCount)
            {
                cv.notify_one();
            }
        });

        stub.Login(controller.get(), request.get(), response.get(), done);
    }

    {
        std::unique_lock<std::mutex> lock(mutex);
        bool ok = cv.wait_for(lock, std::chrono::seconds(3), [&]()
    {
        return done_count == kRequestCount;
    });

    ASSERT_TRUE(ok) << "not all pending calls were completed";
    EXPECT_EQ(done_count, kRequestCount);
    EXPECT_EQ(failed_count, kRequestCount);
    }

    server_thread.join();
}