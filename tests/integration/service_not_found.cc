#include "ghost_service.pb.h"
#include "rpc_channel_pool.h"
#include "rpc_closure.h"
#include "rpc_controller.h"
#include "rpc_header.pb.h"

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <future>
#include <memory>
#include <mutex>
#include <thread>

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

bool readAll(int fd, void* data, size_t size)
{
    char* p = static_cast<char*>(data);
    size_t left = size;
    while (left > 0)
    {
        ssize_t n = ::read(fd, p, left);
        if (n > 0)
        {
            p += n;
            left -= static_cast<size_t>(n);
            continue;
        }
        if (n < 0 && errno == EINTR)
        {
            continue;
        }
        return false;
    }
    return true;
}

bool writeAll(int fd, const void* data, size_t size)
{
    const char* p = static_cast<const char*>(data);
    size_t left = size;
    while (left > 0)
    {
        ssize_t n = ::write(fd, p, left);
        if (n > 0)
        {
            p += n;
            left -= static_cast<size_t>(n);
            continue;
        }
        if (n < 0 && errno == EINTR)
        {
            continue;
        }
        return false;
    }
    return true;
}

int createListeningSocket(uint16_t* port)
{
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
    {
        return -1;
    }

    int opt = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = ::htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;

    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0 ||
        ::listen(fd, 16) < 0)
    {
        ::close(fd);
        return -1;
    }

    sockaddr_in bound{};
    socklen_t len = sizeof(bound);
    if (::getsockname(fd, reinterpret_cast<sockaddr*>(&bound), &len) < 0)
    {
        ::close(fd);
        return -1;
    }

    *port = ::ntohs(bound.sin_port);
    return fd;
}

bool readRequestId(int fd, uint64_t* request_id)
{
    uint32_t net_total_size = 0;
    uint32_t net_header_size = 0;
    if (!readAll(fd, &net_total_size, sizeof(net_total_size)) ||
        !readAll(fd, &net_header_size, sizeof(net_header_size)))
    {
        return false;
    }

    uint32_t total_size = ::ntohl(net_total_size);
    uint32_t header_size = ::ntohl(net_header_size);
    if (total_size < 4 || header_size > total_size - 4)
    {
        return false;
    }

    std::string header_str(header_size, '\0');
    std::string body(total_size - 4 - header_size, '\0');
    if (!readAll(fd, header_str.data(), header_str.size()) ||
        !readAll(fd, body.data(), body.size()))
    {
        return false;
    }

    myrpc::RpcHeader header;
    if (!header.ParseFromString(header_str))
    {
        return false;
    }

    *request_id = header.request_id();
    return true;
}

bool sendServiceNotFoundResponse(int fd, uint64_t request_id)
{
    myrpc::RpcResponseHeader header;
    header.set_request_id(request_id);
    header.set_error_code(myrpc::RPC_SERVICE_NOT_FOUND);
    header.set_error_text("service not found");
    header.set_response_size(0);

    std::string header_str = header.SerializeAsString();
    uint32_t total_size = ::htonl(static_cast<uint32_t>(4 + header_str.size()));
    uint32_t header_size = ::htonl(static_cast<uint32_t>(header_str.size()));

    return writeAll(fd, &total_size, sizeof(total_size)) &&
           writeAll(fd, &header_size, sizeof(header_size)) &&
           writeAll(fd, header_str.data(), header_str.size());
}

bool sendOversizedResponseFrame(int fd)
{
    constexpr uint32_t kMaxResponseFrameSize = 64 * 1024 * 1024;
    uint32_t total_size = ::htonl(kMaxResponseFrameSize + 1);
    uint32_t header_size = ::htonl(0);

    return writeAll(fd, &total_size, sizeof(total_size)) &&
           writeAll(fd, &header_size, sizeof(header_size));
}
}

// 服务端找不到 service 时，客户端必须得到失败 controller 和一次 done。
TEST(ServerErrorTest, ServiceNotFoundShouldCallDoneAndSetControllerFailed)
{
    uint16_t port = 0;
    int listen_fd = createListeningSocket(&port);
    ASSERT_GE(listen_fd, 0);

    std::promise<void> server_done;
    auto server_done_future = server_done.get_future();

    std::thread server_thread([listen_fd, done = std::move(server_done)]() mutable {
        int conn_fd = ::accept(listen_fd, nullptr, nullptr);
        if (conn_fd >= 0)
        {
            uint64_t request_id = 0;
            if (readRequestId(conn_fd, &request_id))
            {
                sendServiceNotFoundResponse(conn_fd, request_id);
            }
            ::close(conn_fd);
        }
        ::close(listen_fd);
        done.set_value();
    });

    RpcChannelPool pool("127.0.0.1", port, 1);
    bool started = pool.start();
    if (!started)
    {
        ::shutdown(listen_fd, SHUT_RDWR);
        server_thread.join();
        FAIL() << "pool.start() failed";
    }

    ghost::GhostService_Stub stub(&pool);

    auto request = std::make_shared<ghost::GhostRequest>();
    auto response = std::make_shared<ghost::GhostResponse>();
    auto controller = std::make_shared<SimpleRpcController>();
    auto state = std::make_shared<AsyncState>();

    request->set_name("Tom");

    google::protobuf::Closure* done = SendResponseClosure(
        [request, response, controller, state] {
            std::lock_guard<std::mutex> lock(state->mutex);
            ++state->done_count;
            state->controller_failed = controller->Failed();
            state->error_text = controller->ErrorText();
            state->cv.notify_one();
        });

    stub.GhostCall(controller.get(), request.get(), response.get(), done);

    {
        std::unique_lock<std::mutex> lock(state->mutex);
        ASSERT_TRUE(state->cv.wait_for(lock, std::chrono::seconds(2), [&] {
            return state->done_count == 1;
        }));
    }

    EXPECT_EQ(state->done_count, 1);
    EXPECT_TRUE(state->controller_failed);
    EXPECT_FALSE(state->error_text.empty());

    pool.stop();
    ASSERT_EQ(server_done_future.wait_for(std::chrono::seconds(1)),
              std::future_status::ready);
    server_thread.join();
}

// 客户端拒绝超大响应帧后，应失败完成当前调用而不影响完成次数。
TEST(ServerErrorTest, OversizedResponseFrameShouldFailAndCallDoneOnce)
{
    uint16_t port = 0;
    int listen_fd = createListeningSocket(&port);
    ASSERT_GE(listen_fd, 0);

    std::promise<void> server_done;
    auto server_done_future = server_done.get_future();

    std::thread server_thread([listen_fd, done = std::move(server_done)]() mutable {
        int conn_fd = ::accept(listen_fd, nullptr, nullptr);
        if (conn_fd >= 0)
        {
            uint64_t request_id = 0;
            if (readRequestId(conn_fd, &request_id))
            {
                sendOversizedResponseFrame(conn_fd);
            }
            ::close(conn_fd);
        }
        ::close(listen_fd);
        done.set_value();
    });

    RpcChannelPool pool("127.0.0.1", port, 1);
    bool started = pool.start();
    if (!started)
    {
        ::shutdown(listen_fd, SHUT_RDWR);
        server_thread.join();
        FAIL() << "pool.start() failed";
    }

    ghost::GhostService_Stub stub(&pool);

    auto request = std::make_shared<ghost::GhostRequest>();
    auto response = std::make_shared<ghost::GhostResponse>();
    auto controller = std::make_shared<SimpleRpcController>();
    auto state = std::make_shared<AsyncState>();

    request->set_name("Tom");

    google::protobuf::Closure* done = SendResponseClosure(
        [request, response, controller, state] {
            std::lock_guard<std::mutex> lock(state->mutex);
            ++state->done_count;
            state->controller_failed = controller->Failed();
            state->error_text = controller->ErrorText();
            state->cv.notify_one();
        });

    stub.GhostCall(controller.get(), request.get(), response.get(), done);

    {
        std::unique_lock<std::mutex> lock(state->mutex);
        ASSERT_TRUE(state->cv.wait_for(lock, std::chrono::seconds(2), [&] {
            return state->done_count == 1;
        }));
    }

    EXPECT_EQ(state->done_count, 1);
    EXPECT_TRUE(state->controller_failed);
    EXPECT_FALSE(state->error_text.empty());

    pool.stop();
    ASSERT_EQ(server_done_future.wait_for(std::chrono::seconds(1)),
              std::future_status::ready);
    server_thread.join();
}


/*
测试内容：
client 发起 RPC
fake server 读出 request_id
fake server 返回 RpcResponseHeader(error_code = RPC_SERVICE_NOT_FOUND)
client reader 线程读到合法 error response
handleResponseFrame()
finishCallWithError()
done 执行一次
controller->Failed() == true
*/
