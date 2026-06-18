#include "rpc_header.pb.h"
#include "tcpserver.h"

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstring>
#include <functional>
#include <string>
#include <thread>

namespace
{
std::string emptyResponse(const myrpc::RpcHeader&, const std::string&)
{
    return {};
}

bool waitUntil(std::chrono::milliseconds timeout, const std::function<bool()>& pred)
{
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline)
    {
        if (pred())
        {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return pred();
}

int connectTo(uint16_t port)
{
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
    {
        return -1;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = ::htons(port);
    addr.sin_addr.s_addr = ::htonl(INADDR_LOOPBACK);

    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0)
    {
        ::close(fd);
        return -1;
    }

    return fd;
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
}

TEST(BadPacketTest, TotalSizeLessThanHeaderSizeFieldShouldBeRejected)
{
    ControlledTcpServer server(0, emptyResponse);
    ASSERT_TRUE(server.start());

    int fd = connectTo(server.port());
    ASSERT_GE(fd, 0);

    uint32_t total_size = ::htonl(3);
    ASSERT_TRUE(writeAll(fd, &total_size, sizeof(total_size)));

    EXPECT_TRUE(waitUntil(std::chrono::seconds(1), [&] {
        return server.badFrameCount() == 1;
    }));

    ::close(fd);
    server.stop();
}

TEST(BadPacketTest, HeaderSizeGreaterThanBodyShouldBeRejected)
{
    ControlledTcpServer server(0, emptyResponse);
    ASSERT_TRUE(server.start());

    int fd = connectTo(server.port());
    ASSERT_GE(fd, 0);

    uint32_t total_size = ::htonl(4);
    uint32_t header_size = ::htonl(100);
    ASSERT_TRUE(writeAll(fd, &total_size, sizeof(total_size)));
    ASSERT_TRUE(writeAll(fd, &header_size, sizeof(header_size)));

    EXPECT_TRUE(waitUntil(std::chrono::seconds(1), [&] {
        return server.badFrameCount() == 1;
    }));

    ::close(fd);
    server.stop();
}

TEST(BadPacketTest, ArgsSizeMismatchShouldBeRejected)
{
    ControlledTcpServer server(0, emptyResponse);
    ASSERT_TRUE(server.start());

    int fd = connectTo(server.port());
    ASSERT_GE(fd, 0);

    myrpc::RpcHeader header;
    header.set_request_id(1);
    header.set_service_name("demo.UserService");
    header.set_method_name("Login");
    header.set_args_size(100);

    std::string header_str = header.SerializeAsString();
    uint32_t total_size = ::htonl(static_cast<uint32_t>(4 + header_str.size()));
    uint32_t header_size = ::htonl(static_cast<uint32_t>(header_str.size()));

    ASSERT_TRUE(writeAll(fd, &total_size, sizeof(total_size)));
    ASSERT_TRUE(writeAll(fd, &header_size, sizeof(header_size)));
    ASSERT_TRUE(writeAll(fd, header_str.data(), header_str.size()));

    EXPECT_TRUE(waitUntil(std::chrono::seconds(1), [&] {
        return server.badFrameCount() == 1;
    }));

    ::close(fd);
    server.stop();
}
