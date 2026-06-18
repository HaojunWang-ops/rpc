#include "rpc_header.pb.h"
#include "tcpserver.h"
#include "user.pb.h"

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstring>
#include <string>
#include <thread>

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

std::string buildLoginFrame(uint64_t request_id,
                            const std::string& name,
                            const std::string& password)
{
    demo::LoginRequest request;
    request.set_name(name);
    request.set_password(password);
    std::string args = request.SerializeAsString();

    myrpc::RpcHeader header;
    header.set_request_id(request_id);
    header.set_service_name("demo.UserService");
    header.set_method_name("Login");
    header.set_args_size(static_cast<uint32_t>(args.size()));
    std::string header_str = header.SerializeAsString();

    uint32_t header_size = static_cast<uint32_t>(header_str.size());
    uint32_t total_size = static_cast<uint32_t>(4 + header_size + args.size());

    uint32_t net_total_size = ::htonl(total_size);
    uint32_t net_header_size = ::htonl(header_size);

    std::string frame;
    frame.append(reinterpret_cast<const char*>(&net_total_size), sizeof(net_total_size));
    frame.append(reinterpret_cast<const char*>(&net_header_size), sizeof(net_header_size));
    frame.append(header_str);
    frame.append(args);
    return frame;
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

bool readLoginResponse(int fd, uint64_t* request_id, demo::LoginResponse* response)
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

    myrpc::RpcResponseHeader header;
    if (!header.ParseFromString(header_str))
    {
        return false;
    }

    if (request_id)
    {
        *request_id = header.request_id();
    }

    return header.error_code() == myrpc::RPC_OK &&
           header.response_size() == body.size() &&
           response->ParseFromString(body);
}
}

TEST(TcpFragmentTest, HalfPacketShouldBeAssembled)
{
    ControlledTcpServer server(0, buildLoginResponseBody);
    ASSERT_TRUE(server.start());

    int fd = connectTo(server.port());
    ASSERT_GE(fd, 0);

    std::string frame = buildLoginFrame(1, "haojun", "123456");
    size_t half = frame.size() / 2;

    ASSERT_TRUE(writeAll(fd, frame.data(), half));
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    ASSERT_TRUE(writeAll(fd, frame.data() + half, frame.size() - half));

    uint64_t request_id = 0;
    demo::LoginResponse response;
    ASSERT_TRUE(readLoginResponse(fd, &request_id, &response));
    EXPECT_EQ(request_id, 1u);
    EXPECT_TRUE(response.success());

    ::close(fd);
    server.stop();
}

TEST(TcpFragmentTest, TwoFullPacketsInOneWriteShouldReturnTwoResponses)
{
    ControlledTcpServer server(0, buildLoginResponseBody);
    ASSERT_TRUE(server.start());

    int fd = connectTo(server.port());
    ASSERT_GE(fd, 0);

    std::string first = buildLoginFrame(1, "haojun", "123456");
    std::string second = buildLoginFrame(2, "tom", "pw");
    std::string combined = first + second;

    ASSERT_TRUE(writeAll(fd, combined.data(), combined.size()));

    uint64_t first_id = 0;
    uint64_t second_id = 0;
    demo::LoginResponse first_response;
    demo::LoginResponse second_response;

    ASSERT_TRUE(readLoginResponse(fd, &first_id, &first_response));
    ASSERT_TRUE(readLoginResponse(fd, &second_id, &second_response));

    EXPECT_EQ(first_id, 1u);
    EXPECT_EQ(second_id, 2u);
    EXPECT_TRUE(first_response.success());
    EXPECT_TRUE(second_response.success());

    ::close(fd);
    server.stop();
}

TEST(TcpFragmentTest, OneAndHalfPacketsThenRemainderShouldReturnTwoResponses)
{
    ControlledTcpServer server(0, buildLoginResponseBody);
    ASSERT_TRUE(server.start());

    int fd = connectTo(server.port());
    ASSERT_GE(fd, 0);

    std::string first = buildLoginFrame(1, "haojun", "123456");
    std::string second = buildLoginFrame(2, "tom", "pw");
    size_t second_half = second.size() / 2;
    std::string first_part = first + second.substr(0, second_half);

    ASSERT_TRUE(writeAll(fd, first_part.data(), first_part.size()));

    uint64_t first_id = 0;
    demo::LoginResponse first_response;
    ASSERT_TRUE(readLoginResponse(fd, &first_id, &first_response));
    EXPECT_EQ(first_id, 1u);
    EXPECT_TRUE(first_response.success());

    ASSERT_TRUE(writeAll(fd, second.data() + second_half, second.size() - second_half));

    uint64_t second_id = 0;
    demo::LoginResponse second_response;
    ASSERT_TRUE(readLoginResponse(fd, &second_id, &second_response));
    EXPECT_EQ(second_id, 2u);
    EXPECT_TRUE(second_response.success());

    ::close(fd);
    server.stop();
}
