#include "rpc_header.pb.h"
#include "user.pb.h"

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstring>
#include <future>
#include <string>
#include <thread>

namespace
{
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

std::string buildUnknownServiceRequest()
{
    myrpc::RpcHeader header;
    header.set_request_id(11);
    header.set_service_name("demo.NotExistService");
    header.set_method_name("Login");
    header.set_args_size(0);

    std::string header_str = header.SerializeAsString();
    uint32_t total_size = ::htonl(static_cast<uint32_t>(4 + header_str.size()));
    uint32_t header_size = ::htonl(static_cast<uint32_t>(header_str.size()));

    std::string frame;
    frame.append(reinterpret_cast<const char*>(&total_size), sizeof(total_size));
    frame.append(reinterpret_cast<const char*>(&header_size), sizeof(header_size));
    frame.append(header_str);
    return frame;
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
    myrpc::RpcResponseHeader response_header;
    response_header.set_request_id(request_id);
    response_header.set_error_code(myrpc::RPC_SERVICE_NOT_FOUND);
    response_header.set_error_text("service not found");
    response_header.set_response_size(0);

    std::string header_str = response_header.SerializeAsString();
    uint32_t total_size = ::htonl(static_cast<uint32_t>(4 + header_str.size()));
    uint32_t header_size = ::htonl(static_cast<uint32_t>(header_str.size()));

    return writeAll(fd, &total_size, sizeof(total_size)) &&
           writeAll(fd, &header_size, sizeof(header_size)) &&
           writeAll(fd, header_str.data(), header_str.size());
}

bool readResponseHeader(int fd, myrpc::RpcResponseHeader* response_header)
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

    return response_header->ParseFromString(header_str);
}
}

TEST(ServiceNotFoundRawProtocolTest, UnknownServiceShouldReceiveErrorFrame)
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

    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(fd, 0);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = ::htonl(INADDR_LOOPBACK);
    addr.sin_port = ::htons(port);
    ASSERT_EQ(::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);

    std::string request = buildUnknownServiceRequest();
    ASSERT_TRUE(writeAll(fd, request.data(), request.size()));

    myrpc::RpcResponseHeader response_header;
    ASSERT_TRUE(readResponseHeader(fd, &response_header));

    EXPECT_EQ(response_header.request_id(), 11u);
    EXPECT_EQ(response_header.error_code(), myrpc::RPC_SERVICE_NOT_FOUND);
    EXPECT_FALSE(response_header.error_text().empty());
    EXPECT_EQ(response_header.response_size(), 0u);

    ::close(fd);
    ASSERT_EQ(server_done_future.wait_for(std::chrono::seconds(1)),
              std::future_status::ready);
    server_thread.join();
}
