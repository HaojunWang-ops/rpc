#include "rpc_transport.h"

#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <dirent.h>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <unistd.h>

namespace
{
class LocalTcpListener
{
public:
    LocalTcpListener()
    {
        fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd_ < 0)
        {
            return;
        }

        int reuse = 1;
        ::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = ::htonl(INADDR_LOOPBACK);
        addr.sin_port = 0;

        if (::bind(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0)
        {
            close();
            return;
        }

        if (::listen(fd_, 4) < 0)
        {
            close();
            return;
        }

        sockaddr_in bound_addr{};
        socklen_t len = sizeof(bound_addr);
        if (::getsockname(fd_, reinterpret_cast<sockaddr*>(&bound_addr), &len) < 0)
        {
            close();
            return;
        }

        port_ = ::ntohs(bound_addr.sin_port);
    }

    ~LocalTcpListener()
    {
        close();
    }

    bool valid() const
    {
        return fd_ >= 0 && port_ != 0;
    }

    uint16_t port() const
    {
        return port_;
    }

private:
    void close()
    {
        if (fd_ >= 0)
        {
            ::close(fd_);
            fd_ = -1;
        }
    }

private:
    int fd_ = -1;
    uint16_t port_ = 0;
};

int openFdCount()
{
    DIR* dir = ::opendir("/proc/self/fd");
    if (!dir)
    {
        return -1;
    }

    int count = 0;
    while (::readdir(dir) != nullptr)
    {
        ++count;
    }

    ::closedir(dir);
    return count;
}
}

TEST(RpcTransportTest, ConnectToLocalListenerSucceedsAndStoresFd)
{
    LocalTcpListener listener;
    ASSERT_TRUE(listener.valid()) << std::strerror(errno);

    RpcTransport transport;
    transport.setConnectTimeoutMs(100);

    std::string error;
    ASSERT_TRUE(transport.connectTo("127.0.0.1", listener.port(), &error)) << error;

    EXPECT_GE(transport.fd(), 0);
    EXPECT_EQ(transport.connectTimeoutMs(), 100);

    transport.close();
    EXPECT_EQ(transport.fd(), -1);
}

TEST(RpcTransportTest, ConnectToBlackholeAddressUsesConfiguredTimeout)
{
    constexpr int kTimeoutMs = 50;
    constexpr auto kUpperBound = std::chrono::milliseconds(1000);

    const int fd_count_before = openFdCount();
    ASSERT_GE(fd_count_before, 0);

    RpcTransport transport;
    transport.setConnectTimeoutMs(kTimeoutMs);

    std::string error;
    auto start = std::chrono::steady_clock::now();
    bool ok = transport.connectTo("203.0.113.1", 9, &error);
    int saved_errno = errno;
    auto elapsed = std::chrono::steady_clock::now() - start;

    if (ok)
    {
        transport.close();
        GTEST_SKIP() << "blackhole test address unexpectedly accepted a connection";
    }

    EXPECT_EQ(transport.fd(), -1);
    EXPECT_EQ(openFdCount(), fd_count_before);

    if (saved_errno != ETIMEDOUT)
    {
        GTEST_SKIP() << "blackhole test address failed without timing out: "
                     << std::strerror(saved_errno);
    }

    EXPECT_GE(elapsed, std::chrono::milliseconds(kTimeoutMs));
    EXPECT_LT(elapsed, kUpperBound);
    EXPECT_NE(error.find("timed out"), std::string::npos) << error;
}
