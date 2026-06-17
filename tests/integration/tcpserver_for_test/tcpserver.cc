#include "tcpserver.h"

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <netinet/in.h>
#include <cerrno>
#include <atomic>
ControlledTcpServer::ControlledTcpServer(uint16_t port, ResponseBuilder builder)
    : port_(port), response_builder_(builder)
{
}

ControlledTcpServer::~ControlledTcpServer()
{
    stop();
}

bool ControlledTcpServer::start()
{
    bool expected = false;
    if (!running_.compare_exchange_strong(expected, true))
    {
        return false;
    }

    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0)
    {
        running_.store(false, std::memory_order_release);
        return false;
    }

    int opt = 1;
    ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    uint16_t port = ::htons(port_);

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = port;

    if (::bind(listen_fd_, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0)
    {
        ::close(listen_fd_);
        listen_fd_ = -1;
        running_.store(false, std::memory_order_release);
        return false;
    }

    if (::listen(listen_fd_, 64) < 0)
    {
        ::close(listen_fd_);
        listen_fd_ = -1;
        running_.store(false, std::memory_order_release);
        return false;
    }

    accept_thread_ = std::thread(&ControlledTcpServer::acceptLoop, this);
    return true;
}

void ControlledTcpServer::stop()
{
    bool expected = true;
    if (!running_.compare_exchange_strong(expected, false))
    {
        return;
    }

    if (listen_fd_ >= 0)
    {
        ::shutdown(listen_fd_, SHUT_RDWR);
        ::close(listen_fd_);
        listen_fd_.store(-1, std::memory_order_release);
    }
    if (accept_thread_.joinable())
    {
        accept_thread_.join();
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);

        for (auto &[id, conn] : conns_)
        {
            if (conn.fd >= 0)
            {
                ::shutdown(conn.fd, SHUT_RDWR);
            }
        }
        cv_.notify_all();
    }

    std::vector<std::thread> threads_to_join;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        threads_to_join.swap(connection_threads_);
    }

    for (auto &thread_to_join : threads_to_join)
    {
        if (thread_to_join.joinable())
        {
            thread_to_join.join();
        }
    }
}

size_t ControlledTcpServer::acceptCount() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return accept_count_;
}

size_t ControlledTcpServer::activeCount() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return active_count_;
}

size_t ControlledTcpServer::totalRequestCount() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return total_request_count_;
}

size_t ControlledTcpServer::badFrameCount() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return bad_frame_count_;
}

size_t ControlledTcpServer::requestCountOf(size_t conn_id) const
{
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = conns_.find(conn_id);
    if (it == conns_.end())
    {
        return 0;
    }

    return it->second.request_count;
}

std::vector<size_t> ControlledTcpServer::connectionIds() const
{
    std::lock_guard<std::mutex> lock(mutex_);

    std::vector<size_t> ids;
    ids.reserve(conns_.size());

    for (const auto &[id, _] : conns_)
    {
        ids.push_back(id);
    }

    return ids;
}

bool ControlledTcpServer::waitForAcceptCount(size_t n, std::chrono::milliseconds timeout)
{
    std::unique_lock<std::mutex> lock(mutex_);
    return cv_.wait_for(lock, timeout, [&]
                        { return accept_count_ >= n; });
}

bool ControlledTcpServer::waitForActiveConnections(size_t n, std::chrono::milliseconds timeout)
{
    std::unique_lock<std::mutex> lock(mutex_);
    return cv_.wait_for(lock, timeout, [&]
                        { return active_count_ == n; });
}

bool ControlledTcpServer::waitForTotalRequests(size_t n, std::chrono::milliseconds timeout)
{
    std::unique_lock<std::mutex> lock(mutex_);
    return cv_.wait_for(lock, timeout, [&]
                        { return total_request_count_ >= n; });
}

bool ControlledTcpServer::waitForNewConnectionAfter(size_t old_accept_count,
                                                    std::chrono::milliseconds timeout)
{
    std::unique_lock<std::mutex> lock(mutex_);
    return cv_.wait_for(lock, timeout, [&]
                        { return accept_count_ > old_accept_count; });
}

void ControlledTcpServer::closeConnection(size_t conn_id)
{
    {
        std::lock_guard<std::mutex> lock(mutex_);

        auto it = conns_.find(conn_id);
        if (it == conns_.end())
        {
            return;
        }

        int fd = it->second.fd;

        if (fd >= 0)
        {
            // fd 所有权仍然属于 connectionLoop。
            // shutdown 足够让客户端 reader 感知连接断开。
            ::shutdown(fd, SHUT_RDWR);
        }
    }
}

void ControlledTcpServer::closeOneConnection()
{

    {
        std::lock_guard<std::mutex> lock(mutex_);
        int fd = -1;
        for (auto &[id, conn] : conns_)
        {
            if (conn.fd >= 0)
            {
                fd = conn.fd;
                break;
            }
        }
        if (fd >= 0)
        {
            ::shutdown(fd, SHUT_RDWR);
        }
    }
}

static bool readN(int fd, void *buf, size_t n)
{
    char *p = static_cast<char *>(buf);
    size_t left = n;

    while (left > 0)
    {
        ssize_t ret = ::read(fd, p, left);

        if (ret == 0)
        {
            return false;
        }

        if (ret < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }

            return false;
        }

        p += ret;
        left -= static_cast<size_t>(ret);
    }

    return true;
}

static bool writeN(int fd, const void *buf, size_t n)
{
    const char *p = static_cast<const char *>(buf);
    size_t left = n;

    while (left > 0)
    {
        ssize_t ret = ::write(fd, p, left);

        if (ret < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }

            return false;
        }

        if (ret == 0)
        {
            return false;
        }

        p += ret;
        left -= static_cast<size_t>(ret);
    }

    return true;
}

void ControlledTcpServer::acceptLoop()
{
    while (running_.load())
    {
        struct sockaddr_in peer{};
        socklen_t len = sizeof(peer);

        int conn_fd = ::accept(listen_fd_.load(std::memory_order_acquire), reinterpret_cast<sockaddr *>(&peer), &len);

        if (conn_fd < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }
            if (!running_.load())
            {
                break;
            }
            continue;
        }

        size_t conn_id = 0;
        {
            std::lock_guard<std::mutex> lock(mutex_);

            conn_id = next_conn_id_++;

            ConnState state;
            state.fd = conn_fd;

            conns_[conn_id] = state;
            accept_count_++;
            active_count_++;

            connection_threads_.emplace_back(
                &ControlledTcpServer::connectLoop,
                this,
                conn_id,
                conn_fd);

            cv_.notify_all();
        }
    }
}

void ControlledTcpServer::connectLoop(size_t conn_id, int conn_fd)
{
    while (running_.load())
    {
        uint32_t net_total_size = 0;
        if (!readN(conn_fd, &net_total_size, sizeof(net_total_size)))
        {
            break;
        }

        uint32_t total_size = ::ntohl(net_total_size);
        if (total_size < 4)
        {
            increaseBadFrame();
            break;
        }

        uint32_t net_header_size = 0;
        if (!readN(conn_fd, &net_header_size, sizeof(net_header_size)))
        {
            break;
        }

        uint32_t header_size = ::ntohl(net_header_size);
        if (header_size > total_size - 4)
        {
            increaseBadFrame();
            break;
        }

        std::string header_str(header_size, '\0');
        if (!readN(conn_fd, header_str.data(), header_str.size()))
        {
            break;
        }

        myrpc::RpcHeader header;
        if (!header.ParseFromString(header_str))
        {
            increaseBadFrame();
            break;
        }

        size_t body_size = total_size - 4 - header_size;
        std::string request_body(body_size, '\0');
        if (header.args_size() != body_size)
        {
            increaseBadFrame();
            break;
        }

        if (!readN(conn_fd, request_body.data(), request_body.size()))
        {
            break;
        }

        {
            std::lock_guard<std::mutex> lock(mutex_);
            auto it = conns_.find(conn_id);
            if (it != conns_.end())
            {
                it->second.request_count++;
            }

            total_request_count_++;

            cv_.notify_all();
        }

        std::string response_body;
        if (response_builder_)
        {
            response_body = response_builder_(header, request_body);
        }

        myrpc::RpcResponseHeader response_header;
        response_header.set_request_id(header.request_id());
        response_header.set_error_code(myrpc::RPC_OK);
        response_header.set_response_size(response_body.size());

        std::string response_header_str;
        if (!response_header.SerializeToString(&response_header_str))
        {
            break;
        }

        uint32_t response_total_size = static_cast<uint32_t>(4 + response_header_str.size() + response_body.size());
        uint32_t net_response_total_size = ::htonl(response_total_size);
        uint32_t net_response_header_size = ::htonl(response_header_str.size());

        std::string out;
        out.reserve(4 + response_total_size);

        out.append(reinterpret_cast<const char *>(&net_response_total_size), 4);
        out.append(reinterpret_cast<const char *>(&net_response_header_size), 4);
        out.append(response_header_str);
        out.append(response_body);

        if (!writeN(conn_fd, out.data(), out.size()))
        {
            break;
        }
    }

    ::close(conn_fd);

    {
        std::lock_guard<std::mutex> lock(mutex_);

        auto it = conns_.find(conn_id);
        if (it != conns_.end())
        {
            it->second.fd = -1;
            it->second.closed = true;
        }

        if (active_count_ > 0)
        {
            active_count_--;
        }

        cv_.notify_all();
    }
}

void ControlledTcpServer::increaseBadFrame()
{
    {
        std::lock_guard<std::mutex> lock(mutex_);
        bad_frame_count_++;
        cv_.notify_all();
    }
}