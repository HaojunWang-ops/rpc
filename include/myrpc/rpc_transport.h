#pragma once

#include <atomic>
#include <cstdint>
#include <mutex>
#include <string>

class RpcTransport
{
public:
    RpcTransport();
    ~RpcTransport();

    RpcTransport(const RpcTransport&) = delete;
    RpcTransport& operator=(const RpcTransport&) = delete;

    bool connectTo(const std::string& ip, uint16_t port, std::string* error);

    bool readN(void* buf, size_t n, const std::atomic<bool>& running);
    bool writeN(const void* buf, size_t n, const std::atomic<bool>& running);

    void shutdown();
    void close();

    int fd() const;

private:
    void setError(std::string* error, const std::string& msg);

private:
    mutable std::mutex fd_mutex_;
    std::atomic<int> fd_{-1};
};