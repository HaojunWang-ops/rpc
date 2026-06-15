#pragma once

#include <chrono>
#include <stdio.h>
#include <functional>
#include <string>
#include <thread>
#include <condition_variable>
#include <mutex>
#include <map>
#include <vector>

#include "rpc_header.pb.h"

class ControlledTcpServer
{
public:
    using ResponseBuilder = std::function<std::string(const myrpc::RpcHeader& req_header,
                                                      const std::string& request_body)>;
    ControlledTcpServer(uint16_t, ResponseBuilder builder);
    ~ControlledTcpServer();
    
    bool start();
    void stop();
    void increaseBadFrame();

    size_t acceptCount() const;
    size_t activeCount() const;
    size_t totalRequestCount() const;
    size_t badFrameCount() const;
    size_t requestCountOf(size_t conn_id) const;
    std::vector<size_t> connectionIds() const;

    bool waitForAcceptCount(size_t n, std::chrono::milliseconds timeout);
    bool waitForActiveCount(size_t n, std::chrono::milliseconds timeout);
    bool waitForTotalRequests(size_t n, std::chrono::milliseconds timeout);
    bool waitForNewConnectionAfter(size_t old_accept_count, std::chrono::milliseconds timeout);
    bool waitForActiveConnections(size_t n, std::chrono::milliseconds timeout);

    void closeConnection(size_t conn_id);
    void closeOneConnection();
private:
    struct ConnState
    {
        int fd = -1;
        size_t request_count = 0;
        bool closed = false;
    };

    void acceptLoop();
    void connectLoop(size_t conn_id, int conn_fd);

private:
    uint16_t port_;
    ResponseBuilder response_builder_;

    std::atomic<bool> running_{false};

    int listen_fd_ = -1;
    std::thread accept_thread_;

    mutable std::mutex mutex_;
    std::condition_variable cv_;

    size_t next_conn_id_ = 0;
    size_t accept_count_ = 0;
    size_t active_count_ = 0;
    size_t total_request_count_ = 0;
    size_t bad_frame_count_ = 0;

    std::map<size_t, ConnState> conns_;
    std::vector<std::thread> connection_threads_;
};