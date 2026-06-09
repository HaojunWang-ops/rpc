#include "rpc_channel.h"

#include "common.h"
#include "rpc_header.pb.h"
#include "rpc_controller.h"
#include "Logging.h"

#include <arpa/inet.h>
#include <google/protobuf/descriptor.h>
#include <string>
#include <iostream>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <errno.h>
#include <stdio.h>
#include <exception>
#include <signal.h>

namespace
{
    class RpcSignalInitializer
    {
    public:
        RpcSignalInitializer()
        {
            struct sigaction sa;
            sa.sa_handler = SIG_IGN;
            sigemptyset(&sa.sa_mask);
            sa.sa_flags = 0;
            ::sigaction(SIGPIPE, &sa, nullptr);
        }
    };
    static RpcSignalInitializer s_initializer;
}

RpcChannel::RpcChannel(const std::string ip, uint16_t port)
    : ip_(ip), port_(port)
{
    connected_ = false;
    running_ = false;
}

RpcChannel::~RpcChannel()
{
    running_.store(false, std::memory_order_release);

    closeSocket();
    handleConnectionLost("~RpcChannel()");

    if (reader_thread_.joinable())
    {
        reader_thread_.join();
    }
}

bool RpcChannel::start()
{
    if (running_.load(std::memory_order_acquire))
    {
        setLastError("channel already started");
        return false;
    }

    if (!connect())
    {
        return false;
    }

    running_.store(true, std::memory_order_release);
    reader_thread_ = std::thread(&RpcChannel::readerInLoop, this);
    return true;
}

bool RpcChannel::connect()
{
    sockfd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd_ < 0)
    {
        setLastError("create socket failed");
        closeSocket();
        connected_ = false;
        return false;
    }

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = ::htons(port_);
    if (::inet_pton(AF_INET, ip_.c_str(), &(addr.sin_addr)) <= 0)
    {
        setLastError("inet_pton failed");
        closeSocket();
        connected_ = false;
        return false;
    }

    if (::connect(sockfd_, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0)
    {
        setLastError("connect failed");
        closeSocket();
        connected_ = false;
        return false;
    }

    connected_ = true;
    return true;
}

bool RpcChannel::connected()
{
    return connected_.load(std::memory_order_acquire);
}

void RpcChannel::CallMethod(const google::protobuf::MethodDescriptor *method,
                            google::protobuf::RpcController *controller,
                            const google::protobuf::Message *request,
                            google::protobuf::Message *response,
                            google::protobuf::Closure *done)
{
    if (!running_.load(std::memory_order_acquire) || !connected_.load(std::memory_order_acquire))
    {
        if (controller)
        {
            controller->SetFailed("channel not connected");
        }
        return;
    }

    uint64_t request_id = next_request_id_.fetch_add(1);
    auto call = std::make_shared<PendingCall>();
    call->controller = controller;
    call->response = response;
    call->done = done;

    std::string service_name = method->service()->full_name();
    std::string method_name = method->name();

    std::string args;
    if (!request->SerializeToString(&args))
    {
        if (controller)
        {
            controller->SetFailed("request SerializedToString failed");
        }
        return;
    }

    myrpc::RpcHeader header;
    header.set_request_id(request_id);
    header.set_service_name(service_name);
    header.set_method_name(method_name);
    header.set_args_size(args.size());

    std::string header_str;
    if (!header.SerializeToString(&header_str))
    {
        if (controller)
        {
            controller->SetFailed("header SerializeToString failed");
        }
        return;
    }

    uint32_t header_size = static_cast<uint32_t>(header_str.size());
    uint32_t total_size = 4 + header_size + args.size();

    uint32_t net_total_size = ::htonl(total_size);
    uint32_t net_header_size = ::htonl(header_size);

    std::string send_buf;
    send_buf.append(reinterpret_cast<char *>(&net_total_size), 4);
    send_buf.append(reinterpret_cast<char *>(&net_header_size), 4);
    send_buf.append(header_str);
    send_buf.append(args);

    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        auto [it, inserted] = pending_.emplace(request_id, call);
        if (!inserted)
        {
            controller->SetFailed("duplicate request_id");
            done->Run();
            return;
        }
    }

    bool write_ok = false;
    {
        std::lock_guard<std::mutex> lock(send_mutex_);
        write_ok = WriteN(send_buf.data(), send_buf.size());
    }

    if (!write_ok)
    {
        erasePending(request_id);

        if (controller)
        {
            controller->SetFailed("send rpc request failed");
        }

        handleConnectionLost("send rpc request failed");
        return;
    }

    if (done == nullptr)
    {
        bool ok;
        {
            std::unique_lock<std::mutex> lock(call->mutex);
            ok = call->cv.wait_for(lock,
                                   std::chrono::milliseconds(timeout_ms_),
                                   [&call]()
                                   { return call->finished; });
        }
        if (!ok)
        {
            // 谁能从 pending_ 里 erase 掉这个 request_id，谁拥有这次调用的完成权
            // 防止出现reader 线程刚刚从 pending_ 里拿走 call，但还没来得及设置 finished，同步线程 wait_for 超时
            auto timeout_call = erasePendingAndGet(request_id);

            if (timeout_call)
            {
                if (controller)
                {
                    controller->SetFailed("rpc call timeout");
                }
                return;
            }

            std::unique_lock<std::mutex> lock(call->mutex);
            call->cv.wait(lock, [&call]()
                          { return call->finished; });

            return;
        }
    }
}

void RpcChannel::readerInLoop()
{
    while (true)
    {
        std::string frame;
        uint32_t net_total_size = 0;
        uint32_t net_header_size = 0;
        if (!ReadN(&net_total_size, sizeof(net_total_size)) || !ReadN(&net_header_size, sizeof(net_header_size)))
        {
            handleConnectionLost("ReadN header_size or total_size failed");
            return;
        }

        uint32_t total_size = ::ntohl(net_total_size);
        uint32_t header_size = ::ntohl(net_header_size);

        if (total_size < 4 || header_size > total_size - 4)
        {
            handleConnectionLost("incorrect response frame");
            return;
        }

        std::string header_str(header_size, '\0');
        if (!ReadN(header_str.data(), header_size))
        {
            handleConnectionLost("ReadN header_str failed");
            return;
        }

        myrpc::RpcResponseHeader header;
        if (!header.ParseFromString(header_str))
        {
            handleConnectionLost("header parse from string failed");
            return;
        }

        if (total_size != 4 + header_size + header.response_size())
        {
            handleConnectionLost("incorrect response frame");
            return;
        }

        std::string args(header.response_size(), '\0');
        if (!ReadN(args.data(), header.response_size()))
        {
            handleConnectionLost("ReadN header_str failed");
            return;
        }

        handleResponseFrame(header, args);
    }
}

void RpcChannel::handleConnectionLost(const std::string &reason)
{
    setLastError(reason);

    running_.store(false, std::memory_order_release);
    connected_.store(false, std::memory_order_release);

    closeSocket();

    std::unordered_map<uint64_t, std::shared_ptr<PendingCall>> pending;
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        pending.swap(pending_);
    }
    for (auto &[id, call] : pending)
    {
        finishCall(call, reason);
    }
}

void RpcChannel::handleResponseFrame(myrpc::RpcResponseHeader header, const std::string &body)
{
    uint64_t request_id = header.request_id();

    if (request_id == 0)
    {
        handleConnectionLost("invalid request id = 0");
        return;
    }

    {
        auto call = erasePendingAndGet(request_id);

        if (!call)
        {
            LOG_WARN << "pending call not found, request id = " << request_id;
            return;
        }

        if (header.error_code() != myrpc::RPC_OK)
        {
            if (call->controller)
            {
                call->controller->SetFailed(header.error_text());
            }
        }
        else
        {
            if (!call->response)
            {
                if (call->controller)
                {
                    call->controller->SetFailed("response is null");
                }
            }
            else if (!call->response->ParseFromString(body))
            {
                if (call->controller)
                {
                    call->controller->SetFailed("response parse from string failed");
                }
            }
        }

        {
            std::lock_guard<std::mutex> lock(call->mutex);
            call->finished = true;
        }

        call->cv.notify_one();

        if (call->done)
        {
            call->done->Run();
        }
    }
}
    void RpcChannel::setLastError(const std::string &error)
    {
        std::lock_guard<std::mutex> lock(error_mutex_);
        last_error_ = error;
    }

    std::string RpcChannel::LastError()
    {
        std::lock_guard<std::mutex> lock(error_mutex_);
        return last_error_;
    }

    void RpcChannel::erasePending(uint64_t request_id)
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        pending_.erase(request_id);
    }

    std::shared_ptr<PendingCall> RpcChannel::erasePendingAndGet(uint64_t request_id)
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);

        auto it = pending_.find(request_id);
        if (it == pending_.end())
        {
            return nullptr;
        }
        else
        {
            auto pending = it->second;
            pending_.erase(it);
            return pending;
        }
    }
    void RpcChannel::finishCall(const std::shared_ptr<PendingCall> &call,
                                const std::string &error)
    {
        if (call->controller && !error.empty())
        {
            call->controller->SetFailed(error);
        }

        {
            std::lock_guard<std::mutex> lock(call->mutex);
            call->finished = true;
        }

        call->cv.notify_one();

        if (call->done)
        {
            call->done->Run();
        }
    }

    void RpcChannel::closeSocket()
    {
        std::lock_guard<std::mutex> lock(close_mutex_);

        int fd = sockfd_;
        sockfd_ = -1;

        if (fd >= 0)
        {
            ::shutdown(fd, SHUT_RDWR);
            ::close(fd);
        }
    }

    bool RpcChannel::WriteN(const void *buf, size_t n)
    {
        const char *p = reinterpret_cast<const char *>(buf);
        size_t left = n;
        while (left > 0)
        {
            if (!running_.load(std::memory_order_acquire))
            {
                errno = ECANCELED;
                return false;
            }

            ssize_t nw = ::write(sockfd_, p, left);
            if (nw > 0)
            {
                p += nw;
                left -= nw;
            }
            else if (nw == 0)
            {
                errno = EPIPE;
                return false;
            }
            else
            {
                if (errno == EINTR)
                {
                    continue;
                }
                return false;
            }
        }
        return true;
    }

    bool RpcChannel::ReadN(void *buf, size_t n)
    {
        char *p = reinterpret_cast<char *>(buf);
        size_t left = n;
        while (left > 0)
        {
            if (!running_.load(std::memory_order_acquire))
            {
                errno = ECANCELED;
                return false;
            }

            ssize_t nr = ::read(sockfd_, p, left);
            if (nr > 0)
            {
                p += nr;
                left -= nr;
            }
            else if (nr == 0)
            {
                return false;
            }
            else
            {
                if (errno == EINTR)
                    continue;
                return false;
            }
        }
        return true;
    }