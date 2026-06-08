#include "rpc_channel.h"

#include "common.h"
#include "rpc_header.pb.h"
#include "rpc_controller.h"

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

RpcChannel::RpcChannel(const std::string ip, uint16_t port)
    : ip_(ip), port_(port)
{
    connected_ = false;
    running_ = false;
}

RpcChannel::~RpcChannel()
{
    running_.store(false, std::memory_order_release);

    int fd = sockfd_;
    sockfd_ = -1;

    if (fd >= 0)
    {
        ::shutdown(fd, SHUT_WR);
        ::close(fd);
    }

    if (reader_thread_.joinable())
    {
        reader_thread_.join();
    }
}
bool RpcChannel::start()
{
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
        last_error_ = "create socket failed_";
        sockfd_ = -1;
        connected_ = false;
        return false;
    }

    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = ::htons(port_);
    if (::inet_pton(AF_INET, ip_.c_str(), &(addr.sin_addr)) <= 0)
    {
        last_error_ = "inet_pton failed";
        ::close(sockfd_);
        sockfd_ = -1;
        connected_ = false;
        return false;
    }

    if (::connect(sockfd_, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0)
    {
        last_error_ = "connect failed";
        ::close(sockfd_);
        sockfd_ = -1;
        connected_ = false;
        return false;
    }

    connected_ = true;
    return true;
}

void RpcChannel::CallMethod(const google::protobuf::MethodDescriptor *method,
                            google::protobuf::RpcController *controller,
                            const google::protobuf::Message *request,
                            google::protobuf::Message *response,
                            google::protobuf::Closure *done)
{
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
        pending_[request_id] = call;
    }

    {
        std::unique_lock<std::mutex> lock(send_mutex_);
        if (!WriteN(send_buf.data(), send_buf.size()))
        {
            if (controller)
            {
                controller->SetFailed("send rpc request failed");
            }
            pending_.erase(request_id);
            handleConnectionLost();
            return;
        }
    }

    if (done == nullptr)
    {
        {
            std::unique_lock<std::mutex> lock(call->mutex);
            call->cv.wait(lock, [&call]()
                          { return call->finished; });
        }
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

void RpcChannel::readerInLoop()
{
    while (true)
    {
        std::string frame;
        uint32_t net_total_size = 0;
        uint32_t net_header_size = 0;
        if (!ReadN(&net_total_size, sizeof(net_total_size)) || !ReadN(&net_header_size, sizeof(net_header_size)))
        {
            last_error_ = "ReadN header_size or total_size failed";
            connected_ = false;
            break;
        }

        uint32_t total_size = ::ntohl(net_total_size);
        uint32_t header_size = ::ntohl(net_header_size);

        if (total_size < 4 || header_size > total_size - 4)
        {
            last_error_ = "incorrect response frame";
            connected_ = false;
            break;
        }

        std::string header_str(header_size, '\0');
        if (!ReadN(header_str.data(), header_size))
        {
            last_error_ = "ReadN header_str failed";
            connected_ = false;
            break;
        }

        myrpc::RpcResponseHeader header;
        if (!header.ParseFromString(header_str))
        {
            last_error_ = "header parse from string failed";
            connected_ = false;
            break;
        }

        if (total_size != 4 + header_size + header.response_size())
        {
            last_error_ = "incorrect response frame";
            connected_ = false;
            break;
        }

        std::string args(header.response_size(), '\0');
        if (!ReadN(args.data(), header.response_size()))
        {
            last_error_ = "ReadN header_str failed";
            connected_ = false;
            break;
        }

        handleResponseFrame(header, args);
    }

    handleConnectionLost();
}

void RpcChannel::handleConnectionLost()
{
    running_.store(false, std::memory_order_release);
    if (sockfd_ >= 0)
    {
        ::close(sockfd_);
        sockfd_ = -1;
    }
    connected_ = false;

    std::unordered_map<uint64_t, std::shared_ptr<PendingCall>> pending;
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        pending.swap(pending_);
    }
    for (auto &[id, call] : pending)
    {
        call->controller->SetFailed("connection lost");
        if (call->done)
        {
            call->done->Run();
        }
        else
        {
            std::lock_guard<std::mutex> lock(call->mutex);
            call->finished = true;
            call->cv.notify_all();
        }
    }
    pending_.clear();
}

void RpcChannel::handleResponseFrame(myrpc::RpcResponseHeader header, const std::string &body)
{
    uint64_t request_id = header.request_id();

    if (request_id == 0)
    {
        last_error_ = "invalid response request_id == 0";
        handleConnectionLost();
        return;
    }
    std::shared_ptr<PendingCall> call;
    {
        std::unique_lock<std::mutex> lock(pending_mutex_);

        auto it = pending_.find(request_id);
        if (it == pending_.end())
        {
            return;
        }

        call = it->second;
        pending_.erase(it);
    }

    if (header.error_code() != myrpc::RPC_OK)
    {
        call->controller->SetFailed(header.error_text());
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