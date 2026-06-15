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

MyRpcChannel::MyRpcChannel(const std::string ip, uint16_t port)
    : ip_(ip), port_(port)
{
    running_ = false;
    sockfd_ = -1;
    state_ = State::kStopped;
    accepting_call_ = false;
}

MyRpcChannel::~MyRpcChannel()
{
    if (reader_thread_.joinable() && 
        reader_thread_.get_id() == std::this_thread::get_id())
    {
        LOG_ERROR << "~MyRpcChannel called in reader thread, this is unsafe";
        std::terminate();
    }

    stop();
}

bool MyRpcChannel::isReaderThread() const
{
    return reader_thread_.joinable() && reader_thread_.get_id() == std::this_thread::get_id();
}

void MyRpcChannel::joinReaderIfNeeded()
{
    std::thread reader_to_join;

    {
        std::lock_guard<std::mutex> lock(lifecycle_mutex_);

        if (reader_thread_.joinable() && reader_thread_.get_id() != std::this_thread::get_id())
        {
            reader_to_join = std::move(reader_thread_);
        }
    }

    if (reader_to_join.joinable())
    {
        reader_to_join.join();
    }
}
void MyRpcChannel::stop()
{
    std::unordered_map<uint64_t, std::shared_ptr<PendingCall>> pending;
    {
        std::lock_guard<std::mutex> lock(lifecycle_mutex_);

        if (state_ == State::kStopped)
        {
        }
        else
        {
            state_ = State::kStopping;
        }
    }

    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        accepting_call_ = false;
        pending.swap(pending_);
    }

    running_.store(false);
    closeSocket();

    for (auto &[id, call] : pending)
    {
        finishCallWithError(call, "RpcChannel stopped");
    }

    if (!isReaderThread())
    {
        joinReaderIfNeeded();
    }

    {
        std::lock_guard<std::mutex> lock(lifecycle_mutex_);
        state_ = State::kStopped;
    }
}

bool MyRpcChannel::start()
{
    joinReaderIfNeeded();

    std::lock_guard<std::mutex> lock(lifecycle_mutex_);

    if (state_ == State::kRunning)
    {
        return true;
    }

    if (state_ == State::kStopping)
    {
        return false;
    }

    if (!connect())
    {
        state_ = State::kStopped;
        return false;
    }

    running_.store(true);
    reader_thread_ = std::thread(&MyRpcChannel::readerInLoop, this);

    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        accepting_call_ = true;
        pending_.clear();
    }

    state_ = State::kRunning;

    return true;
}

bool MyRpcChannel::reconnect()
{
    stop();
    return start();
}

bool MyRpcChannel::isAvailable()
{
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    return state_ == State::kRunning;
}

bool MyRpcChannel::connect()
{
    sockfd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd_ < 0)
    {
        setLastError("create socket failed");
        closeSocket();
        return false;
    }

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = ::htons(port_);
    if (::inet_pton(AF_INET, ip_.c_str(), &(addr.sin_addr)) <= 0)
    {
        setLastError("inet_pton failed");
        closeSocket();
        return false;
    }

    if (::connect(sockfd_, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0)
    {
        setLastError("connect failed");
        closeSocket();
        return false;
    }

    return true;
}

void MyRpcChannel::CallMethod(const google::protobuf::MethodDescriptor *method,
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
        finishEarlyError(controller, done, "request SerializedToString failed");
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
        finishEarlyError(controller, done, "header SerializeToString failed");
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

    // 一定不能在锁里面执行回调
    bool reject = false;
    bool duplicate = false;
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);

        if (!accepting_call_)
        {
            reject = true;
        }
        else
        {
            auto [it, inserted] = pending_.emplace(request_id, call);
            if (!inserted)
            {
                duplicate = true;
            }
        }
    }
    if (reject)
    {
        finishEarlyError(controller, done, "RpcChannel is not accepting calls");
        return;
    }
    if (duplicate)
    {
        finishEarlyError(controller, done, "duplicate request id");
        return;
    }

    bool write_ok = false;
    {
        std::lock_guard<std::mutex> lock(send_mutex_);
        write_ok = WriteN(send_buf.data(), send_buf.size());
    }

    if (!write_ok)
    {
        auto failed_call = erasePendingAndGet(request_id);
        if (failed_call)
        {
            finishCallWithError(failed_call, "send rpc request failed");
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

void MyRpcChannel::readerInLoop()
{
    while (running_.load(std::memory_order_acquire))
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

void MyRpcChannel::handleConnectionLost(const std::string &reason)
{
    {
        std::lock_guard<std::mutex> lock(lifecycle_mutex_);
        if (state_ == State::kRunning)
        {
            state_ = State::kStopping;
        }
        else if (state_ == State::kStopped || state_ == State::kStopping)
        {
            return;
        }
    }

    setLastError(reason);

    running_.store(false, std::memory_order_release);

    closeSocket();

    std::unordered_map<uint64_t, std::shared_ptr<PendingCall>> pending;
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        accepting_call_ = false;
        pending.swap(pending_);
    }
    for (auto &[id, call] : pending)
    {
        finishCallWithError(call, reason);
    }

    if (isReaderThread())
    {
        return;
    }

    joinReaderIfNeeded();

    {
        std::lock_guard<std::mutex> lock(lifecycle_mutex_);
        state_ = State::kStopped;
    }
}

void MyRpcChannel::handleResponseFrame(myrpc::RpcResponseHeader header, const std::string &body)
{
    uint64_t request_id = header.request_id();

    if (request_id == 0)
    {
        handleConnectionLost("invalid request id = 0");
        return;
    }

    auto call = erasePendingAndGet(request_id);

    if (!call)
    {
        LOG_WARN << "pending call not found, request id = " << request_id;
        return;
    }

    if (header.error_code() != myrpc::RPC_OK)
    {

        finishCallWithError(call, header.error_text());
        return;
    }
    if (!call->response)
    {
        finishCallWithError(call, "response is null");
        return;
    }
    if (!call->response->ParseFromString(body))
    {
        finishCallWithError(call, "response parse from string failed");
        return;
    }

    finishCall(call);
}
void MyRpcChannel::setLastError(const std::string &error)
{
    std::lock_guard<std::mutex> lock(error_mutex_);
    last_error_ = error;
}

std::string MyRpcChannel::LastError()
{
    std::lock_guard<std::mutex> lock(error_mutex_);
    return last_error_;
}

void MyRpcChannel::erasePending(uint64_t request_id)
{
    std::lock_guard<std::mutex> lock(pending_mutex_);
    pending_.erase(request_id);
}

std::shared_ptr<PendingCall> MyRpcChannel::erasePendingAndGet(uint64_t request_id)
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

void MyRpcChannel::finishEarlyError(google::protobuf::RpcController *controller,
                                    google::protobuf::Closure *done,
                                    const std::string &error)
{
    if (controller)
    {
        controller->SetFailed(error);
    }
    if (done)
    {
        done->Run();
    }
}
void MyRpcChannel::finishCall(const std::shared_ptr<PendingCall> &call)
{
    if (!call)
    {
        return;
    }

    auto *done = call->done;
    call->done = nullptr;
    {
        std::lock_guard<std::mutex> lock(call->mutex);
        call->finished = true;
    }
    call->cv.notify_one();
    if (done)
    {
        done->Run();
    }
}

void MyRpcChannel::finishCallWithError(const std::shared_ptr<PendingCall> &call,
                                       const std::string &error)
{
    if (!call)
    {
        return;
    }

    if (call->controller && !error.empty())
    {
        call->controller->SetFailed(error);
    }

    finishCall(call);
}

void MyRpcChannel::closeSocket()
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

bool MyRpcChannel::WriteN(const void *buf, size_t n)
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

bool MyRpcChannel::ReadN(void *buf, size_t n)
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