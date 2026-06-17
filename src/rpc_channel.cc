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
    running_.store(false, std::memory_order_release);
    sockfd_.store(-1, std::memory_order_release);
    state_ = State::kStopped;
    accepting_call_ = false;
}

MyRpcChannel::~MyRpcChannel()
{
    if (isReaderThread())
    {
        LOG_ERROR << "~MyRpcChannel called in reader thread, this is unsafe";
        std::terminate();
    }

    stopInternal();
}

void MyRpcChannel::setTimeoutMs(int timeout_ms)
{
    timeout_ms_.store(timeout_ms, std::memory_order_release);
}

int MyRpcChannel::timeoutMs() const
{
    return timeout_ms_.load(std::memory_order_acquire);
}

bool MyRpcChannel::isReaderThread() const
{
    std::lock_guard<std::mutex> lock(reader_mutex_);
    return reader_thread_.joinable() && reader_thread_id_ == std::this_thread::get_id();
}

void MyRpcChannel::joinReaderIfNeeded()
{
    std::thread reader_to_join;

    {
        std::lock_guard<std::mutex> lock(reader_mutex_);

        if (reader_thread_.joinable() && reader_thread_id_ != std::this_thread::get_id())
        {
            reader_to_join = std::move(reader_thread_);
            reader_thread_id_ = std::thread::id{};
        }
    }

    if (reader_to_join.joinable())
    {
        reader_to_join.join();
    }
}

void MyRpcChannel::shutdownSocket()
{
    std::lock_guard<std::mutex> lock(fd_mutex_);

    int fd = sockfd_.load(std::memory_order_acquire);

    if (fd >= 0)
    {
        ::shutdown(fd, SHUT_RDWR);
    }
}

void MyRpcChannel::stop()
{
    std::shared_ptr<MyRpcChannel> self = shared_from_this();

    auto pending = markPendingFailed("stop() is called");

    bool need_to_fail = false;
    {
        std::lock_guard<std::mutex> lock(lifecycle_mutex_);
        if (state_ == State::kRunning)
        {
            need_to_fail = true;       
        }
        if (state_ == State::kStopped)
        {
            return;
        }
    }

    if (need_to_fail)
    {
        markPendingFailed("stop() is called");
    }

    cleanupStoppedConnection();

    {
        std::lock_guard<std::mutex> lock(lifecycle_mutex_);
        state_ = State::kStopped;
    }

    for (auto& [id, call] : pending)
    {
        finishCallWithError(call, "stop() is called");
    }
}

void MyRpcChannel::stopInternal()
{   
    auto pending = markPendingFailed("stopInternal() is called");

    bool need_to_fail = false;
    {
        std::lock_guard<std::mutex> lock(lifecycle_mutex_);
        if (state_ == State::kRunning)
        {
            need_to_fail = true;       
        }
        if (state_ == State::kStopped)
        {
            return;
        }
    }

    if (need_to_fail)
    {
        markPendingFailed("stop() is called");
    }

    cleanupStoppedConnection();

    {
        std::lock_guard<std::mutex> lock(lifecycle_mutex_);
        state_ = State::kStopped;
    } 

    for (auto& [id, call] : pending)
    {
        finishCallWithError(call, "stopInternal() is called");
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

    running_.store(true, std::memory_order_release);
    
    {
        std::lock_guard<std::mutex> lock(reader_mutex_);
        auto self = shared_from_this();
        reader_thread_ = std::thread([self](){
            self->readerInLoop();
        });
        reader_thread_id_ = reader_thread_.get_id();
    }

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
    int fd= ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
    {
        setLastError("create socket failed");
        closeSocketAfterIoStopped();
        return false;
    }

    sockfd_.store(fd, std::memory_order_release);

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = ::htons(port_);
    if (::inet_pton(AF_INET, ip_.c_str(), &(addr.sin_addr)) <= 0)
    {
        setLastError("inet_pton failed");
        closeSocketAfterIoStopped();
        return false;
    }

    if (::connect(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0)
    {
        setLastError("connect failed");
        closeSocketAfterIoStopped();
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
        auto self = shared_from_this();

        auto pending = markPendingFailed("send rpc error");

        for (auto [id, call] : pending)
        {
            finishCallWithError(call, "send rpc error");
        }
    }

    if (done == nullptr)
    {
        bool ok;
        {
            std::unique_lock<std::mutex> lock(call->mutex);
            ok = call->cv.wait_for(lock,
                                   std::chrono::milliseconds(timeout_ms_.load(std::memory_order_acquire)),
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
            failFromReaderThread("ReadN header_size or total_size failed");
            return;
        }

        uint32_t total_size = ::ntohl(net_total_size);
        uint32_t header_size = ::ntohl(net_header_size);

        if (total_size < 4 || header_size > total_size - 4)
        {
            failFromReaderThread("incorrect response frame");
            return;
        }

        std::string header_str(header_size, '\0');
        if (!ReadN(header_str.data(), header_size))
        {
            failFromReaderThread("ReadN header_str failed");
            return;
        }

        myrpc::RpcResponseHeader header;
        if (!header.ParseFromString(header_str))
        {
           failFromReaderThread("header parse from string failed");
            return;
        }

        if (total_size != 4 + header_size + header.response_size())
        {
            failFromReaderThread("incorrect response frame");
            return;
        }

        std::string args(header.response_size(), '\0');
        if (!ReadN(args.data(), header.response_size()))
        {
            failFromReaderThread("ReadN header_str failed");
            return;
        }

        handleResponseFrame(header, args);
    }
}

void MyRpcChannel::handleResponseFrame(myrpc::RpcResponseHeader header, const std::string &body)
{
    uint64_t request_id = header.request_id();

    if (request_id == 0)
    {
        markPendingFailed("invalid request id = 0");
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

void MyRpcChannel::closeSocketAfterIoStopped()
{
    std::lock_guard<std::mutex> lock(fd_mutex_);

    int fd = sockfd_.exchange(-1, std::memory_order_acq_rel);

    if (fd >= 0)
    {
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

        int fd = sockfd_.load(std::memory_order_acquire);
        if (fd < 0)
        {
            errno = ECANCELED;
            return false;
        }

        ssize_t nw = ::write(fd, p ,left);
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

        int fd = sockfd_.load(std::memory_order_acquire);
        if (fd < 0)
        {
            errno = ECANCELED;
            return false;
        }
        ssize_t nr = ::read(fd, p, left);
        
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

std::unordered_map<uint64_t, std::shared_ptr<PendingCall> > MyRpcChannel::markPendingFailed(const std::string& reason)
{
    {
        std::lock_guard<std::mutex> lock(lifecycle_mutex_);

        if (state_ == State::kRunning)
        {
            state_ = State::kStopping;
        }
        else
        {
            return {};
        }
    }

    setLastError(reason);

    running_.store(false, std::memory_order_release);

    shutdownSocket();

    std::unordered_map<uint64_t, std::shared_ptr<PendingCall> > pending;
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        pending.swap(pending_);
        accepting_call_ = false;
    }

    return pending;
}

void MyRpcChannel::cleanupStoppedConnection()
{
    if (isReaderThread())
    {
        LOG_ERROR << "cleanupStoppedConnection is called int reader thread";
        return;
    }

    joinReaderIfNeeded();

    {
        std::lock_guard<std::mutex> lock(send_mutex_);
        closeSocketAfterIoStopped();
    }

}

void MyRpcChannel::failFromReaderThread(const std::string& reason)
{
    auto self = shared_from_this();

    auto pending = markPendingFailed(reason);

    detachReaderHandleIfCurrentThread();

    for (auto& [id, call] : pending)
    {
        finishCallWithError(call, reason);
    }
}

void MyRpcChannel::detachReaderHandleIfCurrentThread()
{
    std::lock_guard<std::mutex> lock(reader_mutex_);
    
    if (reader_thread_.joinable() && reader_thread_id_ == std::this_thread::get_id())
    {
        reader_thread_.detach();
        reader_thread_id_ = std::thread::id{};
    }
}