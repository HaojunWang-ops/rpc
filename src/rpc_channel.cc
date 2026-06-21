#include "rpc_channel.h"

#include "common.h"
#include "rpc_header.pb.h"
#include "rpc_controller.h"
#include "Logging.h"
#include "CountDownLatch.h"
#include <rpc_codec.h>

#include <google/protobuf/descriptor.h>
#include <string>
#include <iostream>
#include <errno.h>
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

MyRpcChannel::MyRpcChannel(const std::string ip, uint16_t port, CallbackExecutor* callback_executor)
    : ip_(ip), port_(port),
      callback_executor_(callback_executor)
{
    running_.store(false, std::memory_order_release);
    state_ = State::kStopped;
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
    transport_.shutdown();
}

void MyRpcChannel::stop()
{
    std::shared_ptr<MyRpcChannel> self = shared_from_this();

    auto pending = markPendingFailed("stop() is called");

    cleanupStoppedConnection();

    {
        std::lock_guard<std::mutex> lock(lifecycle_mutex_);
        state_ = State::kStopped;
    }

    for (auto &[id, call] : pending)
    {
        finishCallWithError(call, "stop() is called");
    }
}

void MyRpcChannel::stopInternal()
{
    auto pending = markPendingFailed("stopInternal() is called");

    cleanupStoppedConnection();

    {
        std::lock_guard<std::mutex> lock(lifecycle_mutex_);
        state_ = State::kStopped;
    }

    for (auto &[id, call] : pending)
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

    auto self = shared_from_this();

    auto start_latch = std::make_shared<reactor::CountDownLatch>(1);

    std::thread new_reader([self, start_latch]()
                           {
        start_latch->wait();
        self->readerInLoop(); });

    {
        std::lock_guard<std::mutex> lock(reader_mutex_);
        reader_thread_ = std::move(new_reader);
        reader_thread_id_ = reader_thread_.get_id();
    }

    pending_.resetForStart();

    state_ = State::kRunning;

    start_latch->countDown();
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
    std::string error;

    if (!transport_.connectTo(ip_, port_, &error))
    {
        setLastError(error);
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
    auto self = shared_from_this();

    uint64_t request_id = next_request_id_.fetch_add(1);
    auto call = std::make_shared<PendingCall>();
    call->controller = controller;
    call->response = response;
    call->done = done;

    std::string send_buf;
    std::string error;

    if (!RpcCodec::encodeRequestFrame(request_id, method, request, &send_buf, &error))
    {
        finishEarlyError(controller, done, error);
        return;
    }

    
    auto add_result = pending_.add(request_id, call);

    if (add_result == PendingCallManager::AddResult::kNotAccepting)
    {
        finishEarlyError(controller, done, "RpcChannel is not accepting calls");
        return;
    }

    if (add_result == PendingCallManager::AddResult::kDuplicate)
    {
        finishEarlyError(controller, done, "duplicate request id");
        return;
    }

    bool write_ok = false;
    {
        std::lock_guard<std::mutex> lock(send_mutex_);
        write_ok = WriteN(send_buf.data(), send_buf.size());
    }

    // 一定不能在锁里面执行回调
    if (!write_ok)
    {
        auto pending = markPendingFailed("send rpc error");

        for (auto &[id, call] : pending)
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
            auto timeout_call = pending_.take(request_id);

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

        RpcCodec::FrameMeta meta;
        std::string error;

        if (!RpcCodec::decodeFrameMeta(net_total_size, net_header_size, &meta, &error))
        {
            failFromReaderThread(error);
            return;
        }

        std::string header_str(meta.header_size, '\0');
        if (!ReadN(header_str.data(), meta.header_size))
        {
            failFromReaderThread("ReadN header_str failed");
            return;
        }

        myrpc::RpcResponseHeader header;
        if (!RpcCodec::decodeResponseHeader(header_str, meta, &header, &error))
        {
            failFromReaderThread(error);
            return;
        }

        std::string body(meta.body_size, '\0');
        if (!ReadN(body.data(), meta.body_size))
        {
            failFromReaderThread("ReadN response body failed");
            return;
        }

        handleResponseFrame(header, body);
    }
}

void MyRpcChannel::handleResponseFrame(const myrpc::RpcResponseHeader header, const std::string &body)
{
    uint64_t request_id = header.request_id();

    if (request_id == 0)
    {
        failFromReaderThread("invalid request id = 0");
        return;
    }

    auto call = pending_.take(request_id);

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
    std::string error;
    if (!RpcCodec::decodeResponseBody(body, call->response, &error))
    {
        finishCallWithError(call, error);
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
       if (callback_executor_)
       {
        bool ok = callback_executor_->post([done](){
            done->Run();
        });

        if (!ok)
        {
            LOG_ERROR << "finishEarlyError: post callback failed";
        }
       } 
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
       if (callback_executor_)
       {
        bool ok = callback_executor_->post([done](){
            done->Run();
        });

        if (!ok)
        {
            LOG_ERROR << "finishCall: post callback failed";
        }
       } 
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
    transport_.close();
}

bool MyRpcChannel::WriteN(const void *buf, size_t n)
{
    return transport_.writeN(buf, n, running_);
}

bool MyRpcChannel::ReadN(void *buf, size_t n)
{
    return transport_.readN(buf, n, running_);
}

std::unordered_map<uint64_t, std::shared_ptr<PendingCall>> MyRpcChannel::markPendingFailed(const std::string &reason)
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

    return pending_.failAllAndStopAccepting();
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

void MyRpcChannel::failFromReaderThread(const std::string &reason)
{
    auto self = shared_from_this();

    auto pending = markPendingFailed(reason);

    detachReaderHandleIfCurrentThread();

    for (auto &[id, call] : pending)
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