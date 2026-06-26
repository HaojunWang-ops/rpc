#pragma once

#include "rpc_header.pb.h"
#include "pending_call_manager.h"
#include "rpc_transport.h"
#include "CallbackExecutor.h"
#include "rpc_timeout_manager.h"
#include <rpc_future.h>

#include <google/protobuf/service.h>
#include <google/protobuf/message.h>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <memory>
#include <future>

class MyRpcChannel : public google::protobuf::RpcChannel,
                     public std::enable_shared_from_this<MyRpcChannel>
{
public:
    enum class State
    {
        kStopped,
        kRunning,
        kStopping
    };

    MyRpcChannel(const std::string ip, uint16_t port, CallbackExecutor *callback_executor);
    ~MyRpcChannel();

    bool start();
    void stop();
    bool reconnect();
    bool isAvailable();

    void CallMethod(const google::protobuf::MethodDescriptor *method,
                    google::protobuf::RpcController *controller,
                    const google::protobuf::Message *request,
                    google::protobuf::Message *response,
                    google::protobuf::Closure *done);

    void setTimeoutMs(int timeout_ms);

    int timeoutMs() const;

private:
    bool ReadN(void *buf, size_t n);
    bool WriteN(const void *buf, size_t n);

    void readerInLoop();

    void handleResponseFrame(const myrpc::RpcResponseHeader header, const std::string &body);

    void setLastError(const std::string &error);
    std::string LastError();

    bool connect();

    void runDone(google::protobuf::Closure *done);
    void finishEarlyError(google::protobuf::RpcController *controller,
                          google::protobuf::Closure *done,
                          const std::string &error);
    void finishCall(const std::shared_ptr<PendingCall> &call);
    void finishCallWithError(const std::shared_ptr<PendingCall> &call, const std::string &error);

    void closeSocketAfterIoStopped();

    void joinReaderIfNeeded();
    bool isReaderThread() const;

    void shutdownSocket();

    void cleanupStoppedConnection();

    void stopInternal();

private:
    std::string ip_;
    uint16_t port_;

    RpcTransport transport_;

    std::atomic<uint64_t> next_request_id_{1};

    PendingCallManager pending_;

    std::mutex lifecycle_mutex_;
    State state_{State::kStopped}; // 状态机，start/stop中，由lifecycle_mutex_保护

    std::atomic<bool> running_{false}; // 通知readerInLoop退出

    // controller中error_text_表示这次rpc的错误原因
    // last_error_表示channel层最近的错误原因
    std::string last_error_;
    std::mutex error_mutex_;

    std::mutex send_mutex_;

    // 保护reader_thread_ 的 move/join/get_id
    std::thread reader_thread_;
    mutable std::mutex reader_mutex_;
    std::thread::id reader_thread_id_;

    std::atomic<int> timeout_ms_{3000};

    CallbackExecutor *callback_executor_;

    void onRpcTimeout(uint64_t request_id);

private:
    std::unordered_map<uint64_t, std::shared_ptr<PendingCall>> markPendingFailed(const std::string &reason);

    void failFromReaderThread(const std::string &reason);
    void detachReaderHandleIfCurrentThread();

    RpcTimeoutManager timeout_manager_;

public:
    template <typename Response, typename Request>
    std::future<RpcFutureResult<Response>> CallMethodFuture(
        const google::protobuf::MethodDescriptor *method,
        const Request &request)
    {
        using State = FutureCallState<Request, Response>;

        auto state = std::make_shared<State>();
        state->request = request;

        auto future = state->promise.get_future();

        auto *done = new FutureClosure<Request, Response>(state);

        CallMethod(
            method,
            &state->controller,
            &state->request,
            &state->response,
            done);

        return future;
    }
};

/*
1. MyRpcChannel 不允许在 reader 线程析构。
2. MyRpcChannelPool 是 channel 的生命周期 owner。
3. reader 线程只做 handleConnectionLost，不负责销毁对象。
4. reconnect 由 pool 触发。
5. pool 析构前必须 stop 所有 channel。
*/

/*
保护fd的ABA问题
1. running_ = false，阻止新 I/O
2. shutdown(fd)，唤醒正在阻塞的 read/write
3. join reader，确认没有 reader 正在 read
4. 拿 send_mutex_，确认没有 writer 正在 write
5. close(fd)，释放 fd
所以read/write是不可能和close并发的
*/