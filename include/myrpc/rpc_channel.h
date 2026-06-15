#pragma once

#include "rpc_header.pb.h"

#include <google/protobuf/service.h>
#include <google/protobuf/message.h>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <memory>

struct PendingCall
{
    google::protobuf::RpcController* controller = nullptr;
    google::protobuf::Message* response = nullptr;
    google::protobuf::Closure* done = nullptr;

    std::mutex mutex;
    std::condition_variable cv;
    bool finished = false;
};

class MyRpcChannel : public google::protobuf::RpcChannel
{
public:
    enum class State{
        kStopped,
        kRunning,
        kStopping
    };

    MyRpcChannel(const std::string ip, uint16_t port);
    ~MyRpcChannel();

    bool start();
    void stop();
    bool reconnect();
    bool isAvailable();

    void CallMethod(const google::protobuf::MethodDescriptor* method,
                    google::protobuf::RpcController* controller,
                    const google::protobuf::Message* request,
                    google::protobuf::Message* response,
                    google::protobuf::Closure* done);
    
private:
    bool ReadN(void* buf, size_t n);
    bool WriteN(const void* buf, size_t n);

    void readerInLoop();

    void handleResponseFrame(myrpc::RpcResponseHeader header, const std::string& body);
    void handleConnectionLost(const std::string& reason);

    void setLastError(const std::string& error);
    std::string LastError();

    bool connect();

    void erasePending(uint64_t request_id);
    std::shared_ptr<PendingCall> erasePendingAndGet(uint64_t request_id);

    void finishEarlyError(google::protobuf::RpcController* controller,
                          google::protobuf::Closure* done,
                          const std::string& error);
    void finishCall(const std::shared_ptr<PendingCall>& call);
    void finishCallWithError(const std::shared_ptr<PendingCall>& call, const std::string& error);

    void closeSocket();

    void joinReaderIfNeeded();
    bool isReaderThread() const;
    void markStopped();
private:
    std::string ip_;
    uint16_t port_;
    
    int sockfd_ = -1;
    std::mutex close_mutex_;

    std::atomic<uint64_t> next_request_id_{1};

    std::mutex send_mutex_;

    std::mutex pending_mutex_;
    std::unordered_map<uint64_t, std::shared_ptr<PendingCall> > pending_;
    bool accepting_call_{false};//能否发起新的rpc请求,由pending_mutex_保护

    std::mutex lifecycle_mutex_;
    State state_{State::kStopped};//状态机，start/stop中，由lifecycle_mutex_保护

    std::atomic<bool> running_{false};//通知readerInLoop退出

    //controller中error_text_表示这次rpc的错误原因
    //last_error_表示channel层最近的错误原因
     std::string last_error_;
     std::mutex error_mutex_;

     const int timeout_ms_ = 1000;
     
     std::thread reader_thread_;
};

/*
1. MyRpcChannel 不允许在 reader 线程析构。
2. MyRpcChannelPool 是 channel 的生命周期 owner。
3. reader 线程只做 handleConnectionLost，不负责销毁对象。
4. reconnect 由 pool 触发。
5. pool 析构前必须 stop 所有 channel。
*/