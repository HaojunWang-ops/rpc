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

class RpcChannel : public google::protobuf::RpcChannel
{
public:
    RpcChannel(const std::string ip, uint16_t port);
    ~RpcChannel();

    bool start();

    void CallMethod(const google::protobuf::MethodDescriptor* method,
                    google::protobuf::RpcController* controller,
                    const google::protobuf::Message* request,
                    google::protobuf::Message* response,
                    google::protobuf::Closure* done);
    
private:
    bool ReadN(void* buf, size_t n);
    bool WriteN(const void* buf, size_t n);

    bool connect();
    void readerInLoop();
    void handleResponseFrame(myrpc::RpcResponseHeader header, const std::string& body);
    void failAllPending(const std::string& reason);
    void handleConnectionLost(const std::string& reason);

    void setLastError(const std::string& error);
    std::string LastError();
    bool connected();

    void erasePending(uint64_t request_id);
    std::shared_ptr<PendingCall> erasePendingAndGet(uint64_t request_id);

    void finishEarlyError(google::protobuf::RpcController* controller,
                          google::protobuf::Closure* done,
                          const std::string& error);
    void finishCall(const std::shared_ptr<PendingCall>& call);
    void finishCallWithError(const std::shared_ptr<PendingCall>& call, const std::string& error);

    void closeSocket();
private:
    std::string ip_;
    uint16_t port_;
    
    int sockfd_ = -1;
    std::mutex close_mutex_;

    std::atomic<uint64_t> next_request_id_{1};

    std::mutex send_mutex_;

    std::mutex pending_mutex_;
    std::unordered_map<uint64_t, std::shared_ptr<PendingCall> > pending_;

    std::thread reader_thread_;
    std::atomic<bool> running_{false};

    std::atomic<bool> connected_{false};

    //controller中error_text_表示这次rpc的错误原因
    //last_error_表示channel层最近的错误原因
     std::string last_error_;
     std::mutex error_mutex_;

     const int timeout_ms_ = 1000;
};