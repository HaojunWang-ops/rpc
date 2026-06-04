#pragma once

#include "rpc_controller.h"
#include "rpc_closure.h"
#include "ThreadPool.h"

#include "net/TcpConnection.h"
#include "net/TcpServer.h"
#include "net/EventLoop.h"
#include "net/Buffer.h"

#include <google/protobuf/service.h>
#include <google/protobuf/descriptor.h>

#include <unordered_map>
#include <string>

class RpcProvider
{
public:
    RpcProvider(size_t threadNum);

    void NotifyService(google::protobuf::Service* service);
    void Run(const std::string& ip, uint16_t port);

private:
    void onConnection(const reactor::net::TcpConnectionPtr& conn);

    void onMessage(const reactor::net::TcpConnectionPtr& conn,
                   reactor::net::Buffer* buffer,
                   reactor::Timestamp receive_time);

    bool SendRpcFrame(const reactor::net::TcpConnectionPtr& conn,
                      myrpc::RpcErrorCode error_code,
                      const std::string& error_text,
                      const std::string& response_body);
    
    bool SendRpcResponse(const reactor::net::TcpConnectionPtr& conn,
                         std::shared_ptr<google::protobuf::Message> response,
                         std::shared_ptr<SimpleRpcController> controller);                        
    
    bool SendRpcError(const reactor::net::TcpConnectionPtr& conn,
                      std::shared_ptr<SimpleRpcController> controller);
    
    void doRpcTask(const reactor::net::TcpConnectionPtr& conn,
                   std::string request_frame);
    
    void serThreadNum(int num){
        threadNum_ = num;
    }

private:
    static constexpr uint32_t kMaxRpcFrameSize = 64 * 1024 *1024;

    struct ServiceInfo {
        google::protobuf::Service* service;
        std::unordered_map<std::string, const google::protobuf::MethodDescriptor*> method_map;
    };

    std::unordered_map<std::string, ServiceInfo> service_map_;

    size_t threadNum_;
    ThreadPool business_thread_pool_;
};