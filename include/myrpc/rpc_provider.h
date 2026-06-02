#pragma once

#include "rpc_controller.h"
#include "rpc_closure.h"

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
    void NotifyService(google::protobuf::Service* service);
    void Run(const std::string& ip, uint16_t port);

private:
    void onConnection(const reactor::net::TcpConnectionPtr& conn);

    void onMessage(const reactor::net::TcpConnectionPtr& conn,
                   reactor::net::Buffer* buffer,
                   reactor::Timestamp receive_time);

    bool SendRpcResponse(const reactor::net::TcpConnectionPtr& conn,
                         google::protobuf::Message* response,
                         SimpleRpcController* controller);                        
    
    void SendRpcError(const reactor::net::TcpConnectionPtr& conn,
                      SimpleRpcController* controller);
private:
    struct ServiceInfo {
        google::protobuf::Service* service;
        std::unordered_map<std::string, const google::protobuf::MethodDescriptor*> method_map;
    };

    std::unordered_map<std::string, ServiceInfo> service_map_;
};