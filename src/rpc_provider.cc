#include "rpc_provider.h"

#include "common.h"
#include "rpc_header.pb.h"

#include <arpa/inet.h>
#include <google/protobuf/message.h>
#include <memory>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <iostream>

RpcProvider::RpcProvider(size_t threadNum)
    :threadNum_(threadNum),
     business_thread_pool_(ThreadPool(threadNum_))
{
}

void RpcProvider::NotifyService(google::protobuf::Service *service)
{
    const google::protobuf::ServiceDescriptor *service_desc = service->GetDescriptor();

    struct ServiceInfo serviceinfo;
    serviceinfo.service = service;

    for (int i = 0; i < service_desc->method_count(); i++)
    {
        const google::protobuf::MethodDescriptor *method_desc = service_desc->method(i);
        std::string method_name = method_desc->name();
        serviceinfo.method_map.emplace(method_name, method_desc);
    }

    std::string service_name = service_desc->full_name();
    service_map_.emplace(service_name, serviceinfo);
}

void RpcProvider::onConnection(const reactor::net::TcpConnectionPtr &conn)
{
    std::cout << "peer - " << conn->peerAddress().toIpPort() << " -> "
              << conn->localAddress().toIpPort() << " is "
              << (conn->connected() ? "UP" : "DOWN") << std::endl;
}

void RpcProvider::doRpcTask(const reactor::net::TcpConnectionPtr& conn,
                            std::string request_frame)
{
        const char* data = request_frame.data();
        size_t total_size = request_frame.size();

        std::shared_ptr<SimpleRpcController> controller(new SimpleRpcController);

        uint32_t header_size = 0;
        ::memcpy(&header_size, data, sizeof(header_size));
        header_size = ::ntohl(header_size);
        
        if (total_size < 4 + header_size)
        {
            controller->SetFailed(myrpc::RPC_BAD_REQUEST, "total_size < 4 + header_size");
            SendRpcError(conn, 0, controller);
            conn->shutdown();
            return;
        }

        std::string header_str(data + 4, header_size);

        myrpc::RpcHeader header;
        if (!header.ParseFromString(header_str))
        {
            controller->SetFailed(myrpc::RPC_PARSE_HEADER_FAILED, "parse rpc header failed");
            SendRpcError(conn, 0, controller);
            conn->shutdown();
            return;
        }

        std::string service_name = header.service_name();
        std::string method_name = header.method_name();
        uint64_t request_id = header.request_id();
        uint32_t args_size = header.args_size();

        if (request_id == 0)
        {
            controller->SetFailed(myrpc::RPC_BAD_REQUEST, "request_id == 0");
            SendRpcError(conn, 0, controller);
            conn->shutdown();
            return;
        }
        if (total_size != 4 + header_size + args_size)
        {
            controller->SetFailed(myrpc::RPC_BAD_REQUEST, "total_size != 4 + header_size + args_size");
            SendRpcError(conn, request_id, controller);
            conn->shutdown();
            return;
        }

        std::string args_str(data + 4 + header_size, args_size);

        auto service_it = service_map_.find(service_name);
        if (service_it == service_map_.end()){
            controller->SetFailed(myrpc::RPC_SERVICE_NOT_FOUND, "service not found");
            SendRpcError(conn, request_id, controller);
            return;
        }

        auto method_it = service_it->second.method_map.find(method_name);
        if (method_it == service_it->second.method_map.end()){
            controller->SetFailed(myrpc::RPC_METHOD_NOT_FOUND, "method not found");
            SendRpcError(conn, request_id, controller);
            return;
        }

        google::protobuf::Service* service = service_it->second.service;
        const google::protobuf::MethodDescriptor* method = method_it->second;

        std::shared_ptr<google::protobuf::Message> request(service->GetRequestPrototype(method).New());
        if(!request->ParseFromString(args_str))
        {
            controller->SetFailed(myrpc::RPC_PARSE_REQUEST_FAILED, "parse request failed");
            SendRpcError(conn, request_id, controller);
            conn->shutdown();
            return;
        }

        std::shared_ptr<google::protobuf::Message> response(service->GetResponsePrototype(method).New());
        
        google::protobuf::Closure* done = SendResponseClosure([this, conn, controller, request, response, request_id](){
            SendRpcResponse(conn, request_id, response, controller);
        });

        service->CallMethod(method, controller.get(), request.get(), response.get(), done);
}
void RpcProvider::onMessage(const reactor::net::TcpConnectionPtr &conn,
                            reactor::net::Buffer *buffer,
                            reactor::Timestamp receive_time)
{
    if (conn->disconnecting())
    {
        buffer->retrieveAll();
        return;
    }

    while (buffer->readableBytes() >= 4)
    {
        std::shared_ptr<SimpleRpcController> controller(new SimpleRpcController);
        
        const char *data = buffer->peek();

        uint32_t total_size = 0;
        ::memcpy(&total_size, data, sizeof(total_size));
        total_size = ::ntohl(total_size);

        if (total_size < 4)
        {
            
            controller->SetFailed(myrpc::RPC_BAD_REQUEST, "total_size < 4 bytes");
            SendRpcError(conn, 0, controller);
            conn->shutdown();
            buffer->retrieveAll();
            return;
        }

        if (total_size > kMaxRpcFrameSize)
        {
            controller->SetFailed(myrpc::RPC_BAD_REQUEST, "total_size > kMaxRpcFrmaeSize");
            SendRpcError(conn, 0, controller);
            conn->shutdown();
            buffer->retrieveAll();
            return;
        }

        if (buffer->readableBytes() < 4 + total_size)
        {
            return;
        }

        buffer->retrieve(4);

        std::string request_frame(total_size, '\0');
        ::memcpy(request_frame.data(), buffer->peek(), total_size);
        buffer->retrieve(total_size);

        business_thread_pool_.submit([this, conn, request_frame = std::move(request_frame)]() mutable{
            doRpcTask(conn, std::move(request_frame));
            std::cout << "[business thread] id = "
                << std::this_thread::get_id()
                << std::endl;
        });


    }
}

bool RpcProvider::SendRpcFrame(const reactor::net::TcpConnectionPtr& conn,
                               uint64_t request_id,
                               myrpc::RpcErrorCode error_code,
                               const std::string& error_text,
                               const std::string& response_body)
{
    myrpc::RpcResponseHeader response_header;
    response_header.set_request_id(request_id);
    response_header.set_error_code(error_code);
    response_header.set_error_text(error_text);
    response_header.set_response_size(static_cast<uint32_t>(response_body.size()));

    std::string response_header_str;
    if (!response_header.SerializePartialToString(&response_header_str))
    {
        return false;
    }

    uint32_t header_size = static_cast<uint32_t>(response_header_str.size());
    uint32_t total_size = static_cast<uint32_t>(4 + header_size + static_cast<uint32_t>(response_body.size()));

    uint32_t net_header_size = ::htonl(header_size);
    uint32_t net_total_size = ::htonl(total_size);

    std::string send_buf;
    send_buf.append(reinterpret_cast<const char*>(&net_total_size), sizeof(net_total_size));
    send_buf.append(reinterpret_cast<const char*>(&net_header_size), sizeof(net_header_size));
    send_buf.append(response_header_str);
    send_buf.append(response_body);

    conn->send(std::move(send_buf));
    
    return true;
}
bool RpcProvider::SendRpcResponse(const reactor::net::TcpConnectionPtr &conn,
                                  uint64_t request_id,
                                  std::shared_ptr<google::protobuf::Message> response,
                                  std::shared_ptr<SimpleRpcController> controller)
{
    std::string response_body;

    if (controller->error_code() == myrpc::RPC_OK && response != nullptr)
    {
        if (!response->SerializeToString(&response_body))
        {
            controller->SetFailed(myrpc::RPC_INTERNAL_ERROR, "serialize response failed");
            return SendRpcFrame(conn,
                                request_id,
                                myrpc::RPC_INTERNAL_ERROR,
                                "serialize response failed",
                                "");
        }
    }

    return SendRpcFrame(conn, request_id, controller->error_code(), controller->error_text(), std::move(response_body));
}

bool RpcProvider::SendRpcError(const reactor::net::TcpConnectionPtr &conn,
                               uint64_t request_id,
                               std::shared_ptr<SimpleRpcController> controller)
{
    return SendRpcFrame(conn, request_id, controller->error_code(), controller->error_text(), "");
}

void RpcProvider::Run(const std::string &ip, uint16_t port)
{
    reactor::net::EventLoop loop;
    reactor::net::InetAddress addr(ip, port);
    reactor::net::TcpServer server(&loop, addr, "server");
    // server.setThreadNum(4);
    server.setConnectionCallback(
        [this](const reactor::net::TcpConnectionPtr &conn)
        {
            this->onConnection(conn);
        });

    server.setMessageCallback(
        [this](const reactor::net::TcpConnectionPtr &conn,
               reactor::net::Buffer *buffer,
               reactor::Timestamp receive_time)
        {
            this->onMessage(conn, buffer, receive_time);
        });
    
    business_thread_pool_.start();
    server.start();
    loop.loop();
}