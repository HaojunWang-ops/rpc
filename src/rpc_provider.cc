#include "rpc_provider.h"

#include "common.h"
#include "rpc_header.pb.h"

#include <arpa/inet.h>
#include <google/protobuf/message.h>
#include <iostream>
#include <memory>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <iostream>

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

void RpcProvider::onMessage(const reactor::net::TcpConnectionPtr &conn,
                            reactor::net::Buffer *buffer,
                            reactor::Timestamp receive_time)
{
    while (buffer->readableBytes() >= 4)
    {
        const char *data = buffer->peek();

        uint32_t total_size = 0;
        ::memcpy(&total_size, data, sizeof(total_size));
        total_size = ::ntohl(total_size);

        if (buffer->readableBytes() < 4 + total_size)
        {
            return;
        }

        if (total_size < 4)
        {
            conn->shutdown();
            return;
        }

        auto controller = std::make_unique<SimpleRpcController>();

        buffer->retrieve(4);

        uint32_t header_size = 0;
        ::memcpy(&header_size, buffer->peek(), sizeof(header_size));
        header_size = ::ntohl(header_size);
        buffer->retrieve(4);

        uint32_t body_size = total_size - 4;
        if (header_size > body_size)
        {
            conn->shutdown();
            return;
        }

        std::string header_str(header_size, '\0');
        ::memcpy(header_str.data(), buffer->peek(), header_size);
        buffer->retrieve(header_size);

        myrpc::RpcHeader header;
        if (!header.ParseFromString(header_str))
        {
            std::cerr << "parse rpc header failed\n";
            // shutdown() or handleclose()
            conn->shutdown();
            return;
        }

        uint32_t args_size = header.args_size();
        uint32_t real_args_size = body_size - header_size;
        if (args_size != real_args_size)
        {
            conn->shutdown();
            return;
        }

        std::string service_name = header.service_name();
        std::string method_name = header.method_name();

        std::string args_str(args_size, '\0');
        ::memcpy(args_str.data(), buffer->peek(), args_size);
        buffer->retrieve(args_size);

        auto service_it = service_map_.find(service_name);
        if (service_it == service_map_.end())
        {
            std::string error = "service not found " + service_name + '\n';
            std::cerr << error;

            controller->SetFailed(1001, error);
            SendRpcError(conn, controller.get());
            conn->shutdown();
            return;
        }

        auto method_it = service_it->second.method_map.find(method_name);
        if (method_it == service_it->second.method_map.end())
        {
            std::string error = "method not found " + method_name + '\n';
            std::cerr << error;

            controller->SetFailed(1002, error);
            SendRpcError(conn, controller.get());
            conn->shutdown();
            return;
        }

        google::protobuf::Service *service = service_it->second.service;
        const google::protobuf::MethodDescriptor *method_desc = method_it->second;

        google::protobuf::Message *request = service->GetRequestPrototype(method_desc).New();
        if (!request->ParseFromString(args_str))
        {
            std::string error = "parse request failed\n";
            std::cerr << error;

            controller->SetFailed(1003, error);
            SendRpcError(conn, controller.get());

            delete request;
            conn->shutdown();
            return;
        }

        google::protobuf::Message *response = service->GetResponsePrototype(method_desc).New();

        auto raw_controller = controller.release();
        google::protobuf::Closure *done = SendResponseClosure([this, conn, response, raw_controller, request]()
                                                              {
            SendRpcResponse(conn, response, raw_controller);
            delete request;
            delete response;
            delete raw_controller; });

        service->CallMethod(method_desc, raw_controller, request, response, done);
    }
}

bool RpcProvider::SendRpcResponse(const reactor::net::TcpConnectionPtr &conn,
                                  google::protobuf::Message *response,
                                  SimpleRpcController *controller)
{
    std::string response_body;

    if (controller->error_code() == 0 && response != nullptr)
    {
        if (!response->SerializeToString(&response_body))
        {
            conn->shutdown();
            return false;
        }
    }

    myrpc::RpcResponseHeader response_header;
    response_header.set_error_code(controller->error_code());
    response_header.set_error_text(controller->error_text());
    response_header.set_response_size(static_cast<uint32_t>(response_body.size()));

    std::string response_header_str;
    if (!response_header.SerializeToString(&response_header_str))
    {
        return false;
    }

    uint32_t header_size = static_cast<uint32_t>(response_header_str.size());
    uint32_t total_size = sizeof(header_size) + header_size + static_cast<uint32_t>(response_body.size());

    uint32_t net_header_size = ::htonl(header_size);
    uint32_t net_total_size = ::htonl(total_size);

    std::string send_buf;
    send_buf.append(reinterpret_cast<char *>(&net_total_size), sizeof(total_size));
    send_buf.append(reinterpret_cast<char *>(&net_header_size), sizeof(net_header_size));
    send_buf.append(response_header_str);
    send_buf.append(response_body);
    conn->send(send_buf);

    return true;
}

void RpcProvider::SendRpcError(const reactor::net::TcpConnectionPtr &conn,
                               SimpleRpcController *controller)
{
    SendRpcResponse(conn, nullptr, controller);
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

    server.start();
    loop.loop();
}