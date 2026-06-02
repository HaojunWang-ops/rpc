#include "rpc_channel.h"

#include "common.h"
#include "rpc_header.pb.h"

#include <arpa/inet.h>
#include <google/protobuf/descriptor.h>
#include <string>
#include <iostream>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <errno.h>
#include <stdio.h>
RpcChannel::RpcChannel(std::string ip, std::uint16_t port)
    : ip_(ip), port_(port)
{
}

void RpcChannel::CallMethod(const google::protobuf::MethodDescriptor *method,
                            google::protobuf::RpcController *controller,
                            const google::protobuf::Message *request,
                            google::protobuf::Message *response,
                            google::protobuf::Closure *done)
{
    int fd = -1;
    {
        std::string service_name = method->service()->full_name();
        std::string method_name = method->name();

        std::string args_str;
        if (!request->SerializeToString(&args_str))
        {
            if (controller)
            {
                controller->SetFailed("request SerializedToString failed");
            }
            return;
        }

        myrpc::RpcHeader header;
        header.set_service_name(service_name);
        header.set_method_name(method_name);
        uint32_t args_size = static_cast<uint32_t>(args_str.size());
        header.set_args_size(args_size);

        std::string header_str;
        if (!header.SerializeToString(&header_str))
        {
            if (controller)
            {
                controller->SetFailed("header SerializedToString failed");
            }
            return;
        }

        uint32_t header_size = static_cast<uint32_t>(header_str.size());
        uint32_t total_size = 4 + header_size + args_size;

        uint32_t net_total_size = ::htonl(total_size);
        uint32_t net_header_size = ::htonl(header_size);

        std::string send_buf;
        send_buf.append(reinterpret_cast<char *>(&net_total_size), 4);
        send_buf.append(reinterpret_cast<char *>(&net_header_size), 4);
        send_buf.append(header_str);
        send_buf.append(args_str);

        fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0)
        {
            if (controller)
            {
                controller->SetFailed("create sokcet fd failed");
            }
            return;
        }

        struct sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = ::htons(port_);

        if (::inet_pton(AF_INET, ip_.c_str(), &addr.sin_addr) <= 0)
        {
            if (controller)
            {
                controller->SetFailed("invalid server ip");
            }
            ::close(fd);
            return;
        }

        if (::connect(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0)
        {
            if (controller)
            {
                controller->SetFailed("connect failed");
            }
            ::close(fd);
            return;
        }

        if (!WriteN(fd, send_buf.data(), send_buf.size()))
        {
            if (controller)
            {
                controller->SetFailed("send rpc request failed");
            }
            ::close(fd);
            return;
        }
    }
    {
        uint32_t net_total_size = 0;
        if (!ReadN(fd, &net_total_size, sizeof(net_total_size)))
        {
            if (controller)
            {
                controller->SetFailed("read response total size failed");
            }
            ::close(fd);
            return;
        }

        uint32_t total_size = ::ntohl(net_total_size);
        if (total_size < 4)
        {
            if (controller)
            {
                controller->SetFailed("incorrect response total size");
            }
            ::close(fd);
            return;
        }

        uint32_t net_header_size = 0;
        if (!ReadN(fd, &net_header_size, sizeof(net_header_size)))
        {
            if (controller)
            {
                controller->SetFailed("read response header size failed");
            }
            ::close(fd);
            return;
        }

        uint32_t header_size = ::ntohl(net_header_size);
        uint32_t body_size = total_size - 4;
        if (header_size > body_size)
        {
            if (controller)
            {
                controller->SetFailed("incorrent response header size");
            }
            ::close(fd);
            return;           
        }

        std::string response_header_str(header_size, '\0');
        if (!ReadN(fd, response_header_str.data(), header_size))
        {
            if (controller)
            {
                controller->SetFailed("read response header failed");
            }
            ::close(fd);
            return;
        }

        myrpc::RpcResponseHeader response_header;
        if (!response_header.ParseFromString(response_header_str))
        {
            if (controller)
            {
                controller->SetFailed("parse response header failed");
            }
            ::close(fd);
            return;
        }

        if (response_header.error_code() != 0)
        {
            if (controller)
            {
                controller->SetFailed(response_header.error_text());
            }
            ::close(fd);
            return;
        }

        uint32_t response_size = response_header.response_size();
        uint32_t real_size = body_size - header_size;
        if (real_size != response_size)
        {
            if (controller)
            {
                controller->SetFailed("incorrect response size");
            }
            ::close(fd);
            return;
        }
        std::string response_str(response_size, '\0');

        if (!ReadN(fd, response_str.data(), response_size))
        {
            if (controller)
            {
                controller->SetFailed("read response failed");
            }
            ::close(fd);
            return;
        }

        if (!response->ParseFromString(response_str))
        {
            if (controller)
            {
                controller->SetFailed("parse to response failed");
            }
            ::close(fd);
            return;
        }

        if (done)
        {
            done->Run();
        }
    }
    ::close(fd);
}