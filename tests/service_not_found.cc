#include "user.pb.h"
#include "rpc_header.pb.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cstring>
#include <iostream>
#include <string>

bool WriteN(int fd, const void *data, size_t n)
{
    const char *p = static_cast<const char *>(data);
    size_t left = n;

    while (left > 0)
    {
        ssize_t nw = ::write(fd, p, left);
        if (nw <= 0)
        {
            return false;
        }
        p += nw;
        left -= nw;
    }

    return true;
}

int Connect()
{
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
    {
        perror("socket");
        return -1;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = ::htons(8000);

    if (::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr) <= 0)
    {
        perror("inet_pton");
        ::close(fd);
        return -1;
    }

    if (::connect(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0)
    {
        perror("connect");
        ::close(fd);
        return -1;
    }

    return fd;
}

void sendServiceNotFound()
{
    int fd = Connect();
    if (fd < 0)
        return;

    myrpc::RpcHeader header;
    header.set_service_name("demo.NotExistService");
    header.set_method_name("Login");
    header.set_args_size(0);

    std::string header_str;
    if (!header.SerializeToString(&header_str))
    {
        std::cout << "header serialize to strin failed";
        std::cout << std::endl;
        return;
    }

    uint32_t header_size = header_str.size();
    uint32_t total_size = 4 + header_size;

    uint32_t net_total_size = ::htonl(total_size);
    uint32_t net_header_size = ::htonl(header_size);

    WriteN(fd, &net_total_size, sizeof(net_total_size));
    WriteN(fd, &net_header_size, sizeof(net_header_size));
    WriteN(fd, header_str.data(), header_size);

    std::cout << "sent service not found" << std::endl;
    ::close(fd);
    return;
}

int main()
{
    sendServiceNotFound();
    return 0;
}