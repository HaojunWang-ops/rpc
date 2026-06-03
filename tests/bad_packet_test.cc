#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>
#include <iostream>
#include <string>

bool WriteN(int fd, const void* data, size_t n)
{
    const char* p = static_cast<const char*>(data);
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

    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0)
    {
        perror("connect");
        ::close(fd);
        return -1;
    }

    return fd;
}

void SendTotalSizeLessThan4()
{
    int fd = Connect();
    if (fd < 0) return;

    uint32_t total_size = ::htonl(3);
    WriteN(fd, &total_size, sizeof(total_size));

    std::cout << "sent bad packet: total_size < 4\n";
    ::close(fd);
}

void SendHeaderSizeGreaterThanBody()
{
    int fd = Connect();
    if (fd < 0) return;

    uint32_t total_size = ::htonl(4);      // body_size = total_size - 4 = 0
    uint32_t header_size = ::htonl(100);   // header_size > body_size

    WriteN(fd, &total_size, sizeof(total_size));
    WriteN(fd, &header_size, sizeof(header_size));

    std::cout << "sent bad packet: header_size > body_size\n";
    ::close(fd);
}

void SendIncompletePacket()
{
    int fd = Connect();
    if (fd < 0) return;

    uint32_t total_size = ::htonl(100);
    WriteN(fd, &total_size, sizeof(total_size));

    std::cout << "sent incomplete packet\n";
    ::close(fd);
}

int main()
{
    SendTotalSizeLessThan4();
    SendHeaderSizeGreaterThanBody();
    SendIncompletePacket();
    return 0;
}