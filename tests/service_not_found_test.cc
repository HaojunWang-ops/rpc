#include "user.pb.h"
#include "rpc_header.pb.h"
#include "common.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cstring>
#include <iostream>
#include <string>

void getResponse(int fd)
{
    // ---------- 调试：开始 getResponse ----------
    std::cerr << "[getResponse] fd=" << fd << " start" << std::endl;

    // --- ReadN 1: 读取 total_size ---
    std::cerr << "[getResponse] ReadN 1: trying to read total_size (4 bytes)" << std::endl;
    uint32_t net_total_size = 0;
    if (!ReadN(fd, &net_total_size, sizeof(net_total_size)))
    {
        std::cerr << "[getResponse] ReadN 1 FAILED: read total_size failed" << std::endl;
        return;
    }
    std::cerr << "[getResponse] ReadN 1 OK: net_total_size=0x" << std::hex << net_total_size << std::dec << std::endl;

    uint32_t total_size = ::ntohl(net_total_size);
    if (total_size < 4)
    {
        std::cerr << "[getResponse] total_size < 4 (" << total_size << ")" << std::endl;
        return;
    }

    // --- ReadN 2: 读取 header_size ---
    std::cerr << "[getResponse] ReadN 2: trying to read header_size (4 bytes)" << std::endl;
    uint32_t net_header_size = 0;
    if (!ReadN(fd, &net_header_size, sizeof(net_header_size)))
    {
        std::cerr << "[getResponse] ReadN 2 FAILED: read header_size failed" << std::endl;
        return;
    }
    std::cerr << "[getResponse] ReadN 2 OK: net_header_size=0x" << std::hex << net_header_size << std::dec << std::endl;

    uint32_t header_size = ::ntohl(net_header_size);
    uint32_t body_size = total_size - 4;
    if (header_size > body_size)
    {
        std::cerr << "[getResponse] header_size > body_size (" << header_size << " > " << body_size << ")" << std::endl;
        return;
    }

    // --- ReadN 3: 读取 header 内容 ---
    std::cerr << "[getResponse] ReadN 3: trying to read header_str (" << header_size << " bytes)" << std::endl;
    std::string response_header_str(header_size, '\0');
    if (!ReadN(fd, response_header_str.data(), header_size))
    {
        std::cerr << "[getResponse] ReadN 3 FAILED: read response header failed" << std::endl;
        return;
    }
    std::cerr << "[getResponse] ReadN 3 OK" << std::endl;

    myrpc::RpcResponseHeader response_header;
    if (!response_header.ParseFromString(response_header_str))
    {
        std::cerr << "[getResponse] parse response header failed" << std::endl;
        return;
    }

    std::cout << "error_code: " << response_header.error_code() << "error_text: " << response_header.error_text() << std::endl;
    if (response_header.error_code() != 0)
    {
        std::cerr << "!!!!!" << std::endl;
        std::cerr << "[getResponse] server error: " << response_header.error_text() << std::endl;
        return;
    }

    uint32_t response_size = response_header.response_size();
    uint32_t real_size = body_size - header_size;
    if (real_size != response_size)
    {
        std::cerr << "[getResponse] size mismatch: real=" << real_size << " header says=" << response_size << std::endl;
        return;
    }

    // --- ReadN 4: 读取 response body ---
    std::cerr << "[getResponse] ReadN 4: trying to read response body (" << response_size << " bytes)" << std::endl;
    std::string response_str(response_size, '\0');
    if (!ReadN(fd, response_str.data(), response_size))
    {
        std::cerr << "[getResponse] ReadN 4 FAILED: read response body failed" << std::endl;
        return;
    }
    std::cerr << "[getResponse] ReadN 4 OK" << std::endl;

    // 注意：下面的代码存在未初始化的指针问题，仅作调试占位
    google::protobuf::Message *response = nullptr; // 原代码未初始化，这里临时设为 nullptr
    if (response == nullptr || !response->ParseFromString(response_str))
    {
        std::cerr << "[getResponse] parse to response failed (response object not ready)" << std::endl;
        return;
    }

    std::cerr << "[getResponse] finished successfully" << std::endl;
    ::close(fd);
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

    getResponse(fd);

    std::cout << "sent service not found" << std::endl;
    ::close(fd);
    return;
}

int main()
{
    sendServiceNotFound();
    return 0;
}