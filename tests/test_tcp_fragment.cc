#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>

#include <cstring>
#include <iostream>
#include <chrono>
#include <thread>

#include <user.pb.h>
#include <rpc_header.pb.h>

std::string BuildLoginFrame(const std::string& name, const std::string& password)
{
    demo::LoginRequest req;
    req.set_name(name);
    req.set_password(password);
    std::string args = req.SerializeAsString();

    myrpc::RpcHeader header;
    header.set_service_name("demo.UserService");
    header.set_method_name("Login");
    header.set_args_size(static_cast<uint32_t>(args.size()));
    std::string header_str = header.SerializeAsString();

    uint32_t header_size = static_cast<uint32_t>(header_str.size());
    uint32_t total_size = static_cast<uint32_t>(4 + header_size + args.size());

    uint32_t net_header_size = ::htonl(header_size);
    uint32_t net_total_size = ::htonl(total_size);

    std::string frame;
    frame.append(reinterpret_cast<const char*>(&net_total_size), sizeof(net_total_size));
    frame.append(reinterpret_cast<const char*>(&net_header_size), sizeof(net_header_size));
    frame.append(header_str);
    frame.append(args);

    return frame;
}

bool ReadN(int fd, void* buf, size_t n)
{
    char* p = reinterpret_cast<char*>(buf);
    size_t left = n;
    while(left > 0)
    {
        size_t ret = ::read(fd, buf, left);
        if (ret > 0)
        {
            p += ret;
            left -= ret;
        }
        else if (ret == 0)
        {
            return false;
        }
        else
        {
            if (errno == EINTR) 
            {
                continue;
            }
            return false;
        }
    }
    return true;
}

void ReadAndPrintResponse(int fd, const char* scenario) {
    uint32_t net_total, net_header;
    if (!ReadN(fd, &net_total, 4) || !ReadN(fd, &net_header, 4)) {
        std::cerr << "[" << scenario << "] read frame header failed\n";
        return;
    }
    uint32_t total_size = ntohl(net_total);
    uint32_t header_size = ntohl(net_header);

    if (total_size < 4 || header_size > total_size - 4) {
        std::cerr << "[" << scenario << "] invalid frame\n";
        return;
    }

    std::string header_str(header_size, '\0');
    std::string body_str(total_size - 4 - header_size, '\0');
    if (!ReadN(fd, &header_str[0], header_size) ||
        !ReadN(fd, &body_str[0], body_str.size())) {
        std::cerr << "[" << scenario << "] read body failed\n";
        return;
    }

    myrpc::RpcResponseHeader resp_header;
    if (!resp_header.ParseFromString(header_str)) {
        std::cerr << "[" << scenario << "] parse response header failed\n";
        return;
    }

    std::cout << "[" << scenario << "] errcode=" << resp_header.error_code()
              << ", errtext=" << resp_header.error_text()
              << ", body_size=" << resp_header.response_size() << "\n";
}

int Connect() {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); return -1; }

    struct timeval tv = {3, 0};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(8000);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    if (connect(fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("connect");
        close(fd);
        return -1;
    }
    return fd;
}

void TestHalfPacket() {
    std::cout << "\n=== Test 1: half packet ===\n";
    int fd = Connect();
    if (fd < 0) return;

    std::string frame = BuildLoginFrame("haojun", "123456");
    size_t half = frame.size() / 2;

    // 发送前半
    ssize_t n1 = write(fd, frame.data(), half);
    std::cout << "sent first half: " << n1 << " bytes\n";
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // 发送后半
    ssize_t n2 = write(fd, frame.data() + half, frame.size() - half);
    std::cout << "sent second half: " << n2 << " bytes\n";

    ReadAndPrintResponse(fd, "half");
    close(fd);
}

// 2. 一次 write 两个完整请求
void TestTwoFullPackets() {
    std::cout << "\n=== Test 2: two full packets ===\n";
    int fd = Connect();
    if (fd < 0) return;

    std::string frame1 = BuildLoginFrame("haojun", "123456");
    std::string frame2 = BuildLoginFrame("testuser", "pass");

    std::string combined = frame1 + frame2;
    ssize_t n = write(fd, combined.data(), combined.size());
    std::cout << "sent two packets: " << n << " bytes\n";

    // 应该收到两个响应
    ReadAndPrintResponse(fd, "two-1");
    ReadAndPrintResponse(fd, "two-2");
    close(fd);
}

// 3. 一次 write 一个半请求（完整一帧 + 另一帧的前半）
void TestOneAndHalfPacket() {
    std::cout << "\n=== Test 3: one and a half packet ===\n";
    int fd = Connect();
    if (fd < 0) return;

    std::string frame1 = BuildLoginFrame("haojun", "123456");
    std::string frame2 = BuildLoginFrame("testuser", "pass");
    std::string combined = frame1 + frame2.substr(0, frame2.size()/2);

    ssize_t n = write(fd, combined.data(), combined.size());
    std::cout << "sent one full + half: " << n << " bytes\n";

    // 第一个完整帧应产生响应
    ReadAndPrintResponse(fd, "1.5-1");

    // 等待一会儿，再把后半发完
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    ssize_t n2 = write(fd, frame2.data() + frame2.size()/2,
                       frame2.size() - frame2.size()/2);
    std::cout << "sent remaining half: " << n2 << " bytes\n";

    // 第二个完整帧的响应
    ReadAndPrintResponse(fd, "1.5-2");
    close(fd);
}

int main() {
    TestHalfPacket();
    TestTwoFullPackets();
    TestOneAndHalfPacket();
    return 0;
}