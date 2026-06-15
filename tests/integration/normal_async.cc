#include "common.h"
#include "rpc_channel.h"
#include "user.pb.h"
#include "rpc_controller.h"
#include "rpc_channel_pool.h"

#include <google/protobuf/stubs/common.h>
#include <iostream>

void PrintController(const char* case_name, SimpleRpcController& controller)
{
    if (controller.Failed())
    {
        std::cout << "[" << case_name << "] RPC failed: "
                  << controller.ErrorText() << "\n";
    }
    else
    {
        std::cout << "[" << case_name << "] RPC ok\n";
    }
}

void TestLoginSuccess(demo::UserService_Stub& stub)
{
    SimpleRpcController controller;
    demo::LoginRequest request;
    demo::LoginResponse response;

    request.set_name("haojun");
    request.set_password("123456");

    stub.Login(&controller, &request, &response, nullptr);

    PrintController("LoginSuccess", controller);
    std::cout << "code=" << response.code()
              << ", message=" << response.message()
              << ", success=" << response.success() << "\n\n";
}

int main()
{

    RpcChannelPool pool("127.0.0.1", 8000, 1);
    pool.start();
    demo::UserService_Stub stub(&pool);

    TestLoginSuccess(stub);

    return 0;
}
