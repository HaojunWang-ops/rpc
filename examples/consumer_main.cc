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

void TestLoginWrongPassword(demo::UserService_Stub& stub)
{
    SimpleRpcController controller;
    demo::LoginRequest request;
    demo::LoginResponse response;

    request.set_name("haojun");
    request.set_password("wrong");

    stub.Login(&controller, &request, &response, nullptr);

    PrintController("LoginWrongPassword", controller);
    std::cout << "code=" << response.code()
              << ", message=" << response.message()
              << ", success=" << response.success() << "\n\n";
}

void TestLoginEmptyName(demo::UserService_Stub& stub)
{
    SimpleRpcController controller;
    demo::LoginRequest request;
    demo::LoginResponse response;

    request.set_name("");
    request.set_password("123456");

    stub.Login(&controller, &request, &response, nullptr);

    PrintController("LoginEmptyName", controller);
    std::cout << "code=" << response.code()
              << ", message=" << response.message()
              << ", success=" << response.success() << "\n\n";
}

void TestRegisterSuccess(demo::UserService_Stub& stub)
{
    SimpleRpcController controller;
    demo::RegisterRequest request;
    demo::RegisterResponse response;

    request.set_name("haojun");
    request.set_password("123456");

    stub.Register(&controller, &request, &response, nullptr);

    PrintController("RegisterSuccess", controller);
    std::cout << "code=" << response.code()
              << ", message=" << response.message()
              << ", success=" << response.success() << "\n\n";
}

void TestRegisterEmptyName(demo::UserService_Stub& stub)
{
    SimpleRpcController controller;
    demo::RegisterRequest request;
    demo::RegisterResponse response;

    request.set_name("");
    request.set_password("123456");

    stub.Register(&controller, &request, &response, nullptr);

    PrintController("RegisterEmptyName", controller);
    std::cout << "code=" << response.code()
              << ", message=" << response.message()
              << ", success=" << response.success() << "\n\n";
}

int main()
{
    RpcChannelPool pool("127.0.0.1", 8000, 4);
    pool.start();
    demo::UserService_Stub stub(&pool);

    TestLoginSuccess(stub);
    TestLoginWrongPassword(stub);
    TestLoginEmptyName(stub);
    //test for rpc call timeout
    //sleep(10);
    TestRegisterSuccess(stub);
    TestRegisterEmptyName(stub);

    return 0;
}