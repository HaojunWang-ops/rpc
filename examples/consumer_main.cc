#include "common.h"
#include "rpc_channel.h"
#include "examples/user.pb.h"
#include "rpc_controller.h"

#include <google/protobuf/stubs/common.h>
#include <iostream>

int main()
{
    GOOGLE_PROTOBUF_VERIFY_VERSION;

    RpcChannel channel("127.0.0.1", 8000);

    demo::UserService_Stub stub(&channel);

    {
        demo::LoginRequest request;
        request.set_name("haojun");
        request.set_password("123456");

        demo::LoginResponse response;
        SimpleRpcController controller;

        stub.Login(&controller, &request, &response, nullptr);

        if (controller.Failed())
        {
            std::cerr << "rpc failed: "
                      << controller.ErrorText()
                      << "\n";
            return 1;
        }
        else
        {
            std::cout << "Login response:\n";
            std::cout << "  code = " << response.code() << "\n";
            std::cout << "  message = " << response.message() << "\n";
            std::cout << "  success = " << std::boolalpha
                      << response.success() << "\n";
        }
    }

    {
        demo::RegisterRequest request;
        request.set_name("haojun");
        request.set_password("abcdef");

        demo::RegisterResponse response;
        SimpleRpcController controller;

        stub.Register(&controller, &request, &response, nullptr);

        if (controller.Failed())
        {
            std::cerr << "Register rpc failed: "
                      << controller.ErrorText()
                      << "\n";
        }
        else
        {
            std::cout << "Register response:\n";
            std::cout << "  code = " << response.code() << "\n";
            std::cout << "  message = " << response.message() << "\n";
            std::cout << "  success = " << std::boolalpha
                      << response.success() << "\n";
        }
    }

    google::protobuf::ShutdownProtobufLibrary();
    return 0;
}