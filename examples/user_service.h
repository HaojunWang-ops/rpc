#include "rpc_provider.h"
#include "examples/user.pb.h"

#include <google/protobuf/stubs/common.h>
#include <iostream>

class UserServiceImpl : public demo::UserService
{
public:
    void Login(google::protobuf::RpcController *controller,
               const demo::LoginRequest *request,
               demo::LoginResponse *response,
               google::protobuf::Closure *done) override
    {
        std::cout << "UserServiceImpl::Login called\n";
        std::cout << "name = " << request->name() << "\n";
        std::cout << "password = " << request->password() << std::endl;

        if (request->name().empty()){
            controller->SetFailed("login name is empty");

            if (done){
                done->Run();
            }
            return;
        }

        if (request->name() == "haojun" &&
            request->password() == "123456")
        {
            response->set_code(0);
            response->set_message("login success");
            response->set_success(true);
        }
        else
        {
            response->set_code(1);
            response->set_message("login failed");
            response->set_success(false);
        }

        if (done)
        {
            done->Run();
        }
    }

    void Register(google::protobuf::RpcController *controller,
                  const demo::RegisterRequest *request,
                  demo::RegisterResponse *response, 
                  google::protobuf::Closure *done) override
    {
        std::cout << "UserServiceImpl::Register called\n";
        std::cout << "name = " << request->name() << "\n";
        //如果endl，内容很可能在缓冲区，输出不出来
        std::cout << "password= " << request->password() << std::endl;

        if (request->name().empty()){
            controller->SetFailed("register name is empty");

            if (done){
                done->Run();
            }

            return;
        }
        
        response->set_code(0);
        response->set_message("register success");
        response->set_success(true);

        if (done){
            done->Run();
        }
    }
};