#include "rpc_provider.h"
#include "user.pb.h"
#include "rpc_header.pb.h"
#include "rpc_controller.h"
#include "rpc_channel.h"
#include "Logging.h"

#include <google/protobuf/stubs/common.h>
#include <iostream>
#include <thread>
#include <chrono>
#include <mutex>
#include <condition_variable>

void batch_async(RpcChannel* channel, demo::UserService_Stub& stub) 
{
    const int kRequestCount = 100;
    auto completed = std::make_shared<std::atomic<int>>(0);

    std::mutex mutex;
    std::condition_variable cv;
    bool all_done;

    for (int i = 0; i < kRequestCount; i++)
    {
        auto* request = new demo::LoginRequest;
        auto* response = new demo::LoginResponse;
        auto* controller = new SimpleRpcController;

        request->set_name("haojun");
        request->set_password("123456");

        google::protobuf::Closure* done = SendResponseClosure(
            [completed, request, response, controller, &mutex, &cv, &all_done, kRequestCount]()
            {
                if (controller->Failed())
                {
                    LOG_ERROR << "request " << request->name() << " failed: " << controller->ErrorText();
                }
                else
                {
                    LOG_INFO << "request " << request->name() << " success: " << response->DebugString();
                }

                delete request;
                delete response;
                delete controller;

                int n = ++(*completed);

                if (n == kRequestCount) {
                    {
                        std::lock_guard<std::mutex> lock(mutex);
                        all_done = true;
                    }
                    cv.notify_one();
                    LOG_INFO << "all requests completed";
                }
            }
        );
        stub.Login(controller, request, response, done);


    }        
    std::unique_lock<std::mutex> lock(mutex);
    cv.wait(lock, [&all_done](){
        return all_done;
    });
}

int main()
{
    RpcChannel channel("127.0.0.1", 8000);
    channel.start();
    demo::UserService_Stub stub(&channel);

    batch_async(&channel, stub);

    return 0;
}