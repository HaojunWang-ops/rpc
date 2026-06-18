#pragma once

#include <google/protobuf/message.h>
#include <google/protobuf/service.h>

#include <condition_variable>
#include <mutex>

struct PendingCall
{
    google::protobuf::RpcController* controller = nullptr;
    google::protobuf::Message* response = nullptr;
    google::protobuf::Closure* done = nullptr;

    std::mutex mutex;
    std::condition_variable cv;
    bool finished = false;
};