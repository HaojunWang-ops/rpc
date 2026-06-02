#include "rpc_controller.h"

void SimpleRpcController::StartCancel()
{
    canceled_ = true;
    if (on_cancel_)
    {
        google::protobuf::Closure* callback = on_cancel_;
        on_cancel_ = nullptr;
        callback->Run();
    }
}

void SimpleRpcController::NotifyOnCancel(google::protobuf::Closure* callback)
{
    if (canceled_)
    {
        callback->Run();
    } 
    else 
    {
        on_cancel_ = callback;
    }
}