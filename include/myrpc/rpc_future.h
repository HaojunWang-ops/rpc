#pragma once

#include "rpc_controller.h"
#include "rpc_header.pb.h"

#include <future>
#include <memory>
#include <string>

#include <google/protobuf/service.h>

template <typename Response>
struct RpcFutureResult
{
    bool ok = false;

    myrpc::RpcErrorCode error_code = myrpc::RPC_OK;
    std::string error_text;

    Response response;
};

template <typename Request, typename Response>
struct FutureCallState
{
    SimpleRpcController controller;
    Request request;
    Response response;

    std::promise<RpcFutureResult<Response> > promise;
};

template <typename Request, typename Response>
class FutureClosure : public google::protobuf::Closure
{
public:
    using State = FutureCallState<Request, Response>;

    explicit FutureClosure(std::shared_ptr<State> state)
        : state_(std::move(state))
    {
    }
    
    void Run() override
    {
        std::unique_ptr<FutureClosure> self_guard(this);

        RpcFutureResult<Response> result;

        if(state_->controller.Failed())
        {
            result.ok = false;
            result.error_code = state_->controller.error_code();
            result.error_text = state_->controller.error_text();
        }
        else 
        {
            result.ok = true;
            result.error_code = myrpc::RPC_OK;
            result.response = state_->response;
        }
        
        try
        {
            state_->promise.set_value(std::move(result));
        }
        catch(const std::future_error&)
        {       
        }
    }
private:
    std::shared_ptr<State> state_;
};