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

/*
1. Future destruction does not cancel the RPC.
2. RPC completion is independent of whether the user calls get().
3. If the channel/pool stops before response arrives, the future becomes ready with controller failed / exception / error result.
4. Timeout completion may happen on timeout manager thread.
5. Normal response completion may happen on reader thread or callback executor thread, depending on implementation.
6.Future API owns the request, response and controller inside FutureCallState.
The caller does not need to keep external request/response/controller objects alive after submitting a future call.
Destroying the returned future does not cancel the RPC.
The internal FutureCallState is kept alive by the pending call / completion closure until the RPC finishes, times out, or is failed by channel stop.
*/