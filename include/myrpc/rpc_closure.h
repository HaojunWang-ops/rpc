#pragma once

#include "rpc_controller.h"

#include <functional>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>

template <typename F>
class SimpleRpcClosure : public google::protobuf::Closure {
public:
    template <typename Fn>
    explicit SimpleRpcClosure(Fn&& f)
        : f_(std::forward<Fn>(f))
    {
    }

    void Run() override
    {
        std::unique_ptr<SimpleRpcClosure> guard(this);
        f_();
    }

private:
    F f_;
};

template <typename F>
google::protobuf::Closure* SendResponseClosure(F&& f)
{
    using Fn = std::decay_t<F>;
    return new SimpleRpcClosure<Fn>(std::forward<F>(f));
}