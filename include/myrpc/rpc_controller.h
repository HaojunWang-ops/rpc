#pragma once

#include "rpc_header.pb.h"

#include <google/protobuf/service.h>
#include <string>

class SimpleRpcController : public google::protobuf::RpcController {
public:
    void Reset() override {
        canceled_ = false;
        on_cancel_ = nullptr;

        error_code_ = myrpc::RPC_OK;
        error_text_.clear();
    }

    bool Failed() const override {
        return (error_code_ == myrpc::RPC_OK);
    }

    std::string ErrorText() const override {
        return error_text_;
    }

    void StartCancel() override;

    void SetFailed(const std::string& reason) override
    {

    }

    void SetFailed(myrpc::RpcErrorCode code)
    {
        error_code_ = code;
    }

    void SetFailed(myrpc::RpcErrorCode code, const std::string& reason) {
        error_code_ = code;
        error_text_ = reason;
    }

    myrpc::RpcErrorCode error_code() { return error_code_; }
    const std::string& error_text() { return error_text_; }
     
    bool IsCanceled() const override {
        return canceled_;
    }

    void NotifyOnCancel(google::protobuf::Closure* callback) override;

private:
    bool canceled_ = false;
    google::protobuf::Closure* on_cancel_;

    myrpc::RpcErrorCode error_code_;
    std::string error_text_;
};