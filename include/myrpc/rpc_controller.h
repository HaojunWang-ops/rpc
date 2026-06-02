#pragma once

#include <google/protobuf/service.h>
#include <string>

class SimpleRpcController : public google::protobuf::RpcController {
public:
    void Reset() override {
        failed_ = false;
        canceled_ = false;
        on_cancel_ = nullptr;
        error_text_.clear();
    }

    bool Failed() const override {
        return failed_;
    }

    std::string ErrorText() const override {
        return error_text_;
    }

    void StartCancel() override;

    void SetFailed(const std::string& reason) override {
        failed_ = true;
        error_text_ = reason;
    }

    void SetFailed(int code, const std::string& reason) {
        failed_ = true;
        error_code_ = code;
        error_text_ = reason;
    }

    int error_code() { return error_code_; }
    const std::string& error_text() { return error_text_; }
     
    bool IsCanceled() const override {
        return canceled_;
    }

    void NotifyOnCancel(google::protobuf::Closure* callback) override;

private:
    bool failed_ = false;
    bool canceled_ = false;
    google::protobuf::Closure* on_cancel_;

    int error_code_;
    std::string error_text_;
};