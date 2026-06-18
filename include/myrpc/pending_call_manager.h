#pragma once

#include "pending_call.h"

#include <cstdint>
#include <memory>
#include <mutex>
#include <unordered_map>

class PendingCallManager
{
public:
    enum class AddResult
    {
        kOk,
        kNotAccepting,
        kDuplicate
    };

    AddResult add(uint64_t request_id,
                  const std::shared_ptr<PendingCall>& call);

    std::shared_ptr<PendingCall> take(uint64_t request_id);

    std::unordered_map<uint64_t, std::shared_ptr<PendingCall>> failAllAndStopAccepting();

    void startAccepting();
    void stopAccepting();
    void clear();
    void resetForStart();

private:
    std::mutex mutex_;
    bool accepting_ = false;
    std::unordered_map<uint64_t, std::shared_ptr<PendingCall>> pending_;
};