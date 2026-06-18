#include "pending_call_manager.h"

PendingCallManager::AddResult PendingCallManager::add(uint64_t request_id,
                        const std::shared_ptr<PendingCall>& call)
{
    std::lock_guard<std::mutex> lock(mutex_);

    if (!accepting_)
    {
        return AddResult::kNotAccepting;
    }

    auto [it, inserted] = pending_.emplace(request_id, call);
    if (!inserted)
    {
        return AddResult::kDuplicate;
    }

    return AddResult::kOk;
}

std::shared_ptr<PendingCall> PendingCallManager::take(uint64_t request_id)
{
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = pending_.find(request_id);
    if (it == pending_.end())
    {
        return nullptr;
    }

    auto call = it->second;
    pending_.erase(it);
    return call;
}

std::unordered_map<uint64_t, std::shared_ptr<PendingCall>> PendingCallManager::failAllAndStopAccepting()
{
    std::lock_guard<std::mutex> lock(mutex_);

    accepting_ = false;

    std::unordered_map<uint64_t, std::shared_ptr<PendingCall>> pending;
    pending.swap(pending_);
    return pending;
}

void PendingCallManager::startAccepting()
{
    std::lock_guard<std::mutex> lock(mutex_);
    accepting_ = true;
}

void PendingCallManager::stopAccepting()
{
    std::lock_guard<std::mutex> lock(mutex_);
    accepting_ = false;
}

void PendingCallManager::clear()
{
    std::lock_guard<std::mutex> lock(mutex_);
    pending_.clear();
}

void PendingCallManager::resetForStart()
{
    std::lock_guard<std::mutex> lock(mutex_);
    pending_.clear();
    accepting_ = true;
}