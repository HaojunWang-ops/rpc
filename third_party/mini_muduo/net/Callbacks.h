#pragma once 

#include "base/Timestamp.h"

#include <functional>
#include <memory>

namespace reactor{
    namespace net
    {
        class Buffer;
        class TcpConnection;

        typedef std::shared_ptr<TcpConnection> TcpConnectionPtr;
        typedef std::function<void()> TimerCallback;
        typedef std::function<void (const TcpConnectionPtr&)> ConnectionCallback;
        typedef std::function<void (const TcpConnectionPtr&)> CloseCallback;
        typedef std::function<void (const TcpConnectionPtr&)> WriteCompleteCallback;
        typedef std::function<void (const TcpConnectionPtr&, size_t)> HighWaterMarkCallback;

        typedef std::function<void (const TcpConnectionPtr&, Buffer*, Timestamp)> MessageCallback;
        
        void defaultConnectionCallback(const TcpConnectionPtr& conn);
        void defaultMessageCallback(const TcpConnectionPtr& conn, Buffer* buffer, Timestamp receiveTime);
    }
}