#include "rpc_provider.h"
#include "user.pb.h"
#include "user_services.h"
#include "Logging.h"

static reactor::Logger::LogLevel parseLogLevel(const std::string& level)
{
    if (level == "trace") return reactor::Logger::TRACE;
    if (level == "debug") return reactor::Logger::DEBUG;
    if (level == "info")  return reactor::Logger::INFO;
    if (level == "warn")  return reactor::Logger::WARN;
    if (level == "error") return reactor::Logger::ERROR;
    if (level == "fatal") return reactor::Logger::FATAL;

    return reactor::Logger::INFO;
}

int main(int argc, char* argv[])
{
    if (argc >= 2)
    {
        reactor::Logger::setLogLevel(parseLogLevel(argv[1]));
    }else {
        reactor::Logger::setLogLevel(reactor::Logger::WARN);
    }

    RpcProvider provider(4);

    UserServiceImpl user_service;

    provider.NotifyService(&user_service);

    provider.Run("0.0.0.0", 8000);

    return 0;
}