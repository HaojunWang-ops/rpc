#include "rpc_provider.h"
#include "examples/user.pb.h"
#include "examples/user_service.h"

int main()
{
    RpcProvider provider(4);

    UserServiceImpl user_service;

    provider.NotifyService(&user_service);

    provider.Run("0.0.0.0", 8000);

    return 0;
}