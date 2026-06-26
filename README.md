# my_rpc
`my_rpc` 是一个基于 protobuf 和内置 `third_party/mini_muduo` 反应器（reactor）的 C++17 RPC 框架。它提供了 protobuf `RpcChannel` 实现、持久连接池、超时处理、异步回调完成、future 风格的客户端 API，以及由业务线程池支撑的服务端分发器。
本文档描述当前项目结构、请求流程、并发模型、边界行为以及测试命令。
## 目录结构
- `include/myrpc/`：公共框架头文件。
- `src/`：框架实现及内部 RPC 协议定义。
- `proto/`：演示服务 protobuf 定义。
- `services/`：演示 `UserService` 实现。
- `examples/`：服务提供者（provider）和消费者（consumer）可执行程序。
- `tests/unit/`：编解码、未决调用（pending-call）、传输层和线程池测试。
- `tests/integration/`：端到端、超时、连接丢失、连接池、future 及协议边界测试。
- `third_party/mini_muduo/`：内置反应器/网络库。
## 构建与测试
Debug 构建：
```bash
cmake -S . -B build-debug -DCMAKE_BUILD_TYPE=Debug
cmake --build build-debug -j
ctest --test-dir build-debug --output-on-failure
```
ASAN 构建：
```bash

cmake -S . -B build-asan -DCMAKE_BUILD_TYPE=Debug -DMYRPC_ENABLE_ASAN=ON
cmake --build build-asan -j
ctest --test-dir build-asan --output-on-failure
```
TSAN 构建：
```bash

cmake -S . -B build-tsan -DCMAKE_BUILD_TYPE=Debug -DMYRPC_ENABLE_TSAN=ON
cmake --build build-tsan -j
ctest --test-dir build-tsan --output-on-failure
```
ASAN 和 TSAN 是互斥的 CMake 选项。

当前工作区内的验证结果：

- `build-debug`：53/53 测试通过。
- `build-asan`：53/53 测试通过。
- `build-tsan`：53/53 测试通过。
    
## 运行示例

启动服务提供者：

```bash

./build-debug/examples/provider
```
在另一个终端中运行消费者：
```bash

./build-debug/examples/consumer
```

演示服务定义在 `proto/user.proto` 中，由 `services/user_services.h` 中的 `UserServiceImpl` 实现。

## 线路协议

客户端请求帧格式为：

```text

uint32 total_size   网络字节序
uint32 header_size  网络字节序
RpcHeader header    protobuf 字节
request body        请求体 protobuf 字节
```
`total_size` 包含 `header_size` 字段、序列化后的 `RpcHeader` 以及请求体。它不包含开头的 `total_size` 字段本身。

`RpcHeader` 包含：

- `request_id`
- `service_name` 
- `method_name`
- `args_size`
    
服务端响应帧使用相同的外部布局：

```text

uint32 total_size
uint32 header_size
RpcResponseHeader header
response body
```

`RpcResponseHeader` 包含：

- `request_id`
- `error_code`
- `error_text`
- `response_size`
    
## 服务端流程

`RpcProvider` 拥有服务端分发器：

1. `NotifyService()` 注册 protobuf 服务及其方法描述符。
2. `Run()` 启动业务 `ThreadPool`，创建 `mini_muduo` 的 `TcpServer`，并进入反应器事件循环。
3. `onMessage()` 在 IO 线程上运行。它校验外部帧大小，组装完整帧，并将每个完整请求帧提交到业务线程池。
4. `doRpcTask()` 在业务线程上运行。它解析 `RpcHeader`，校验 `request_id`、服务名、方法名和消息体大小，创建 protobuf 请求和响应对象，然后调用目标 protobuf 服务方法。
5. 服务方法调用 `done->Run()`。该闭包序列化并发送正常响应或错误响应。
    

服务注册应在 `Run()` 之前完成。`service_map_` 在启动后只读，运行时无需锁保护。

## 客户端流程

`MyRpcChannel` 持有一条持久 TCP 连接：

1. `start()` 连接套接字，启动读线程，启动超时管理器，并启用未决调用接受。
2. `CallMethod()` 编码请求帧，创建 `PendingCall`，将其插入 `PendingCallManager`，在 `send_mutex_` 保护下写入帧，并注册超时。
3. 对于同步调用（`done == nullptr`），调用者在 `PendingCall` 条件变量上等待。
4. 对于异步调用，完成回调被投递到 `CallbackExecutor`。
5. 读线程读取响应帧，解码响应头和响应体，通过 `request_id` 移除未决调用，并完成该调用。
6. 超时、连接丢失、发送失败和停止路径都通过相同的未决调用所有权机制完成未决调用。
    
`RpcChannelPool` 拥有多个 `MyRpcChannel` 实例和一个 `CallbackExecutor`：

1. `start()` 创建固定数量的通道，并发布不可变快照。
2. `CallMethod()` 进入池生命周期门控，轮询选择一个可用通道，并委托调用。
3. `pickChannel()` 可以机会性地修复不可用通道。
4. `repairDeadChannels()` 复制当前快照，在修复锁外启动替换通道，只有当旧快照仍然是最新的时才发布新快照。
5. `stop()` 清空发布的快照，停止所有通道，等待活动提交离开生命周期门控，然后停止回调执行器。
    

future API 通过将请求、响应、控制器和 promise 状态封装在共享的 `FutureCallState` 中，包装了异步路径。

## 并发模型

重要的所有权和生命周期规则：

- `RpcChannelPool` 拥有其 `CallbackExecutor` 和所有 `MyRpcChannel` 对象。
- `MyRpcChannel` 持有回调执行器的非拥有指针。
- `MyRpcChannel` 必须由 `std::shared_ptr` 管理；`CallMethod()` 使用 `shared_from_this()`。
- 对于普通异步调用，调用者必须确保 `request`、`response` 和 `controller` 在 `done` 运行之前存活。`Future` API 在内部处理该所有权。
- `RpcProvider` 不拥有服务实现对象；注册的服务对象必须比 `Run()` 存活更久。
- 外部调用线程在销毁 `RpcChannelPool` 之前必须停止使用它。`stop()` 被设计为能与调用和修复操作发生竞态，但在另一个线程可能仍会调用池时销毁对象是不安全的。
    

锁与同步：

- `PendingCallManager::mutex_` 保护未决调用映射和接受标志。
- `MyRpcChannel::lifecycle_mutex_` 保护通道状态机。
- `MyRpcChannel::send_mutex_` 序列化写操作，并在 IO 停止后协调套接字关闭。
- `MyRpcChannel::reader_mutex_` 保护读线程所有权、连接和分离。
- `RpcTransport::fd_mutex_` 保护 fd 替换、关闭和 shutdown。
- `RpcChannelPool::lifecycle_mutex_` 保护池状态和活动提交计数。
- `RpcChannelPool::repair_mutex_` 保护快照发布和修复序列化。
- `RpcTimeoutManager` 使用一个生命周期互斥量，外加内部堆互斥量和条件变量。
- `CallbackExecutor` 和 `ThreadPool` 各自使用自己的互斥量和条件变量保护任务队列。
    
原子操作：

- `MyRpcChannel::running_` 是读线程和写线程的跨线程停止信号。存储使用 release 顺序，读取使用 acquire 顺序。
- `MyRpcChannel::timeout_ms_` 使用 release 顺序更新，使用 acquire 顺序读取。
- `MyRpcChannel::next_request_id_` 是原子单调递增的请求 id 来源。
- `RpcTransport::fd_` 是原子 fd 快照；生命周期操作仍使用 `fd_mutex_`。
- `RpcChannelPool::next_` 使用 relaxed 顺序，因为它只是一个分发计数器。
- `RpcChannelPool::channels_snapshot_` 通过对 `shared_ptr` 的显式 acquire/release 操作来发布和加载。
    
完成所有权：

- `PendingCallManager::take()` 将完成所有权精确交给一个线程。
- `failAllAndStopAccepting()` 原子性地停止接受，并将所有未完成的未决调用转移给调用者。
- 响应、超时、停止、读失败和发送失败路径只完成它们已成功从 `PendingCallManager` 取出的调用。
- `SimpleRpcClosure` 和 `FutureClosure` 在 `Run()` 中删除自身。
    
## 边界行为

已验证的协议和运行时边界：

- 服务端拒绝 `total_size < 4` 的畸形帧。
- 服务端拒绝大于 64 MiB 的帧。
- 服务端拒绝头/体大小不匹配。
- 服务端拒绝 `request_id == 0`。
- 服务端对未知服务、未知方法、解析失败和响应序列化失败返回错误帧。
- 客户端拒绝大于 64 MiB 的响应帧。
- 客户端拒绝大于 1 MiB 的响应头。
- 客户端拒绝畸形响应头/体大小以及 protobuf 解析错误。
- 客户端超时默认 3000 ms，可逐通道配置。
- 传输连接超时默认 1000 ms，可配置。
- 池构造时若大小为 `0` 则无法启动。
- 通过已停止的池调用会立即失败并内联运行 `done`。
    
操作注意事项：

- `MyRpcChannel` 直接异步使用要求回调执行器已存活并已启动；如果回调投递失败，当前实现会记录投递失败。
- `RpcTimeoutManager::stop()` 停止工作线程，但不会清空超时堆；过期的超时条目是无害的，因为未决调用通过请求 id 进行键控，但它们可能一直存在直到下一次启动时被清理。
- `RpcTransport` 在 `MyRpcChannel` 的关闭顺序下是安全的。直接公开使用 `close()` 并与 `readN()` 或 `writeN()` 并发时，应遵循相同的停止、shutdown、连接/序列化、关闭模式。
- 服务端响应发送依赖于 `mini_muduo::TcpConnection::send()` 使用所拥有的数据调用。`RpcProvider` 使用右值字符串发送路径，该路径捕获响应缓冲区以进行跨线程分发。
    
## 测试覆盖率

单元测试覆盖：

- RPC 帧编码和响应解码。
- 帧大小和空输入校验。
- 未决调用的添加、取出、全部失败语义以及并发访问。
- 线程池启动、停止、排空和并发提交。
- 传输连接成功、配置的超时及 fd 清理。

集成测试覆盖：

- 正常的同步和异步 RPC 调用。
- 批处理异步完成且精确执行一次。
- 并发同步调用。
- 请求未决时连接丢失。
- 通道超时和延迟响应行为。
- 启动前调用和池停止后调用。
- 池启动/停止、固定连接数、轮询分发、修复、并发调用加修复、并发修复加停止以及双重启动/停止。
- 坏包处理、TCP 分片/合并、服务未找到错误处理、超大响应帧以及 future API 成功/错误路径。

## 维护注意事项

- 除非有意进行协议迁移，否则保持协议布局稳定。    
- 不要在持有内部锁时运行用户回调。
- 对于每个新增的 RPC 错误路径，确保 `done` 被恰好调用一次。
- 对于新的并发改动，请记录对象所有权、生命周期假设、锁顺序及原子内存顺序假设。
- 更倾向使用聚焦于特定竞态或边界条件的测试；先运行 Debug 测试，再运行 ASAN/TSAN 以覆盖生命周期和数据竞态。