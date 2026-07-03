# my_rpc

`my_rpc` 是一个基于 protobuf generic services 和内置 `third_party/mini_muduo` reactor 的 C++17 RPC 框架。当前项目提供：

- protobuf `RpcChannel` 支持
- 客户端持久连接
- 固定大小的客户端连接池
- 同步调用、异步回调调用和 `future` 风格调用
- 请求超时处理
- 基于业务 `ThreadPool` 的服务端分发器
- 协议、超时、连接丢失、future、连接池和坏包相关测试

项目规模刻意保持较小，适合学习和本地实验。当前实现已经覆盖了不少并发和错误路径，但生命周期、停止语义和资源控制还没有达到生产级 RPC runtime 的强度。本文档同时说明当前代码实际保证的语义和已知薄弱点。

## 目录结构

- `include/myrpc/`：框架公共头文件。
- `src/`：框架实现和内部 RPC 协议定义。
- `proto/`：演示 protobuf 服务定义。
- `services/`：演示 `UserService` 实现。
- `examples/`：provider 和 consumer 示例程序。
- `tests/unit/`：codec、pending-call、provider、transport 和 thread-pool 单元测试。
- `tests/integration/`：端到端 RPC、连接丢失、超时、连接池、future、坏包和 TCP 分片测试。
- `third_party/mini_muduo/`：内置 reactor/network 库。

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

`MYRPC_ENABLE_ASAN` 和 `MYRPC_ENABLE_TSAN` 互斥。

当前 Debug 验证结果：

```text
ctest --test-dir build-debug --output-on-failure
57/57 tests passed
```

## 运行示例

启动 provider：

```bash
./build-debug/examples/provider
```

另一个终端运行 consumer：

```bash
./build-debug/examples/consumer
```

演示服务定义在 `proto/user.proto`，实现位于 `services/user_services.h`。

## 基本用法

服务端：

```cpp
RpcProvider provider(4);
UserServiceImpl service;

provider.NotifyService(&service);
provider.Run("0.0.0.0", 8000);
```

服务端使用规则：

- 服务必须在 `Run()` 之前注册。
- `RpcProvider` 只保存服务对象裸指针，不拥有服务对象。
- 注册的服务对象必须比 `Run()` 存活更久。
- `Run()` 会阻塞在 `mini_muduo` 事件循环中。
- 当前没有公开的 `RpcProvider::Stop()` API。

客户端连接池：

```cpp
RpcChannelPool pool("127.0.0.1", 8000, 4);

if (!pool.start()) {
    // handle startup failure
}

demo::UserService_Stub stub(&pool);
demo::LoginRequest request;
demo::LoginResponse response;
SimpleRpcController controller;

request.set_name("haojun");
request.set_password("123456");

stub.Login(&controller, &request, &response, nullptr);

pool.stop();
```

Future API：

```cpp
auto future = pool.CallMethodFuture<demo::LoginResponse>(
    demo::UserService::descriptor()->FindMethodByName("Login"),
    request);

auto result = future.get();
if (!result.ok) {
    // inspect result.error_code and result.error_text
}
```

Future 调用会通过 `FutureCallState` 持有内部 request、response、controller、promise 和完成闭包。提交 future 调用后，调用者不需要继续保持外部 request/response/controller 对象存活。

## 线路协议

客户端请求帧：

```text
uint32 total_size   网络字节序
uint32 header_size  网络字节序
RpcHeader header    protobuf 字节
request body        protobuf 字节
```

`total_size` 包含 `header_size` 字段、序列化后的 `RpcHeader` 和请求体。它不包含最前面的 `total_size` 字段自身。

`RpcHeader` 字段：

- `request_id`
- `service_name`
- `method_name`
- `args_size`

服务端响应帧：

```text
uint32 total_size   网络字节序
uint32 header_size  网络字节序
RpcResponseHeader header
response body
```

`RpcResponseHeader` 字段：

- `request_id`
- `error_code`
- `error_text`
- `response_size`

当前协议限制：

- 客户端响应帧最大 64 MiB
- 客户端响应头最大 1 MiB
- 服务端请求帧最大 64 MiB

除非明确要做协议迁移，否则不要改变以上布局。

## 客户端流程

`MyRpcChannel` 拥有一条 TCP 连接：

1. `start()` 连接 socket，启动 reader 线程，启动 timeout manager，并启用 pending-call 接受。
2. `CallMethod()` 编码请求帧并创建 `PendingCall`。
3. `PendingCallManager::add()` 按 `request_id` 发布 pending call。
4. `send_mutex_` 串行化 socket 写入。
5. timeout manager 为请求记录 deadline。
6. reader 线程解码响应帧，并通过 `request_id` 取得 pending-call 完成权。
7. 响应、超时、连接丢失、发送失败和停止路径都通过同一套 pending-call 完成权机制完成调用。

`RpcChannelPool` 拥有多个 channel 和一个 `CallbackExecutor`：

1. `start()` 启动 callback executor，创建所有 channel，启动所有 channel，并发布不可变 channel 快照。
2. `CallMethod()` 进入 pool 生命周期门控，选择可用 channel，委托调用，然后离开门控。
3. `pickChannel()` 使用轮询选择，并可能机会性修复不可用 channel。
4. `repairDeadChannels()` 复制当前快照，在 snapshot 锁外启动替换 channel，只有旧快照仍为当前快照时才发布新快照。
5. `stop()` 清空已发布快照，停止旧 channel，等待活跃提交离开生命周期门控，然后停止 callback executor。

## 服务端流程

`RpcProvider` 拥有服务分发表和业务线程池：

1. `NotifyService()` 记录 protobuf service 指针和 method descriptor。
2. `Run()` 创建 `mini_muduo::TcpServer`，安装连接和消息回调，启动业务线程池，启动 TCP server，并进入事件循环。
3. `onMessage()` 运行在 IO 线程。它校验帧大小，等待完整帧，复制每个完整请求帧，并提交业务任务。
4. `doRpcTask()` 运行在业务线程。它解析 `RpcHeader`，校验 service/method/body size，构造 protobuf request/response 对象，并调用注册的 protobuf service。
5. service 实现必须恰好调用一次 `done->Run()`。该闭包序列化并发送正常响应或错误响应。

服务端限制：

- `Run()` 没有对应的公开 stop API。
- service registry 启动后没有锁保护，设计假设所有注册都发生在 `Run()` 之前。
- `NotifyService(nullptr)` 没有保护，会解引用空指针。
- 重复注册同名 service 不会报错，`unordered_map::emplace()` 会保留第一次注册。
- service 如果不调用 `done->Run()`，该 RPC 不会返回响应。
- service 如果多次调用 `done->Run()`，会违反闭包所有权并导致未定义行为。

## 所有权与生命周期

对象所有权：

- `RpcChannelPool` 拥有自己的 `CallbackExecutor`。
- `RpcChannelPool` 拥有所有 channel 快照中的 `MyRpcChannel`。
- `MyRpcChannel` 拥有 `RpcTransport`、`PendingCallManager`、`RpcTimeoutManager` 和 reader 线程句柄。
- `MyRpcChannel` 只保存非拥有的 `CallbackExecutor*`。
- `RpcProvider` 不拥有注册的 protobuf service 对象。
- 普通同步/异步调用中的 `PendingCall` 不拥有裸 `request`、`response` 和 `controller` 指针。
- future 调用中的 `FutureCallState` 拥有 request、response、controller、promise 和响应存储。

用户侧生命周期规则：

- 通过 `MyRpcChannel::create()` 创建 channel，保证 `shared_from_this()` 有效。
- 不要在其他线程仍可能调用 `CallMethod()`、`CallMethodFuture()`、`repairDeadChannels()`、`start()` 或 `stop()` 时销毁 `RpcChannelPool`。
- 销毁 pool 前，先停止或 join 用户创建的调用线程和 repair 线程。
- 普通异步调用中，`request`、`response` 和 `controller` 必须存活到 `done` 运行结束。
- 同步调用中，`request`、`response` 和 `controller` 必须存活到 `CallMethod()` 返回。
- future 调用中，请求会被复制进 `FutureCallState`，提交后不依赖外部 request 生命周期。
- provider 注册的服务对象必须比 `RpcProvider::Run()` 存活更久。

## 停止语义

推荐客户端关闭顺序：

```text
1. 用户代码停止创建新的 RPC 调用。
2. 停止或 join 用户自己的 repair 线程。
3. 停止或 join 用户自己的 caller 线程，或以其他方式保证它们不会再进入 pool。
4. 从非 callback-executor worker 线程调用 RpcChannelPool::stop()。
5. stop() 返回且外部使用者都退出后，再销毁 RpcChannelPool。
```

`RpcChannelPool::stop()` 当前保证：

- 对成功 `start()` 后的 pool 可调用。
- 清空已发布 channel 快照，使后续调用被拒绝。
- 先停止 channel，再停止 callback executor。
- 通过 `CallbackExecutor::stop()` 排空已经投递的 callback。
- pool 不再运行后进入的新调用会立即失败，并内联运行 `done`。

pool 停止语义的薄弱点：

- 当另一个线程已经在 stop 过程中时，第二个 `stop()` 会直接返回，而不是等待第一次 stop 完成。
- `stop()` 不等待并发执行中的 `repairDeadChannels()` 返回。
- `repairDeadChannels()` 可能在 `stop()` 已经开始后继续创建替换 channel。未发布的替换 channel 会被停止，但 pool 对象本身必须保持存活。
- 如果在 pool 的 `CallbackExecutor` worker 上调用 `RpcChannelPool::stop()`，最终会在该 worker 上调用 `CallbackExecutor::stop()`，当前实现会直接 `std::terminate()`。

`MyRpcChannel::stop()` 当前保证：

- 将 running channel 转入 stopping。
- 停止接受新的 pending call。
- 将当前所有 pending call 标记失败。
- `shutdown()` socket 以唤醒阻塞的读写。
- 从非 reader 线程调用时，会 join reader 线程。
- reader 停止且序列化写退出 `send_mutex_` 后关闭 fd。

channel 停止语义的薄弱点：

- 如果 callback 投递失败，`done` 会在完成调用的线程内联运行，可能是 reader 线程或 timeout-manager 线程。
- 用户 callback 不应该直接执行依赖 owner 线程语义的 stop/destruct 操作。
- `stop()` 会完成 pending call，但不会取消已经投递出去的用户 callback。

## 线程安全

内部同步或可并发调用：

- `PendingCallManager` 的公开操作。
- 已启动 channel 上的并发 `MyRpcChannel::CallMethod()`。
- pool 对象仍存活时的并发 `RpcChannelPool::CallMethod()` 和 `CallMethodFuture()`。
- executor 存活期间的 `CallbackExecutor::post()`。
- thread pool 已启动期间的 `ThreadPool::submit()`。

需要外部排序，不应裸并发：

- 销毁 `RpcChannelPool` 与任意 pool 公开方法并发。
- 销毁 `MyRpcChannel` 与仍可能持有或获取该 channel 的调用并发。
- RPC 完成线程写入 `SimpleRpcController` 时，用户线程同时读取或写入同一个 controller。
- 普通异步调用的 `done` 运行前，修改或销毁 protobuf request/response/controller。
- `Run()` 后继续注册 provider service。
- 直接使用 `RpcTransport` 时，`close()` 与 `readN()`/`writeN()` 并发，除非遵循 channel 的停止、shutdown、写序列化和 close 顺序。

锁和发布关系：

- `PendingCallManager::mutex_` 保护 pending map 和 accepting 标志。
- `MyRpcChannel::lifecycle_mutex_` 保护 channel 状态机。
- `MyRpcChannel::send_mutex_` 串行化 socket 写，并协调 IO 停止后的关闭。
- `MyRpcChannel::reader_mutex_` 保护 reader 线程句柄所有权。
- `RpcTransport::fd_mutex_` 保护 fd 替换、shutdown 和 close。
- `RpcChannelPool::lifecycle_mutex_` 保护 pool 状态和活跃提交计数。
- `RpcChannelPool::repair_mutex_` 保护快照 load/store 决策，但不覆盖整个 repair 操作。
- `RpcTimeoutManager` 使用生命周期互斥量、heap 互斥量和条件变量。
- `CallbackExecutor` 和 `ThreadPool` 各自用互斥量和条件变量保护任务队列。

原子假设：

- `MyRpcChannel::running_` 是跨线程 IO 停止信号，store 使用 release，load 使用 acquire。
- `MyRpcChannel::timeout_ms_` 使用 release/acquire 发布配置。
- `MyRpcChannel::next_request_id_` 是原子单调递增请求 id 来源。
- `RpcTransport::fd_` 是原子 fd 快照，生命周期操作仍由 `fd_mutex_` 保护。
- `RpcChannelPool::next_` 使用 relaxed，因为它只是负载分配计数器。
- `RpcChannelPool::channels_snapshot_` 通过显式 acquire/release 的 `shared_ptr` 原子操作发布和读取。

## 完成语义

目标规则：每个已接受的 RPC 调用必须恰好完成一次。

机制：

- `PendingCallManager::take()` 将完成权交给唯一一个路径。
- `PendingCallManager::failAllAndStopAccepting()` 原子地停止接受新 pending call，并把当前 pending call 转移给调用者。
- reader、timeout、stop、connection-loss 和 send-failure 路径只完成自己从 `PendingCallManager` 成功取出的调用。
- `SimpleRpcClosure` 和 `FutureClosure` 在 `Run()` 内删除自身。

callback 执行：

- 普通异步 protobuf callback 通常运行在 `CallbackExecutor`。
- 如果 callback 投递失败，则 inline 运行。
- 同步调用即 `done == nullptr` 时，调用线程阻塞等待 pending call 条件变量。
- future 调用通过完成闭包设置 promise。

重要限制：

- `FutureClosure` 捕获并忽略 `std::future_error`。这能避免 broken promise 路径崩溃，但也可能掩盖意外的重复完成 bug。
- `SimpleRpcController` 是轻量状态容器，不是完整同步的取消机制。
- `StartCancel()` 和 `NotifyOnCancel()` 只是本地 controller 操作，不会传播到 channel、server 或线路协议。

## 超时与资源

RPC 超时行为：

- 默认 RPC 超时为 3000 ms，按 channel 配置。
- 当前实现是在请求写入返回后才注册 timeout。
- 阻塞的 socket 写不受 RPC timeout 覆盖。
- timeout 与正常响应通过 `PendingCallManager::take()` 竞争完成权。
- 已超时调用的迟到响应会被视为找不到 pending call 并忽略。

timeout manager 资源行为：

- 当前没有逐请求取消 timeout 的机制。
- 成功完成的调用仍会在 timeout heap 中留下条目，直到 deadline 到期。
- 高 QPS 且 timeout 很长时，heap 大小可能接近 timeout 窗口内已经完成的调用数。
- `RpcTimeoutManager::stop()` 会停止 worker 线程；遗留 heap 条目会在下一次 `start()` 时清理。
- `RpcTimeoutManager::add()` 当前即使 manager 已停止也会接受条目。

transport 行为：

- connect timeout 默认 1000 ms。
- `setConnectTimeoutMs()` 不校验正数，负值会传给 `poll()`，可能导致无限等待。
- `setTimeoutMs()` 不校验正数，非正 RPC timeout 可能导致立即超时或其他意外行为。
- connect 成功后 socket 为阻塞读写。
- `MyRpcChannel::stop()` 通过 `shutdown()` 唤醒阻塞 IO，再关闭 fd。
- channel 构造路径会安装进程级 `SIGPIPE` ignore handler。

## 错误处理

服务端错误响应覆盖：

- 畸形 frame size
- 超大请求帧
- protobuf header 解析失败
- `request_id == 0`
- `args_size` 不匹配
- service not found
- method not found
- request body 解析失败
- response 序列化失败

客户端错误处理覆盖：

- request 编码失败会早失败
- 发送失败会失败该 channel 上所有 pending call
- 畸形响应帧会失败该 channel
- response body 解析失败会失败当前 call
- timeout 会失败当前 call
- channel stop 会失败 pending call
- pool stop 后的新调用会被拒绝

新增错误路径时必须保持：已接受的异步调用中，`done` 恰好运行一次。

## 已知不够健壮的地方

以下是当前实现中最值得优先加固的点：

- `RpcProvider` 没有优雅停止 API，`Run()` 阻塞后只能依赖底层 event loop 退出或进程结束。
- `RpcChannelPool::stop()` 不是并发 repair 的完整屏障，也不会等待另一个正在执行的 stop 完成。
- 从 callback executor worker 内调用 `RpcChannelPool::stop()` 会触发 `CallbackExecutor::stop()` 的自 join 防护并终止进程。
- RPC timeout 不覆盖阻塞写，只覆盖 `WriteN()` 返回并注册 timer 之后的等待阶段。
- 成功响应不会取消 timeout 条目，高吞吐和长 timeout 下 timeout heap 会增长。
- `SimpleRpcController` 内部没有锁，除了框架完成前、用户观察后的顺序使用外，不应并发读写。
- provider 信任业务 service 恰好调用一次 `done`，框架不防御漏调或重复调用。
- `NotifyService()` 没有空 service 防护，也没有重复注册反馈。
- connect timeout 和 RPC timeout setter 没有输入范围校验。
- 直接使用 `MyRpcChannel` 且 callback executor 为空或已停止时，用户 callback 可能在内部线程 inline 运行。

建议修复顺序：

1. 为 provider 增加 stop API，并明确 server shutdown 所有权。
2. 让 pool stop 成为并发 stop/repair 的真正屏障。
3. 支持从 callback 上请求异步关闭，而不是在 callback worker 上直接 stop executor。
4. 为 timeout manager 增加取消或 generation/table 机制。
5. 将 request timeout 覆盖到写入阶段，或增加独立 send timeout。
6. 校验 timeout 配置输入。
7. 增加 provider 注册校验和重复 service 报告。
8. 增加 callback 中 stop、并发 double stop、repair 期间析构、高 QPS timeout heap 等测试。

## 测试覆盖

单元测试覆盖：

- request frame 编码和 response frame 解码。
- size 和空输入校验。
- pending-call add/take/fail-all/reset 语义。
- pending-call 并发访问。
- thread-pool start/stop/drain/concurrent submit。
- transport 本地连接、配置的连接超时和 fd 清理。
- provider 注册表和 method descriptor 分发。

集成测试覆盖：

- 正常同步和异步 RPC。
- 异步批量 callback 恰好完成一次。
- 并发同步调用。
- pending call 存在时连接丢失。
- RPC timeout 和迟到响应。
- channel start 前调用和 pool stop 后调用。
- channel-pool start/stop、固定连接数、轮询分发、修复、并发 call/repair、并发 repair/stop、double start/stop。
- 坏包、TCP 分片/粘包、service-not-found、超大响应帧。
- future API 成功、timeout、stop、并发和 pool 路径。

并发或生命周期改动合入前，建议至少运行：

```bash
cmake -S . -B build-debug -DCMAKE_BUILD_TYPE=Debug
cmake --build build-debug -j
ctest --test-dir build-debug --output-on-failure

cmake -S . -B build-asan -DCMAKE_BUILD_TYPE=Debug -DMYRPC_ENABLE_ASAN=ON
cmake --build build-asan -j
ctest --test-dir build-asan --output-on-failure

cmake -S . -B build-tsan -DCMAKE_BUILD_TYPE=Debug -DMYRPC_ENABLE_TSAN=ON
cmake --build build-tsan -j
ctest --test-dir build-tsan --output-on-failure
```

## 维护规则

- 优先小范围局部补丁。
- 除非明确迁移协议，不改变线路协议。
- 不要在持有框架内部锁时运行用户 callback。
- callback 所有权必须明确：已接受的异步调用应恰好运行一次 `done`。
- 并发改动需要说明对象所有权、生命周期假设、锁顺序和原子内存序假设。
- 将逻辑停止和对象析构分成两个阶段：先 stop 并 join 外部使用者，再析构对象。
- 每个生命周期或竞态修复都应补聚焦测试，再考虑更大范围重构。
