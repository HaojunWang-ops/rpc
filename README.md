# my_rpc

`my_rpc` 是一个基于 Protobuf generic service 和内置 `mini_muduo` reactor 的 C++17 RPC 框架。项目关注一个可读、可测试的客户端并发模型：连接池、异步完成、超时、断连、repair 与 stop 可以并发发生，但每个已接受的 RPC 只能完成一次。

它适合学习 RPC 协议、连接生命周期和 C++ 并发资源管理；当前不是生产级 RPC runtime。

## 功能

- Protobuf `RpcChannel` 客户端和 generic service 服务端。
- 持久 TCP 连接与固定大小 `RpcChannelPool`。
- 同步调用、Protobuf callback 异步调用和 Future API。
- 每个 channel 的 reader 线程、pending-call 表与 timeout manager。
- 请求超时、写失败、断连、坏包和 pool/channel stop 的错误收束。
- 不可变 channel snapshot 与失效连接 repair。
- 单元、集成、确定性竞态和压力测试。

## 依赖

- CMake 3.16+
- 支持 C++17 的编译器
- Protobuf 开发包与 `protoc`
- POSIX threads
- GoogleTest，仅在构建测试时需要

Ubuntu 示例：

```bash
sudo apt-get update
sudo apt-get install -y cmake g++ libprotobuf-dev protobuf-compiler libgtest-dev libboost-dev
```

## 快速开始

默认构建会生成示例程序：

```bash
cmake -S . -B build-debug -DCMAKE_BUILD_TYPE=Debug
cmake --build build-debug -j
```

启动服务端：

```bash
./build-debug/examples/provider
```

在另一终端运行客户端示例：

```bash
./build-debug/examples/consumer
```

示例服务定义在 [`proto/user.proto`](proto/user.proto)，服务实现位于 [`services/user_services.cc`](services/user_services.cc)。provider 默认监听 `0.0.0.0:8000`，consumer 默认连接 `127.0.0.1:8000`。

## 基本用法

### 服务端

`RpcProvider` 不拥有已注册的 service，因此 service 对象必须存活到 `Run()` 结束。`Run()` 会进入事件循环并阻塞；当前没有公开的 provider stop API。

```cpp
RpcProvider provider(4);
UserServiceImpl service;

provider.NotifyService(&service);
provider.Run("0.0.0.0", 8000);
```

业务 service 必须对每个请求恰好调用一次 `done->Run()`。

### 同步客户端调用

`RpcChannelPool` 是 client channel 与 callback executor 的生命周期 owner。先 `start()`，再把 pool 传给 Protobuf 生成的 stub。

```cpp
RpcChannelPool pool("127.0.0.1", 8000, 4);
if (!pool.start()) {
    // 连接或启动失败
    return;
}

demo::UserService_Stub stub(&pool);
demo::LoginRequest request;
demo::LoginResponse response;
SimpleRpcController controller;

request.set_name("haojun");
request.set_password("123456");
stub.Login(&controller, &request, &response, nullptr);

if (controller.Failed()) {
    // controller.ErrorText()
} else {
    // 使用 response
}

pool.stop();
```

同步调用在 `CallMethod()` 返回前等待终态，因此栈上的 `request`、`response` 与 `controller` 可以安全使用。

### 异步 callback 调用

普通 callback API 不接管调用方对象的所有权：

- `request` 在 `CallMethod()` 中同步编码，只需存活到该函数返回；
- `response`、`controller` 和 `done` 必须存活到 `done->Run()` 返回；
- 正常回调通常在 pool 的 callback executor worker 上运行，但在 early error 或 executor 已停止时可能在当前完成线程 inline 运行。

因此异步调用应将结果状态与 closure 放入能覆盖回调期的对象中，例如 `shared_ptr<CallState>`。不要在 RPC 尚未完成时跨线程读取、修改或复用同一个 `SimpleRpcController`。

### Future API

Future API 复制 request，并由内部 `FutureCallState` 持有 request、response、controller、promise 和完成 closure：

```cpp
auto future = pool.CallMethodFuture<demo::LoginResponse>(
    demo::UserService::descriptor()->FindMethodByName("Login"),
    request);

auto result = future.get();
if (!result.ok) {
    // result.error_code, result.error_text
} else {
    // result.response
}
```

销毁返回的 `future` 不会取消已经提交的 RPC；请求仍会正常完成、超时，或在 channel/pool 停止时失败。

## 客户端并发与停止语义

每个 channel 维护 pending-call 表。response、timeout 和 单请求提交失败 都通过 `PendingCallManager::take(request_id)` 取得单个请求的完成权；写失败、reader 失败和 channel stop 通过 `failAllAndStopAccepting()`停止接受新请求，并批量转移当前 pending calls。无论是单个取出还是批量转移，只有成功取走 `PendingCall` 的路径可以修改结果并执行 `done`，因此已接受请求的目标语义是“完成恰好一次”。

pool 通过不可变的 `shared_ptr` snapshot 发布 channel 集合。调用方先取得局部 snapshot/channel 引用，再提交调用；repair 或 stop 替换全局 snapshot 时，不会让已取得局部引用的 channel 立即析构。

推荐关闭顺序：

```text
停止新的外部提交和 repair
  -> join 调用方/repair 线程
  -> 从非 callback worker 的线程调用 pool.stop()
  -> 销毁 pool
```

`RpcChannelPool::stop()` 会摘除已发布的 snapshot，停止旧 channel，等待正在执行的 pool 提交函数退出，再 drain 并停止 callback executor。停止后的新提交会以错误完成，`done` 可能 inline 执行。

不要在 callback executor worker 中调用 `pool.stop()`；实现会拒绝该路径以避免 worker self-join。pool 析构也不能与外部线程继续调用 `CallMethod()`、`CallMethodFuture()`、`repairDeadChannels()`、`start()` 或 `stop()` 并发。

完整的对象所有权、状态机、锁顺序和确定性竞态说明见 [`docs/concurrency.md`](docs/concurrency.md)。

## 超时、错误与日志

默认 RPC timeout 为 3000 ms，可通过 `RpcChannelPool::setTimeoutMs()` 配置。当前实现要求在 pool 停止时、且不与 `start()`、`repairDeadChannels()` 或提交调用并发时设置该值；它会用于后续由 `start()` 或 repair 创建的 channel，不会更新已经运行的 channel。timeout 在请求加入 pending 表后、获取 `send_mutex_` 和执行 socket 写之前注册，因此 deadline 包含发送锁等待、socket 写入和服务端响应时间。

该 timeout 只决定请求完成权，不保证取消或中断正在阻塞的`WriteN()`。因此 timeout 路径可能已经完成请求并执行`done`，而原提交线程仍暂时停留在发送操作中；连接 shutdown、写失败或系统调用返回后，提交线程才会继续退出。

timeout manager 不会在正常 response 到达时逐条删除 deadline；对应 heap 条目会保留到 deadline 后再被弹出。高 QPS 且 timeout 很长时，应评估这一窗口带来的 heap 占用。

当前 response 找不到 pending call 时会以 DEBUG 忽略。该分支无法区分以下情形：

- timeout、断连或 stop 后的迟到 response；
- 重复 response；
- 从未注册过的 request id。

因此日志只适合作为诊断线索，正确性应由完成计数、`pending` 收束和 `done` 恰好一次等断言验证。

## 构建选项

| 选项 | 默认值 | 作用 |
| --- | --- | --- |
| `MYRPC_BUILD_EXAMPLES` | `ON` | 构建 provider/consumer 示例 |
| `MYRPC_BUILD_TESTS` | `OFF` | 构建 unit 与 integration 测试 |
| `MYRPC_BUILD_RACE_TESTS` | `OFF` | 构建 hook 驱动的确定性竞态测试 |
| `MYRPC_BUILD_STRESS_TESTS` | `OFF` | 构建长时间压力测试 |
| `MYRPC_BUILD_BENCHMARKS` | `OFF` | 构建 benchmark |
| `MYRPC_ENABLE_ASAN` | `OFF` | 启用 AddressSanitizer |
| `MYRPC_ENABLE_TSAN` | `OFF` | 启用 ThreadSanitizer |
| `MYRPC_ENABLE_TEST_HOOKS` | `OFF` | 为 race tests 编译测试 hook |

ASAN 与 TSAN 互斥。不要在已用另一种 sanitizer 配置过的构建目录上直接切换，使用独立构建目录。

## 测试

### Debug 单元与集成测试

```bash
cmake -S . -B build-debug \
  -DCMAKE_BUILD_TYPE=Debug \
  -DMYRPC_BUILD_TESTS=ON
cmake --build build-debug -j
ctest --test-dir build-debug -L unit --output-on-failure
ctest --test-dir build-debug -L integration --output-on-failure
```

### ASAN / TSAN 确定性竞态测试

这些测试通过 hook 固定关键交错，覆盖 pending add 与 stop、response 与 timeout、repair snapshot 发布与 stop 的竞争。

```bash
cmake -S . -B build-asan \
  -DCMAKE_BUILD_TYPE=Debug \
  -DMYRPC_BUILD_RACE_TESTS=ON \
  -DMYRPC_ENABLE_TEST_HOOKS=ON \
  -DMYRPC_ENABLE_ASAN=ON
cmake --build build-asan -j
ctest --test-dir build-asan -L race --output-on-failure
```

```bash
cmake -S . -B build-tsan \
  -DCMAKE_BUILD_TYPE=Debug \
  -DMYRPC_BUILD_RACE_TESTS=ON \
  -DMYRPC_ENABLE_TEST_HOOKS=ON \
  -DMYRPC_ENABLE_TSAN=ON
cmake --build build-tsan -j
ctest --test-dir build-tsan -L race --output-on-failure
```

### Stress 测试

```bash
cmake -S . -B build-stress \
  -DCMAKE_BUILD_TYPE=Debug \
  -DMYRPC_BUILD_STRESS_TESTS=ON
cmake --build build-stress -j
ctest --test-dir build-stress -L stress --output-on-failure --parallel 1
```

压力测试包括持续异步提交、断连 repair、submit/stop/repair 竞争，以及 timeout、近超时和迟到 response。它们的默认参数和 CTest timeout 在 [`tests/stress/CMakeLists.txt`](tests/stress/CMakeLists.txt) 中定义；大规模 `max_requests` 运行前应先估算 duration 并相应调大 CTest timeout。

## 协议

请求和响应均采用长度前缀帧：

```text
uint32 total_size, network byte order
uint32 header_size, network byte order
protobuf header
protobuf body
```

`total_size` 不包含它自身，包含其后的 `header_size`、header 与 body。协议定义在 [`src/rpc_header.proto`](src/rpc_header.proto)，当前 client/server 对 frame 总大小限制为 64 MiB、header 限制为 1 MiB。除非进行明确的协议迁移，不应修改该布局。

## Benchmark

使用 Release 构建 benchmark，并先启动 provider：

```bash
cmake -S . -B build-benchmark \
  -DCMAKE_BUILD_TYPE=Release \
  -DMYRPC_BUILD_BENCHMARKS=ON
cmake --build build-benchmark -j

./build-benchmark/examples/provider warn
./build-benchmark/benchmarks/rpc_sync_single_benchmark 127.0.0.1 8000 10000 1000
```

可用 benchmark 覆盖同步单连接、连接池大小、客户端线程数、异步 callback、Future 与 payload 大小。所有 benchmark 都是客户端程序，默认连接 `127.0.0.1:8000`。

## 当前限制

- `RpcProvider` 没有公开的优雅 stop API。
- pool `stop()` 不等待并发的 `repairDeadChannels()` 调用完全退出，也不会等待另一个正在执行的 `stop()` 完成。
- Future 与 callback API 没有显式取消语义。
- `SimpleRpcController` 不是可并发读写的同步对象。
- timeout heap 使用 lazy cleanup，正常完成的请求不会立即移除 timeout 项。
- 未知、重复和迟到 response 当前共用同一 DEBUG 分支，尚未提供精确分类指标。
- 运行中的 pool 不支持动态修改 timeout 配置；应在下一次 `start()` 前设置。

更多设计取舍、已知限制和修改检查清单见 [`docs/concurrency.md`](docs/concurrency.md)。

## 目录

- [`include/myrpc/`](include/myrpc/)：公共 API。
- [`src/`](src/)：客户端、服务端、协议与内部实现。
- [`examples/`](examples/)：provider 与 consumer。
- [`services/`](services/)：示例 service 实现。
- [`tests/`](tests/)：unit、integration、race 与 stress 测试。
- [`benchmarks/`](benchmarks/)：客户端 benchmark。
- [`docs/`](docs/)：并发模型、测试与设计文档。
- [`third_party/mini_muduo/`](third_party/mini_muduo/)：内置 reactor/network 实现。
