# my_rpc 测试指南

本文档说明当前测试体系验证什么、如何选择测试命令，以及并发修改应补在哪一层。测试目录与构建开关以当前 CMake 配置为准。

## 1. 测试原则

客户端 RPC 的正确性不能只由“没有崩溃”证明。每个已接受请求至少应满足：

```text
submitted == completed
inflight == 0
duplicate_done == 0
success + failed == completed
```

其中 response、timeout 和 timeout 注册失败通过 `PendingCallManager::take(request_id)` 取得单请求完成权；写失败、reader 断连和 stop 通过 `failAllAndStopAccepting()` 批量转移当前 pending call。测试应验证：无论是单个取出还是批量转移，同一个 call 都只会被一个终止路径完成。

时间驱动的压力测试适合扩大调度空间，但不能保证某个交错发生。涉及完成权、对象析构或 snapshot 发布的竞态，应优先补充 hook/barrier 驱动的确定性测试。

完整的线程、所有权和停止模型见 [concurrency.md](concurrency.md)。

## 2. 测试分层

| 层级 | 目录 | CTest 标签 | 目标 |
| --- | --- | --- | --- |
| Unit | `tests/unit/` | `unit` | 单个组件的状态、边界值和并发基础语义 |
| Integration | `tests/integration/` | `integration` | 客户端、受控 TCP 服务端和 Protobuf 服务之间的端到端行为 |
| Deterministic race | `tests/integration/*_race_test.cc` | `race` | 用 hook 固定关键线程交错，验证竞争下的唯一完成与生命周期 |
| Stress | `tests/stress/` | `stress` | 在长时间、高并发和故障注入下检查收束、计数与资源边界 |

`unit` 和 `integration` 由 `MYRPC_BUILD_TESTS` 控制；`race` 由 `MYRPC_BUILD_RACE_TESTS` 和 `MYRPC_ENABLE_TEST_HOOKS` 共同控制；`stress` 由 `MYRPC_BUILD_STRESS_TESTS` 控制。

## 3. 构建与运行

### 3.1 Debug：unit 与 integration

日常修改首先运行最小相关测试；不确定影响范围时运行两个标签。

```bash
cmake -S . -B build-debug \
  -DCMAKE_BUILD_TYPE=Debug \
  -DMYRPC_BUILD_TESTS=ON
cmake --build build-debug -j

ctest --test-dir build-debug -L unit --output-on-failure
ctest --test-dir build-debug -L integration --output-on-failure
```

运行单个已发现的 GTest：

```bash
ctest --test-dir build-debug \
  -R 'RpcTimeoutManagerTest\.ExpiredTimeoutsShouldInvokeCallbackOnce' \
  --output-on-failure
```

查看当前配置实际发现的测试与标签：

```bash
ctest --test-dir build-debug -N
ctest --test-dir build-debug -N -L integration
```

### 3.2 ASAN：确定性竞态与内存安全

ASAN 用于检测 use-after-free、越界和泄漏。race tests 需要编译测试 hook；仅打开 `MYRPC_BUILD_RACE_TESTS` 不足以启用这些交错点。`MYRPC_ENABLE_TEST_HOOKS` 是默认 `OFF` 的正式 CMake option；传入 `-DMYRPC_ENABLE_TEST_HOOKS=ON` 即可启用。

```bash
cmake -S . -B build-asan \
  -DCMAKE_BUILD_TYPE=Debug \
  -DMYRPC_BUILD_EXAMPLES=OFF \
  -DMYRPC_BUILD_RACE_TESTS=ON \
  -DMYRPC_ENABLE_TEST_HOOKS=ON \
  -DMYRPC_ENABLE_ASAN=ON
cmake --build build-asan -j

ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 \
ctest --test-dir build-asan -L race --output-on-failure
```

### 3.3 TSAN：确定性竞态与数据竞争

TSAN 重点检查未同步共享访问和部分死锁问题。它不能证明逻辑上的 `done` 恰好一次，因此仍需要测试中的计数断言。

```bash
cmake -S . -B build-tsan \
  -DCMAKE_BUILD_TYPE=Debug \
  -DMYRPC_BUILD_EXAMPLES=OFF \
  -DMYRPC_BUILD_RACE_TESTS=ON \
  -DMYRPC_ENABLE_TEST_HOOKS=ON \
  -DMYRPC_ENABLE_TSAN=ON
cmake --build build-tsan -j

TSAN_OPTIONS=halt_on_error=1:second_deadlock_stack=1 \
ctest --test-dir build-tsan -L race --output-on-failure
```

部分 Linux 本地环境会因较高的地址随机化设置，在 TSAN 启动时报告 `unexpected memory mapping`。运行前可由具备管理员权限的用户执行：

```bash
sudo sysctl -w vm.mmap_rnd_bits=28
```

该设置修改主机的地址随机化策略，应仅用于本地 TSAN 验证并按团队环境要求恢复。GitHub Actions CI 当前可以正常执行 TSAN race tests。

ASAN 与 TSAN 互斥。不要在同一个构建目录中切换 sanitizer；使用 `build-asan` 和 `build-tsan` 这样的独立目录。

### 3.4 Stress：长时间与故障注入

```bash
cmake -S . -B build-stress \
  -DCMAKE_BUILD_TYPE=Debug \
  -DMYRPC_BUILD_STRESS_TESTS=ON
cmake --build build-stress -j

ctest --test-dir build-stress -L stress --output-on-failure --parallel 1
```

stress 测试会创建自己的受控服务端，不依赖示例 provider。默认 CTest 参数定义在 [`tests/stress/CMakeLists.txt`](../tests/stress/CMakeLists.txt)。默认配置中两个持续压力测试运行 300 秒；`submit_stop_race_stress` 和 `timeout_stress` 的 CTest timeout 为 90 秒。

需要缩短或放大某个 stress 场景时，直接运行生成的可执行文件并传入该程序支持的命名参数，例如：

```bash
./build-stress/tests/stress/timeout_stress \
  --duration-seconds=60 \
  --pool_size=16 \
  --client_threads=4 \
  --max-inflight=16 \
  --timeout-ms=40 \
  --max-requests=3000
```

修改 `max_requests`、`max-inflight` 或 timeout 后，必须同步评估执行时长。大量 timeout 请求的保守下界可近似为：

```text
throughput upper bound ~= max_inflight / timeout_seconds
minimum duration ~= max_requests / throughput upper bound
```

CTest 的 `TIMEOUT` 必须大于测试运行时间、drain 时间和服务端清理余量之和。

## 4. 覆盖矩阵

### 4.1 Unit

| 组件 | 主要验证 |
| --- | --- |
| `rpc_codec_test` | 请求编码、响应解析、frame/header 大小和 body size 校验 |
| `pending_call_manager_test` | add/take、重复 id、停止接收、fail-all、重启和并发访问 |
| `rpc_timeout_manager_test` | start/stop、停止后拒绝 add、heap 清理、负 timeout、到期回调和重启 |
| `callback_executor_test` | worker 内调用 stop 不触发 self-join |
| `threadpool_test` | start/stop、drain、并发 submit |
| `rpc_transport_test` | 本地 connect、connect timeout 与 fd 清理 |
| `rpc_provider_test` | service 注册、method 分发、非法帧和 size 校验 |

### 4.2 Integration

| 场景 | 代表测试 | 关键断言 |
| --- | --- | --- |
| 正常同步/异步 | `normal_async`、`batch_async`、`request1000_test` | response 正确，全部 callback 完成一次 |
| 多线程提交 | `multithread_call` | 所有同步调用返回 |
| 未启动或连接断开 | `not_connection`、`connection_lost` | 不阻塞，controller 失败，done once |
| timeout | `rpc_channel_timeout_test` | 同步返回、异步收束、正常 response 不因旧 timer 再完成 |
| Future | `future_test` | 成功、timeout、stop、并发及 pool 路径的 ready 结果 |
| channel pool | `rpc_channel_pool_test` | 固定连接数、轮询、repair、stop、inflight drain、double start/stop |
| 网络与协议 | `bad_packet_test`、`test_tcp_fragment`、`service_not_found*` | 坏包拒绝、分片/粘包、错误响应与 request id |

### 4.3 确定性竞态

| 交错 | 测试 | 固定点 | 验证 |
| --- | --- | --- | --- |
| pending add vs stop | `RpcChannelRaceTest.StopAfterPendingAcceptShouldCompleteCallExactlyOnce` | `pending_.add()` 成功后暂停 | 请求只完成一次，timeout 注册失败不再次完成，pending 收束 |
| response vs timeout | `RpcChannelRaceTest.TimeoutAndResponseRaceShouldCompleteExactlyOnce` | reader 在 `pending_.take()` 前暂停 | timeout 或 response 只有一方取得完成权，controller 不被覆盖，done once |
| repair publish vs stop | `RpcChannelPoolRaceTest.RepairPausedBeforePublishThenStopShouldNotRepublishChannel` | replacement 创建后、snapshot 发布前暂停 | stop 后不发布 replacement，replacement 被停止 |

这些测试只在 `MYRPC_ENABLE_TEST_HOOKS=ON` 的构建中存在。hook 是测试同步点，不是生产控制接口；不要把业务逻辑建立在 hook 上。

### 4.4 Stress

| 程序 | 默认 CTest 参数 | 故障/负载 | 关键断言 |
| --- | --- | --- | --- |
| `async_sustained_stress` | 300 s，4 channels，4 submitters，1000 inflight | 持续异步提交 | 请求收束、inflight 上限、done once |
| `disconnect_repair_stress` | 300 s，100 ms 断连间隔 | 主动断连与 repair | 请求收束、断连发生、repair 尝试非零、done once |
| `submit_stop_race_stress` | 20 轮，30 s 上限 | 循环 start/stop、提交、断连与 repair | 每轮 drain、无重复 done、无悬挂请求 |
| `timeout_stress` | 3000 requests，40 ms timeout，16 inflight | fast、near-timeout、late response | 各类结果出现、服务端完成 response、done once |

`repair_attempt_count` 表示调用了 repair 尝试，并不等价于“replacement 已成功发布”。如果需要验证 repair 成功，测试应通过后续请求恢复或新增受控的成功指标证明。

## 5. 结果判读

### 必须失败的信号

- `submitted != completed`：请求丢失、回调悬挂，或测试自身计数边界错误。
- `inflight != 0`：仍有未收束的请求或 drain 条件不正确。
- `duplicate_done != 0`：至少两条终止路径执行了同一 callback，是最高优先级问题。
- `success + failed != completed`：结果分类遗漏，或完成路径没有统一计数。
- sanitizer 报告：即使测试退出码为零，也必须先处理 ASAN/TSAN 报告。

### 预期但需要解释的现象

- timeout 数量高：可能是服务端队头阻塞、timeout 配置过短或性能退化；不能仅按服务端请求类别比例推断成功比例。
- DEBUG 的 missing-pending response：可能是 timeout/断连/stop 后的迟到 response，也可能是重复或未知 id；当前实现没有终态历史来区分。
- Future 或 callback 在非 executor worker 上运行：early error 或 executor 拒绝投递时，框架会 inline 执行 `done`。

## 6. CI

当前 [CI workflow](../.github/workflows/ci.yml) 执行三组检查：

| Job | 构建 | 执行 |
| --- | --- | --- |
| `debug` | Debug + `MYRPC_BUILD_TESTS=ON` | `unit` 与 `integration` |
| `asan-race` | ASAN + race tests + hooks | `race` |
| `tsan-race` | TSAN + race tests + hooks | `race` |

长时间 stress 不在默认 CI 中运行，避免使每次提交的反馈时间和环境波动过大。它应在本地、定时任务或发布前验证中串行执行。

## 7. 新测试准入标准

### 修改普通逻辑

1. 为新分支补 unit 或 integration 测试。
2. 运行最小相关测试；跨模块改动运行完整 `unit` 与 `integration` 标签。
3. 若触及序列化、错误码或协议边界，覆盖成功和失败输入。

### 修改并发、生命周期或错误收束

1. 写清对象 owner、外部生命周期前提、锁顺序和 atomic memory order。
2. 为每条终止路径检查 `done` 是否恰好一次。
3. 对可以人为暂停的关键交错，添加 hook/barrier 确定性测试；不要只依赖 sleep。
4. 运行相关 Debug 测试，以及 ASAN/TSAN race tests。
5. 当改动影响请求收束、timeout 或 repair 时，增加或运行相应 stress 场景。

### 选择测试层级

| 问题类型 | 首选测试 |
| --- | --- |
| 参数、编码、容器状态 | unit |
| 一次 RPC 的网络可见行为 | integration |
| 两条线程必须以指定顺序交错 | deterministic race |
| 高负载、频繁断连或长期资源边界 | stress |

## 8. 编写测试的约束

- 受控服务端应由测试自身拥有和回收，避免依赖外部 `provider` 进程。
- 等待异步结果使用 predicate + 条件变量、latch 或 barrier；避免用固定 sleep 作为正确性条件。
- 计数在同一同步域中更新并快照，避免测试本身引入 data race。
- 测试结束前显式 drain pending callback，并停止 pool/channel/服务端线程。
- 断言可观察的行为，不断言偶然的线程调度顺序。
- 新增 CTest 目标时选择一个主标签；当前标签为 `unit`、`integration`、`race` 与 `stress`。
