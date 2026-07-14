# my_rpc 并发设计

本文档描述当前 `my_rpc` 客户端侧并发实现的实际行为，主要覆盖 `RpcChannelPool`、`MyRpcChannel`、`PendingCallManager`、`RpcTimeoutManager` 和 `CallbackExecutor`。

本文档中的“保证”仅指当前代码可推导出的行为；“限制”表示当前实现尚未形成更强契约或仍需补充确定性测试的部分。

## 1. 总体模型

`RpcChannelPool` 是 channel 和 callback executor 的生命周期 owner。一个 pool 持有固定大小的不可变 channel snapshot；调用线程从 snapshot 中选择 channel 并提交 RPC。每个 channel 有自己的 reader 线程、pending-call 表和 timeout manager。

```text
RpcChannelPool
  ├── unique_ptr<CallbackExecutor>
  └── shared_ptr<ChannelList> channels_snapshot_
        ├── shared_ptr<MyRpcChannel>
        │     ├── reader thread
        │     ├── PendingCallManager
        │     └── RpcTimeoutManager
        └── ...
```

`MyRpcChannel` 不拥有 `CallbackExecutor`，只保存一个非拥有的 `CallbackExecutor*`。因此 pool 必须先停止所有 channel，再停止 executor；否则 channel 在完成请求时可能访问已经销毁的 executor。

## 2. 线程与职责

| 线程角色 | 创建者 | 主要工作 | 是否执行用户 `done` |
|---|---|---|---|
| 调用方线程 | 用户代码 | 调用 `RpcChannelPool::CallMethod()` 或 future API | pool 拒绝调用、early error、executor 已停止时可能 inline 执行 |
| channel reader 线程 | `MyRpcChannel::start()` | 读取 response frame、匹配 pending call、处理连接失败 | 正常情况下投递 executor；executor 不可用时 inline |
| timeout worker | `RpcTimeoutManager::start()` | 等待 deadline，通知 channel 某个 request id 超时 | 经 `onRpcTimeout()` 投递 executor；executor 不可用时 inline |
| callback executor worker | `CallbackExecutor::start()` | 顺序执行已投递的用户 closure | 是，通常在此执行 |
| pool owner / stop 线程 | 用户代码 | start、stop、等待短生命周期的 `active_calls_` | channel stop 产生的 callback 通常投递 executor |
| repair 线程 | 用户代码或 stress 测试 | 调用 `repairDeadChannels()` 重建不可用 channel | 可能因 stop 新建或停止 channel，间接触发回调 |

重要结论：`done` 没有单一固定线程亲和性。正常异步完成通常经 `CallbackExecutor` 执行，但 early error 和 executor 已停止时会在当前线程 inline 执行。用户回调不能假定始终运行在同一线程。

## 3. 所有权与生命周期

### 3.1 `RpcChannelPool`

- pool 通过 `channels_snapshot_` 间接持有所有 channel。
- snapshot 以 `shared_ptr<ChannelList>` 原子发布；发布后 vector 不再原地修改。
- `pickChannel()` 先取得 snapshot 的 `shared_ptr`，再复制其中的 `shared_ptr<MyRpcChannel>`。因此即使同时发生 repair 或 stop，当前调用线程拿到的 channel 在 `CallMethod()` 返回前仍然存活。
- `RpcChannelPool::~RpcChannelPool()` 调用 `stop()`。析构前调用者必须停止外部提交线程，避免析构与新的外部 API 调用并发。

### 3.2 `MyRpcChannel`

- channel 由 `MyRpcChannel::create()` 以 `shared_ptr` 创建。
- `MyRpcChannel::start()` 创建 reader 线程时，lambda 捕获 `shared_ptr<MyRpcChannel>`；这保证 reader 运行期间对象不会析构。
- `MyRpcChannel::CallMethod()` 和 `MyRpcChannel::stop()` 也用 `shared_from_this()` 在函数作用域内固定对象生命周期。
- `MyRpcChannel` 不允许在 reader 线程析构；析构时检测到这种情况会 `std::terminate()`。

因此，断连不会立即析构 channel：

```text
reader 发现错误
  -> failFromReaderThread()
  -> markPendingFailed() 使 channel 不可用
  -> pool snapshot 仍持有旧 channel
  -> repair 替换旧 channel，或 pool.stop() 清空 snapshot
  -> 所有临时 shared_ptr 和 reader 自引用释放后
  -> MyRpcChannel 才析构
```

### 3.3 `PendingCall`

`PendingCallManager::pending_` 保存所有“已接受但尚未终止”的请求。一个 `PendingCall` 通过 `shared_ptr` 保存 controller、response 和 done closure 的裸指针；这些外部对象的实际生命周期由调用方式决定：

- 同步调用在 `CallMethod()` 内等待 `call->finished`，调用栈中的 request、response、controller 仍然有效。
- 异步调用要求调用者保证 request、response、controller 在 done 前有效；stress 测试通过 `shared_ptr<CallState>` 捕获它们。
- future API 使用 `FutureCallState` 持有 request、response、controller 和 promise，因此外部 request 不需要继续存活。

这是当前异步 API 的重要边界：普通 callback 风格不会自动拥有调用者传入的 request / response / controller。

## 4. 状态机

### 4.1 `RpcChannelPool`

```text
kStopped --start()--> kRunning --stop()--> kStopping --清理完成--> kStopped
```

| 状态 | 新 `CallMethod()` | `start()` | `stop()` | snapshot | callback executor |
|---|---|---|---|---|---|
| `kStopped` | 直接失败，inline 执行 done | 创建 channel 并发布 snapshot | 直接返回 | 空 | 已停止 |
| `kRunning` | 允许进入 `active_calls_` | 返回 false | 进入 `kStopping` | 非空 | 运行中 |
| `kStopping` | `enterCall()` 拒绝 | 返回 false | 直接返回 | 已摘除 | 仍运行，用于 drain callback |

`start()` 先启动 executor，再逐个连接并启动 channel，最后在 `repair_mutex_` 下发布新 snapshot，并把状态设为 `kRunning`。

`stop()` 的顺序是：

```text
1. 拒绝从 callback executor worker 调用 stop，避免 worker 自己 join 自己。
2. 在 lifecycle mutex 下将 pool 置为 kStopping。
3. 在 repair mutex 下摘除 channels_snapshot_。
4. 在锁外逐个 stop 旧 channel。
5. 等待 active_calls_ == 0。
6. stop callback executor；executor 会 drain 已入队的任务。
7. 将 pool 置为 kStopped。
```

`active_calls_` 只覆盖 `RpcChannelPool::CallMethod()` 从 `enterCall()` 到 `leaveCall()` 的提交过程，不覆盖异步 RPC 从提交到 callback 的完整生命周期。异步请求的收束由随后每个 channel 的 `stop()` 负责。

### 4.2 `MyRpcChannel`

```text
kStopped --start/connect--> kRunning --stop 或 reader failure--> kStopping --清理--> kStopped
```

| 状态 | `isAvailable()` | `pending_` 是否接受新请求 | reader | timeout manager |
|---|---|---|---|---|
| `kStopped` | false | 通常不接受 | 未运行 | 已停止 |
| `kRunning` | true | 接受 | 运行 | 运行 |
| `kStopping` | false | 不接受 | 正在退出或已退出 | 等待 `stop()` 停止 |

`markPendingFailed()` 是连接失败到 repair 的关键桥梁：它先在 `lifecycle_mutex_` 下把 `state_` 从 `kRunning` 改成 `kStopping`，随后将 `running_` 设为 false、`shutdown()` socket、并通过 `pending_.failAllAndStopAccepting()` 取走所有 pending call。

因此 repair 判断的不是 `running_`，而是：

```text
failFromReaderThread()
  -> markPendingFailed()
  -> state_ = kStopping
  -> isAvailable() 返回 state_ == kRunning
  -> repair 识别该 channel 不可用
```

`failFromReaderThread()` 不直接调用完整 `stop()`，因为它本身运行在 reader 线程，不能 join 自己。它会 detach 当前 thread handle、完成 pending call；后续 repair 或 pool stop 从非 reader 线程调用 `stop()`，完成 reader 句柄、socket 和 timeout manager 的最终清理。

## 5. 请求完成模型

### 5.1 唯一完成权

一次 RPC 的完成权由 `PendingCallManager::take(request_id)` 决定：谁先成功从 pending 表中取走 call，谁负责设置结果并运行 done。其他路径取到 `nullptr` 时必须退出，不能再次完成同一 call。

```text
正常 response  -> pending_.take(request_id)
timeout         -> pending_.take(request_id)
断连 / stop    -> pending_.failAllAndStopAccepting()

先取得 call 的路径获胜；其他路径不再执行 done。
```

### 5.2 终止路径

| 路径 | 触发线程 | 如何取得完成权 | controller 结果 | 后续迟到事件 |
|---|---|---|---|---|
| 正常响应 | reader | `pending_.take(id)` | 保持成功状态 | timeout 条目到期后 `take` 失败并忽略 |
| 服务端 RPC 错误 | reader | `pending_.take(id)` | `header.error_text()` | 同上 |
| response body 解码失败 | reader | `pending_.take(id)` | 解码错误 | 同上 |
| timeout | timeout worker | `onRpcTimeout()` 中 `pending_.take(id)` | `rpc call timeout` | 后来的 response 记录 DEBUG 并忽略 |
| 写失败 | 调用方线程 | `markPendingFailed()` 批量取走 | `send rpc error` | reader / timeout 后续发现无 pending 并退出 |
| reader 检测断连 | reader | `markPendingFailed()` 批量取走 | reader 错误原因 | timeout 后续 `take` 失败并忽略 |
| `channel.stop()` | pool owner / repair | `markPendingFailed()` 批量取走 | `stop() is called` | timeout 后续 `take` 失败并忽略 |
| timeout manager 不可用 | 调用方线程 | `pending_.take(id)` | `add timeout manager error` | 无 timeout 条目 |

`RpcTimeoutManager` 不持有 `PendingCall`。它只保存 `request_id + deadline`，到期后回调 `MyRpcChannel::onRpcTimeout(request_id)`；真正取走 call 的仍是 `PendingCallManager`。

### 5.3 done 的执行

`finishCall()` 先把 `call->done` 置空、设置 `call->finished = true` 并通知同步等待者，之后调用 `runDone()`。这样同步等待和异步 callback 都在 pending 所有权已经确定后发生。

`runDone()` 的规则：

```text
CallbackExecutor::post() 成功
  -> 在 callback worker 执行 done。

post() 失败，或 executor 为空
  -> 当前线程 inline 执行 done。
```

因此不得在持有 `PendingCallManager` mutex、channel lifecycle mutex 或 send mutex 时执行用户 done；当前实现中 `take()` / `failAllAndStopAccepting()` 在内部锁中完成后才返回 call，callback 在锁外执行。

## 6. Timeout 语义

当前代码的实际顺序是：

```text
1. pending_.add(request_id, call)
2. timeout_manager_.add(request_id, timeout)
3. 获取 send_mutex_
4. WriteN(request frame)
```

因此 timeout 从成功登记到 `RpcTimeoutManager` 时开始，覆盖等待 `send_mutex_`、socket 写入和服务端处理时间。`include/myrpc/rpc_timeout_manager.h` 中“timeout 不覆盖 blocking write”的旧注释与当前调用顺序不一致，应以代码行为为准，并在后续修改时统一文档和注释。

timeout manager 采用 lazy cleanup：正常完成时不会立即从 timeout heap 删除对应条目；该条目到期后，`onRpcTimeout()` 再次 `pending_.take(id)` 会得到空指针，于是安全忽略。

迟到 response 的处理类似：reader 收到 response 后 `pending_.take(id)` 返回空，记录 DEBUG：该请求已被 timeout、断连或 stop 路径完成。

## 7. 共享状态与同步

### 7.1 Pool

| 同步原语 | 保护内容 | 主要使用位置 |
|---|---|---|
| `lifecycle_mutex_` | `pool_state_`、`active_calls_`、stop 等待条件 | `start()`、`stop()`、`enterCall()`、`leaveCall()` |
| `repair_mutex_` | snapshot 读取、替换和发布过程 | `start()` 发布、`stop()` 摘除、repair |
| 原子 `channels_snapshot_` 操作 | 无锁读取 snapshot 的可见性 | `pickChannel()`、`unavailableCount()` |
| 原子 `next_` | round-robin 选取索引 | `pickChannel()`，`memory_order_relaxed` |

pool 的锁顺序是 `lifecycle_mutex_ -> repair_mutex_`，见 `start()` 和 `stop()`。repair 过程避免在 `repair_mutex_` 内执行网络 connect、channel start 或 channel stop；它先复制 snapshot，在锁外做慢操作，再重新取得 repair mutex 比较并尝试发布。

### 7.2 Channel

| 同步原语 | 保护内容 | 主要使用位置 |
|---|---|---|
| `lifecycle_mutex_` | `state_` 状态转换 | `start()`、`markPendingFailed()`、`stop()`、`isAvailable()` |
| 原子 `running_` | reader / transport I/O 是否继续 | reader 循环、`ReadN()`、`WriteN()`、失败路径 |
| `send_mutex_` | 写 socket 与最终 close 的互斥 | `CallMethod()`、`cleanupStoppedConnection()` |
| `reader_mutex_` | `reader_thread_` 的 move、join、detach、thread id | start、join、reader failure |
| `error_mutex_` | `last_error_` | `setLastError()`、`LastError()` |
| `PendingCallManager::mutex_` | `accepting_` 和 pending map | add、take、failAll |
| `RpcTimeoutManager::mutex_` | timeout heap 和 worker wait | add、loop、stop |

fd 关闭顺序由 `cleanupStoppedConnection()` 约束：先让 `running_ = false` 并 `shutdown()` 唤醒阻塞 I/O，再 join reader，之后取得 `send_mutex_` 并 close socket。因此 close 不应与正在进行的 `ReadN()` 或 `WriteN()` 并发。

### 7.3 内存序

- `running_` 使用 release store / acquire load，发布“reader 和 transport 应退出”的事实。
- timeout manager 的 `running_` 同样使用 release store / acquire load；heap 仍由 mutex 保护。
- `channels_snapshot_` 用 release store / acquire load 发布不可变 snapshot。
- `next_` 只用于索引分配，没有发布其他数据，使用 relaxed 合理。

## 8. 关键竞态

### 8.1 Submit vs Stop

```text
提交线程                                stop 线程
----------                              ----------
enterCall(): active_calls_++
                                      pool_state_ = kStopping
                                      摘除 snapshot
pickChannel() 持有 local shared_ptr
channel.CallMethod()
leaveCall(): active_calls_--
                                      等待 active_calls_ == 0
                                      stop channels，再 stop executor
```

`active_calls_` 确保 stop 不会在 pool 的提交函数仍在执行时停止 executor。channel 的局部 `shared_ptr` 和 `CallMethod()` 内的 `self` 确保 channel 不会在提交中析构。

提交恰好与 stop 竞争时有两类结果：

- 提交在线性化点 `enterCall()` 前后看到 pool 非 `kRunning`：controller 失败为 `RpcChannel is stopped`，done inline 执行。
- 提交已经进入 pool：它会拿到一个 channel 并继续；随后 channel stop 可能使该请求以 stop、写失败或连接失败完成。

### 8.2 Response vs Timeout

```text
reader 线程                             timeout worker
-----------                             --------------
pending_.take(request_id)               pending_.take(request_id)
  成功 -> response 路径完成               成功 -> timeout 路径完成
  失败 -> 已由其他路径完成                 失败 -> 已由其他路径完成
```

两条路径共享的线性化点是 `PendingCallManager::take()` 内部 mutex。不会出现两条路径都拿到同一个 `PendingCall`，因此不会因 response / timeout 竞争而重复执行 done。

### 8.3 Repair vs Stop

repair 读取旧 snapshot 后，在锁外创建 replacement channel；发布前再次读取当前 snapshot：

```text
repair 读取 old_snapshot
stop 将 channels_snapshot_ 置空
repair 再次读取 now_snapshot
old_snapshot != now_snapshot
  -> repair 不发布 replacement，停止自己新建的 channel
```

反过来，若 repair 先成功发布 replacement，随后 stop 会读取包含 replacement 的当前 snapshot，并停止其中所有 channel。两条路径依赖 snapshot 指针比较来避免 stop 后重新发布旧视图。

限制：`repairDeadChannels()` 不等待 stop 完成，且没有独立的“repair in progress”计数。当前语义依赖 snapshot 比较保证不发布过期结果；应由 `submit_stop_race_stress` 和 TSAN 持续验证。

### 8.4 Reader Failure vs Repair

```text
reader failure
  -> markPendingFailed()
  -> state_ = kStopping
  -> isAvailable() 变为 false
  -> repairDeadChannels() 创建 replacement
  -> 发布新 snapshot 后 stop old channel
  -> old channel 停止 timeout manager 并进入 kStopped
```

reader failure 本身不执行完整 `stop()`，因为 reader 不能 join 自己。若既不 repair 也不 pool stop，失败 channel 仍被 pool snapshot 持有，并且其 timeout manager 不会由失败路径直接停止；这是当前实现应记录的生命周期限制。

## 9. CallbackExecutor

`CallbackExecutor` 是单 worker 队列：

- `start()` 创建 worker，并等待其进入循环。
- `post()` 在 `started_ && !stopping_` 时入队。
- `stop()` 先设置 `stopping_`，随后 worker 继续处理已入队任务，直到 queue 为空才退出。
- 在 callback worker 自身调用 `stop()` 会被拒绝，避免 self-join。

pool 的 `stop()` 也拒绝从 callback worker 调用，因为它最终会停止 executor。用户回调若需要停止客户端，应通知外部 owner 线程，而不是直接在 callback 内调用 `pool.stop()`。

## 10. 错误、日志与指标

| 事件 | 调用方可见结果 | 当前日志行为 | 建议指标 |
|---|---|---|---|
| timeout | controller 失败：`rpc call timeout` | timeout 本身无必然 WARN | timeout 数 |
| 迟到响应 | 已完成的 call 不再改变 | DEBUG：忽略迟到或已完成 response | late response 数 |
| 写失败 | 所有当前 pending 失败：`send rpc error` | 依赖 channel 错误日志 | send failure 数 |
| reader 失败 | 当前 pending 以 reader 错误完成 | 依赖 reader 错误日志 | connection loss 数 |
| pool 已停止 | controller 失败：`RpcChannel is stopped` | 无专门日志 | rejected submit 数 |
| 无可用 channel | controller 失败：`no available rpc channel` | 无专门日志 | unavailable submit 数 |

压力测试不要只依赖日志。至少应断言：`submitted == completed`、最终 inflight 为零、`duplicate_done == 0`、成功与失败分类之和等于完成数，以及预期的 timeout / repair / disconnect 计数非零。

## 11. 测试证据

| 风险 | 主要测试 | 当前关键断言 |
|---|---|---|
| 长时间异步提交 | `tests/stress/async_sustained_stress.cc` | 提交与完成计数、inflight 上限、无重复 done |
| 断连和 replacement | `tests/stress/disconnect_repair_stress.cc` | 请求收束、断连和 repair 调用计数 |
| submit / stop / repair 竞争 | `tests/stress/submit_stop_race_stress.cc` | 多轮 start/stop、无重复 done、drain |
| timeout、近 timeout、迟到 response | `tests/stress/timeout_stress.cc` | 正常成功、late timeout、服务端完成全部 response、无重复 done |
| timeout manager 基础行为 | `tests/unit/rpc_timeout_manager_test.cc` | start/stop、heap 清理、到期回调、并发 start/stop |
| channel / pool 集成路径 | `tests/integration/rpc_channel_timeout_test.cc`、`tests/integration/rpc_channel_pool_test.cc` | 端到端 timeout、连接池和 stop 行为 |

推荐验证顺序：

```bash
# 单个变更后的最小目标
cmake --build build-debug --target <target> -j
ctest --test-dir build-debug -R '^<test_name>$' --output-on-failure

# 并发生命周期改动完成后
ctest --test-dir build-debug --output-on-failure
ctest --test-dir build-asan --output-on-failure
ctest --test-dir build-tsan --output-on-failure
```

## 12. 当前限制与后续改进

1. `RpcTimeoutManager` 头文件注释与实际注册 timeout 的顺序不一致。应统一 timeout 是否覆盖等待 send mutex 和阻塞写的正式契约。
2. 普通异步 callback API 不拥有调用方传入的 request、response 和 controller；API 文档需要明确生命周期要求，或改为内部持有状态。
3. reader failure 后 channel 进入 `kStopping`，但 timeout manager 的停止依赖后续 repair 或 pool stop；可考虑把失败收束定义得更完整，或明确这是有意的延迟清理。
4. `repairDeadChannels()` 与 `stop()` 的正确性主要依赖 snapshot 比较；应补充带屏障的确定性竞态测试，而不只依赖时间驱动 stress。
5. 迟到响应、重复响应和从未注册的 request id 当前都可能落入“pending 不存在”的分支；如需更强可观测性，应在 pending manager 中保留有界终态记录，区分 timeout、断连、正常完成和未知 id。
6. 默认 CTest 的 duration 与 CTest timeout 必须匹配。对于按时间运行的 stress，满足 `CTest TIMEOUT > duration + drain + cleanup`；对于大 `max_requests`，应单独估算 `max_inflight / timeout` 给出的最低运行时间。

## 13. 并发修改检查清单

- [ ] 明确新对象的 owner、引用类型和销毁线程。
- [ ] 明确每个新线程的退出条件、join 或 detach 规则。
- [ ] 为每个共享字段指定 mutex、atomic 或单线程所有权。
- [ ] 写出锁顺序，并检查没有反向获取路径。
- [ ] 不在持有内部 mutex 时执行用户 callback、阻塞 I/O 或 join。
- [ ] 每个终止路径都通过 `take` 或等价机制保证 done 恰好一次。
- [ ] 明确 timeout 从何时开始，并同步代码、注释和测试。
- [ ] 为新竞态至少添加一个确定性测试或压力测试。
- [ ] 运行最小 Debug 测试；涉及生命周期或跨线程访问时运行 ASAN 和 TSAN。
