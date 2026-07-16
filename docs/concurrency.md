# my_rpc 客户端并发模型与生命周期设计

> 本文档描述当前 `my_rpc` 客户端实现的并发模型、生命周期边界和可验证行为。  
> 覆盖组件包括 `RpcChannelPool`、`MyRpcChannel`、`PendingCallManager`、`RpcTimeoutManager` 与 `CallbackExecutor`。
>
> 文中的“保证”仅表示能够由当前实现及测试证据推导出的行为；“限制”表示当前实现未提供更强契约，或仍需要确定性并发测试进一步验证的部分。

---

## 1. 设计目标与核心结论

客户端并发设计主要解决以下问题：

1. 多线程调用方能够安全地共享连接池并提交 RPC。
2. response、timeout、断连和 stop 等终止路径竞争时，一个请求只完成一次。
3. pool、channel、reader、timeout worker 和 callback executor 之间具有明确的所有权与停止顺序。
4. repair 与 stop 并发时，不允许在 pool 停止后重新发布 replacement channel。
5. 用户回调不得在持有框架内部互斥锁时执行。

当前实现提供以下核心保证：

- `PendingCallManager::take(request_id)` 是单请求终止路径的主要线性化点。
- response、timeout、断连和 stop 不能同时取得同一个 `PendingCall`。
- 不可变 channel snapshot 通过 `shared_ptr` 原子发布，读取方持有局部快照后不受后续替换影响。
- pool 停止时先停止 channel，再停止 callback executor，避免 channel 使用已经销毁的 executor。
- 用户 `done` 在内部锁释放后执行。
- callback executor 会 drain 已成功入队的回调后再退出。

当前实现不提供以下保证：

- 普通异步 callback API 不自动拥有调用方传入的 request、response 和 controller；三者的生命周期要求并不相同。
- `done` 没有固定线程亲和性，不能假定总在 callback worker 上执行。
- 销毁 pool 时，不支持外部线程继续并发调用其成员函数。
- 销毁 `future` 不会取消已经提交的 RPC。
- `repairDeadChannels()` 不提供独立的“repair 已完全退出”屏障。

---

## 2. 总体架构

`RpcChannelPool` 是 channel 集合和 callback executor 的生命周期所有者。pool 持有一个固定大小、发布后不再原地修改的 channel snapshot；调用线程从 snapshot 中选择可用 channel 并提交请求。

每个 `MyRpcChannel` 独立维护：

- 一个 reader 线程；
- 一个 pending-call 表；
- 一个 timeout manager；
- 一个 TCP 连接及其发送互斥；
- channel 生命周期状态。

```text
RpcChannelPool
  ├── unique_ptr<CallbackExecutor>
  └── shared_ptr<const ChannelList> channels_snapshot_
        ├── shared_ptr<MyRpcChannel>
        │     ├── reader thread
        │     ├── PendingCallManager
        │     ├── RpcTimeoutManager
        │     └── socket
        └── ...
```

`MyRpcChannel` 不拥有 `CallbackExecutor`，只保存一个非拥有的 `CallbackExecutor*`。因此生命周期顺序必须满足：

```text
stop all channels
  -> drain and stop CallbackExecutor
  -> destroy pool-owned objects
```

如果先销毁 executor，channel 的 response、timeout 或 stop 完成路径可能访问悬空指针。

---

## 3. 线程模型

| 线程角色 | 创建者 | 主要职责 | 是否可能执行用户 `done` |
|---|---|---|---|
| 调用方线程 | 用户代码 | 调用同步、异步或 Future API | pool 拒绝、early error、executor 不可用时可能 inline 执行 |
| channel reader 线程 | `MyRpcChannel::start()` | 读取 response、解析帧、匹配 pending、处理连接失败 | 通常投递 executor；投递失败时 inline 执行 |
| timeout worker | `RpcTimeoutManager::start()` | 等待 deadline，并通知 channel 处理超时 | 经 `onRpcTimeout()` 完成请求；executor 不可用时可能 inline |
| callback executor worker | `CallbackExecutor::start()` | 顺序执行已入队 closure | 正常异步完成通常在此执行 |
| pool owner / stop 线程 | 用户代码 | `start()`、`stop()`、析构与资源收束 | channel stop 产生的回调通常被投递到 executor |
| repair 调用线程 | 用户代码或测试 | 检测不可用 channel、创建 replacement、发布 snapshot | 停止旧 channel 或未发布 replacement 时可能间接触发回调 |

### 3.1 回调线程亲和性

`done` 没有固定线程亲和性。

正常异步完成路径通常为：

```text
reader / timeout / stop path
  -> CallbackExecutor::post(done)
  -> callback worker 执行 done
```

但以下情况会在当前线程 inline 执行：

- pool 在提交入口拒绝调用；
- 编码或其他 early error；
- callback executor 为空；
- `CallbackExecutor::post()` 失败；
- executor 已进入停止状态，不再接受新任务。

因此用户回调必须满足：

- 不依赖固定线程 ID；
- 不假定串行执行，除非明确知道所有回调均已成功投递到单 worker executor；
- 不在 callback worker 内直接调用 `RpcChannelPool::stop()`。

---

## 4. 所有权与生命周期

### 4.1 `RpcChannelPool`

pool 通过原子发布的 `channels_snapshot_` 间接持有 channel：

- snapshot 类型为 `shared_ptr<ChannelList>` 或等价不可变视图；
- snapshot 发布后，不再原地修改 vector；
- repair 通过构造新 snapshot 并整体替换完成更新；
- stop 通过将当前 snapshot 摘除，使后续提交无法获取旧 channel。

`pickChannel()` 的生命周期行为为：

```text
acquire load snapshot shared_ptr
  -> 从 vector 复制 shared_ptr<MyRpcChannel>
  -> 使用局部 channel shared_ptr 提交请求
```

即使 repair 或 stop 同时替换、清空全局 snapshot，调用线程持有的局部 `shared_ptr` 仍保证 channel 对象存活。

`RpcChannelPool::~RpcChannelPool()` 调用 `stop()`。调用者必须在析构前停止外部提交线程；不支持析构与新的外部 API 调用并发。

### 4.2 `MyRpcChannel`

channel 由 `MyRpcChannel::create()` 以 `shared_ptr` 创建。

以下位置会显式固定对象生命周期：

- `start()` 创建 reader 线程时，线程入口 lambda 捕获 `shared_ptr<MyRpcChannel>`；
- `CallMethod()` 在函数作用域内保存 `shared_from_this()`；
- `stop()` 在函数作用域内保存 `shared_from_this()`。

因此，连接断开不会立即析构 channel：

```text
reader 检测错误
  -> failFromReaderThread()
  -> markPendingFailed()
  -> state_ = kStopping
  -> pool snapshot 仍可能持有旧 channel
  -> repair 替换旧 channel，或 pool.stop() 摘除 snapshot
  -> reader 自引用与所有临时 shared_ptr 释放
  -> MyRpcChannel 析构
```

`MyRpcChannel` 不允许在 reader 线程中析构。若析构时检测到当前线程为 reader 线程，实现会调用 `std::terminate()`，以避免 thread handle、socket 和内部资源在错误线程中被清理。

### 4.3 `PendingCall`

`PendingCallManager::pending_` 保存所有“已接受但尚未终止”的请求。

`PendingCall` 自身由 `shared_ptr` 管理，但其中保存的 controller、response 和 done closure 仍可能是外部裸指针。它们的生命周期取决于调用方式：

| 调用方式          | request 生命周期 | response / controller 生命周期 |
| ------------- | ------------ | ------------------------------ |
| 同步调用          | 至少到 `CallMethod()` 返回 | `CallMethod()` 阻塞至 `call->finished`，调用栈对象在返回前有效 |
| 普通异步 callback | 至少到 `CallMethod()` 返回 | 调用者必须保证存活到 `done` 返回 |
| Future API    | `FutureCallState` 内部持有 | `FutureCallState` 内部持有 |

普通异步 API 的边界必须明确：

> channel 在 `CallMethod()` 内同步序列化 request，但不会保存它；request 至少必须存活到 `CallMethod()` 返回。channel 也不会自动接管 response 和 controller，它们在普通异步调用中必须存活到 `done` 返回。`done` closure 本身也必须在 `Run()` 前保持有效，除非其具体实现自行管理生命周期。

---

## 5. 生命周期状态机

### 5.1 `RpcChannelPool`

```text
kStopped --start()--> kRunning --stop()--> kStopping --cleanup--> kStopped
```

| 状态          | 新 `CallMethod()`                    | `start()`  | `stop()`          | snapshot | callback executor    |
| ----------- | ----------------------------------- | ---------- | ----------------- | -------- | -------------------- |
| `kStopped`  | 拒绝，设置 controller 失败并 inline 执行 done | 尝试创建完整运行环境 | 直接返回              | 空        | 已停止                  |
| `kRunning`  | 允许进入提交过程                            | 返回失败       | 转入 `kStopping`    | 非空       | 运行中                  |
| `kStopping` | `enterCall()` 拒绝                    | 返回失败       | 重复调用直接返回 | 已摘除      | 继续运行以 drain callback |

#### `start()` 顺序

```text
1. 在lifecycle_mutex_下
2. 检查当前状态允许启动。
3. 启动 CallbackExecutor。
4. 逐个创建、连接并启动 channel。
5. 在 repair 同步边界内发布完整 snapshot。
6. 将 pool 状态置为 `kRunning`。
```

启动失败时应回滚已经启动的 channel 和 executor，不发布不完整 snapshot。

#### `stop()` 顺序

当前文档按以下实际语义描述：

```text
1. 拒绝从 callback executor worker 调用 stop，避免 self-join。
2. 在 lifecycle_mutex_ 下将 pool_state_ 置为 kStopping。
3. 在 repair_mutex_ 下摘除 channels_snapshot_，保留 old_snapshot。
4. 在锁外逐个 stop old_snapshot 中的 channel。
5. 等待 active_calls_ == 0。
6. stop CallbackExecutor；executor drain 已成功入队的 done 回调任务。
7. 在lifecycle_mutex_下，将 pool_state_ 置为 kStopped。
```

`active_calls_` 只覆盖：

```text
RpcChannelPool::CallMethod()
  enterCall()
  ...
  leaveCall()
```

它表示“仍有线程停留在 pool 的提交函数中”，不表示“仍有异步 RPC 未完成”。

channel 的 `stop()` 负责终止已经提交的 pending 请求；`active_calls_` 主要保证 callback executor 不会在提交函数仍可能产生完成回调时被停止。

### 5.2 `MyRpcChannel`

```text
kStopped --start/connect--> kRunning
kRunning --stop/reader failure/write failure--> kStopping
kStopping --final cleanup--> kStopped
```

| 状态 | `isAvailable()` | pending 是否接受新请求 | reader | timeout manager |
|---|---|---|---|---|
| `kStopped` | false | 不接受 | 未运行 | 已停止 |
| `kRunning` | true | 接受 | 运行 | 运行 |
| `kStopping` | false | 不接受 | 正在退出或已退出 | 等待完整 `stop()` 收束 |

`markPendingFailed()` 是传输失败与 repair 之间的关键桥梁：

```text
lifecycle_mutex_:
  state_ kRunning -> kStopping

setLastError(reason);
running_ = false
shutdown(socket)
pending_.failAllAndStopAccepting()
```

repair 通过 `isAvailable()` 检查 `state_ == kRunning`，而不是只读取 `running_`。因此 reader failure 将 channel 状态置为 `kStopping` 后，repair 能够识别该 channel 不可用。

`failFromReaderThread()` 不执行完整 `stop()`，因为 reader 线程不能 join 自己。它负责：

- 将 channel 标记为不可用；
- 停止继续接受 pending；
- 终止并完成当前 pending 请求；
- 处理当前 reader thread handle。

最终 socket、reader handle 和 timeout manager 的完整清理依赖后续 repair 或 pool stop 从非 reader 线程调用 `stop()`。

---

## 6. 请求生命周期与唯一完成权

### 6.1 请求提交顺序

当前 `MyRpcChannel::CallMethod()` 的主要顺序为：

```text
1. 固定 channel 生命周期。
2. 生成 request_id 并编码请求帧。
3. pending_.add(request_id, call)。
4. timeout_manager_.add(request_id, timeout)。
5. 获取 send_mutex_。
6. WriteN(request frame)。
7. 同步调用等待 call->finished；异步调用返回。
```

完成路径包括：

- 正常 response；
- 服务端返回 RPC 错误；
- response body 解码失败；
- timeout；
- socket 写失败；
- reader 检测连接失败；
- channel stop；
- timeout manager 无法接受新条目；
- pool 在提交入口拒绝调用。

### 6.2 唯一完成权

一次 RPC 的完成权由 `PendingCallManager` 中的移除操作决定：

```text
正常 response  -> pending_.take(request_id)
timeout         -> pending_.take(request_id)
单请求提交失败  -> pending_.take(request_id)
断连 / stop    -> pending_.failAllAndStopAccepting()
```

规则是：

> 只有成功从 pending 表取得 `PendingCall` 的路径，才允许写入 response/controller 并执行最终完成逻辑。

其他路径得到 `nullptr` 后必须退出，不得修改用户状态，也不得再次执行 `done`。

这一规则尤其要求 response 路径遵守：

```text
错误顺序：
  ParseFromString(response_body)
  pending_.take(request_id)

正确顺序：
  call = pending_.take(request_id)
  if (!call) return
  ParseFromString(response_body)
```

否则 timeout 已经取得完成权后，reader 仍可能修改 response，形成“回调只执行一次，但用户状态被两个路径同时写入”的竞态。

### 6.3 终止路径

| 终止路径 | 触发线程 | 取得完成权 | controller 结果 | 迟到事件 |
|---|---|---|---|---|
| 正常响应 | reader | `pending_.take(id)` | 保持成功 | timeout 到期后 `take` 失败 |
| 服务端 RPC 错误 | reader | `pending_.take(id)` | 服务端错误文本 | timeout 到期后忽略 |
| response 解码失败 | reader | `pending_.take(id)` | response decode error | timeout 到期后忽略 |
| timeout | timeout worker | `pending_.take(id)` | `rpc call timeout` | 迟到 response 被忽略 |
| timeout 注册失败 | 调用方线程 | `pending_.take(id)` | timeout manager error | 不存在有效 timeout 条目 |
| 写失败 | 调用方线程 | `failAllAndStopAccepting()` | `send rpc error` | reader/timeout 后续无法取得 call |
| reader 断连 | reader | `failAllAndStopAccepting()` | reader error | timeout 后续忽略 |
| `channel.stop()` | pool owner / repair | `failAllAndStopAccepting()` | `stop() is called` | timeout 后续忽略 |
| pool 拒绝提交 | 调用方线程 | 请求未进入 pending | `RpcChannel is stopped` 等 | 无后续传输事件 |

`RpcTimeoutManager` 不持有 `PendingCall`。它只持有：

```text
request_id + deadline
```

deadline 到期后调用 `MyRpcChannel::onRpcTimeout(request_id)`，由 channel 再次进入 `PendingCallManager::take()` 竞争完成权。

### 6.4 `finishCall()` 与 `done`

`finishCall()` 的逻辑顺序为：

```text
1. 取出并清空 call->done。
2. 在 call->mutex 下设置 call->finished = true。
3. 通知同步等待者。
4. 调用 runDone(done)。
```

同步等待者和异步回调都只会在请求已从 pending 表移除后观察终态。

`runDone()` 的执行规则：

```text
CallbackExecutor::post(done) 成功
  -> callback worker 执行 done

executor 为空或 post 失败
  -> 当前线程 inline 执行 done
```

框架不得在持有以下锁时执行用户回调：

- `PendingCallManager::mutex_`；
- channel `lifecycle_mutex_`；
- `send_mutex_`；
- `reader_mutex_`；
- pool `lifecycle_mutex_`；
- `repair_mutex_`。

用户 callback 可能重入 RPC、获取用户锁或触发其他阻塞操作，因此必须始终在框架内部临界区之外执行。

---

## 7. Timeout 语义

当前 timeout 注册顺序为：

```text
pending_.add()
  -> timeout_manager_.add()
  -> 等待 send_mutex_
  -> WriteN()
  -> 等待服务端处理与 response
```

因此 timeout 从成功加入 `RpcTimeoutManager` 时开始，覆盖：

- 等待 `send_mutex_`；
- socket 写入；
- 服务端排队与处理；
- response 返回。

### 7.1 Lazy cleanup

timeout manager 使用 lazy cleanup：

- 正常 response 完成后，不立即从 timeout heap 删除对应条目；
- timeout 条目到期后仍会调用 `onRpcTimeout(id)`；
- 此时 `pending_.take(id)` 返回 `nullptr`，路径安全退出。

迟到 response 采用相同原则：

```text
timeout / stop / disconnect 已完成 call
  -> reader 收到迟到 response
  -> pending_.take(id) == nullptr
  -> 记录 DEBUG 并忽略
```

lazy cleanup 简化了 timeout heap 删除逻辑，但会保留过期前的 stale timeout item；相关内存占用取决于 timeout 数量和 deadline 分布。

---

## 8. 共享状态与同步原语

### 8.1 Pool 级同步

| 同步原语 | 保护或发布的状态 | 主要使用位置 |
|---|---|---|
| `lifecycle_mutex_` | `pool_state_`、`active_calls_`、stop 等待条件 | `start()`、`stop()`、`enterCall()`、`leaveCall()` |
| `repair_mutex_` | snapshot 的比较、替换与发布过程 | start 发布、stop 摘除、repair |
| 原子 snapshot load/store | 不可变 snapshot 的跨线程可见性 | `pickChannel()`、`unavailableCount()` |
| 原子 `next_` | round-robin 索引分配 | `pickChannel()` |

`next_` 不发布其他共享数据，只用于分配索引，因此使用 `memory_order_relaxed` 足够。

snapshot 使用 release store / acquire load，使构造完成的不可变 channel 列表安全发布给读取线程。

### 8.2 Channel 级同步

| 同步原语                  | 保护或发布的状态                                        | 主要使用位置                                                   |
| --------------------- | ----------------------------------------------- | -------------------------------------------------------- |
| `lifecycle_mutex_`    | `state_` 转换                                     | `start()`、`markPendingFailed()`、`stop()`、`isAvailable()` |
| 原子 `running_`         | reader 与传输 I/O 是否继续                             | reader loop、`ReadN()`、`WriteN()`、失败路径                    |
| `send_mutex_`         | socket 写入与最终 close 的互斥                          | `CallMethod()`、`cleanupStoppedConnection()`              |
| `reader_mutex_`       | reader thread handle、thread id、move/join/detach | start、join、reader failure                                |
| `error_mutex_`        | `last_error_`                                   | `setLastError()`、`LastError()`                           |
| pending manager mutex | `accepting_` 与 pending map                      | add、take、failAll                                         |
| timeout manager mutex | timeout heap 与 worker wait                      | add、loop、stop                                            |

### 8.3 Socket 关闭顺序

`cleanupStoppedConnection()` 应满足：

```text
running_ = false
  -> shutdown(socket)，唤醒阻塞 I/O
  -> join reader
  -> 获取 send_mutex_
  -> close(socket)
```

目的：

- `shutdown()` 使阻塞的 `ReadN()` 或 `WriteN()` 尽快返回；
- join reader 后不再有 reader 使用 fd；
- `send_mutex_` 保证 close 不与正在执行的写操作并发。

### 8.4 内存序

当前主要使用方式：

- channel `running_`：release store / acquire load；
- timeout manager `running_`：release store / acquire load；
- channel snapshot：release store / acquire load；
- round-robin `next_`：relaxed。

需要注意：atomic 只发布状态变化；pending map、timeout heap、thread handle 等复合状态仍由各自 mutex 保护。

---

## 9. 锁顺序与阻塞操作

### 9.1 已确认的嵌套顺序

```text
RpcChannelPool::start() / stop()
  lifecycle_mutex_
    -> repair_mutex_

RpcChannelPool::repairDeadChannels()
  repair_mutex_
    -> MyRpcChannel::lifecycle_mutex_（经 isAvailable()）

RpcTimeoutManager::start() / stop()
  lifecycle_mutex_
    -> heap mutex
```

`pending_.add/take`、`timeout_manager_.add` 和 `send_mutex_` 通常在独立短临界区中使用，不跨较大调用链持有。

### 9.2 禁止在内部锁内执行的操作

| 操作 | 约束 | 原因 |
|---|---|---|
| 用户 `done` | 必须在内部锁外执行 | callback 可重入框架并获取任意用户锁 |
| 网络 connect | 不持有 `repair_mutex_` | connect 可能阻塞 |
| channel start | 不持有 `repair_mutex_` | 可能创建线程和访问网络 |
| channel stop | 不持有 `repair_mutex_` | 可能 shutdown、join、drain callback |
| reader join | 不持有 `reader_mutex_` 等待 | 避免持元数据锁等待线程退出 |
| socket I/O | 不持有生命周期锁长期阻塞 | 避免阻塞状态转换 |
| executor stop/join | 不从 executor worker 调用 | 避免 self-join |

---

## 10. 关键并发交错

### 10.1 Submit vs Stop

```text
提交线程                              stop 线程
--------                              ---------
enterCall():
  pool_state_ == kRunning
  active_calls_++

                                    pool_state_ = kStopping
                                    摘除 channels_snapshot_

pickChannel():
  持有局部 snapshot/channel shared_ptr

channel->CallMethod()                stop old_snapshot channels

leaveCall():
  active_calls_--

                                    等待 active_calls_ == 0
                                    stop callback executor
```

可能结果：

1. 提交在 `enterCall()` 处观察到 pool 已非 `kRunning`：
   - 请求不进入 channel；
   - controller 设置为 `RpcChannel is stopped`；
   - `done` 在当前线程 inline 执行。

2. 提交已经通过 `enterCall()`：
   - 允许继续使用局部 channel `shared_ptr`；
   - channel stop 可能与提交竞争；
   - 请求最终可能正常完成，也可能以 stop、写失败或连接失败终止；
   - `leaveCall()` 后 pool 才允许最终停止 executor。

`active_calls_` 不阻止 channel stop 与提交过程并发；它保证的是在提交函数完全退出前，pool 不销毁 callback executor。

### 10.2 Response vs Timeout

```text
reader thread                         timeout worker
-------------                         --------------
pending_.take(request_id)             pending_.take(request_id)
```

两条路径共享 `PendingCallManager` 内部 mutex。只有一条路径能够移除目标 call。

胜出的路径必须：

1. 先取得 `PendingCall`；
2. 再写 response 或 controller；
3. 再调用 `finishCall()`。

失败路径得到 `nullptr` 后不得修改用户对象。

### 10.3 Repair vs Stop

repair 使用“复制旧 snapshot、锁外构建、重新比较、条件发布”的模式：

```text
repair:
  old_snapshot = load current snapshot
  在锁外创建 replacement

stop:
  将 current snapshot 置空

repair publish phase:
  now_snapshot = load current snapshot
  if now_snapshot != old_snapshot:
      不发布 replacement
      在锁外 stop replacement
```

两种有效结果：

- repair 先发布：后续 stop 取得包含 replacement 的 snapshot，并停止所有 channel；
- stop 先摘除：repair 的 snapshot 比较失败，不允许重新发布旧视图，replacement 自行 stop。

当前正确性依赖 snapshot 身份比较和发布前复核。

当前限制：

- 没有独立的 `repair_in_progress` 计数；
- `stop()` 不等待所有 repair 调用返回；
### 10.4 Reader Failure vs Repair

```text
reader failure
  -> markPendingFailed()
  -> state_ = kStopping
  -> detachReaderHandleIfCurrentThread()
  -> isAvailable() == false
  -> repair 创建 replacement
  -> 条件发布新 snapshot
  -> 在锁外 stop old channel
  -> old timeout manager 最终停止
```

reader failure 不能在 reader 线程执行完整 stop，因此失败 channel 的最终清理依赖 repair 或 pool stop。

如果应用既不调用 repair，也不调用 pool stop：

- 失败 channel 仍可能被 snapshot 持有；
- timeout manager 不一定立即停止；
- channel 保持不可用但尚未完全清理的状态。

这是当前实现的已知生命周期限制。

### 10.5 Callback vs Stop

以下调用链会形成 self-join：

```text
callback worker
  -> 用户 done
  -> pool.stop()
  -> callback_executor.stop()
  -> join callback worker
```

因此：

- `CallbackExecutor::stop()` 拒绝从自身 worker 调用；
- `RpcChannelPool::stop()` 也拒绝从 callback worker 调用；
- 用户回调需要停止客户端时，应通知外部 owner 线程执行 stop。

---

## 11. CallbackExecutor 语义

`CallbackExecutor` 是单 worker 任务队列。

### `start()`

- 创建 worker；
- 等待 worker 进入运行循环；
- 此后 `post()` 才可成功接受任务。

### `post()`

仅在以下条件成立时入队：

```text
started_ == true
stopping_ == false
```

成功入队后，任务由 worker 顺序执行。

### `stop()`

- 设置 `stopping_`；
- 拒绝新的 `post()`；
- worker 继续执行已经入队的任务；
- queue 为空后退出；
- 非 worker 线程 join worker。

因此 pool 必须先使所有 channel 不再产生新的完成事件，再停止 executor。

需要区分：

- **已成功入队的回调**：stop 时会被 drain；
- **stop 后尝试 post 的回调**：post 失败，由 `runDone()` 在当前线程 inline 执行。

---

## 12. 调用方使用契约

### 12.1 同步调用

同步调用传入 `done == nullptr`：

```cpp
stub.Login(&controller, &request, &response, nullptr);

if (controller.Failed()) {
    // timeout、断连、stop、编码失败或写失败
    return;
}

// 只有 controller 成功时才读取业务 response
```

`CallMethod()` 会等待 `call->finished`，因此调用栈中的 request、response 和 controller 在函数返回前保持有效。

同步调用返回后必须先检查 `controller.Failed()`，失败路径不保证业务 response 有效。

### 12.2 普通异步 callback

调用者必须让 request、response 和 controller 存活到 `done` 返回：

```cpp
auto state = std::make_shared<CallState>();

auto done = NewClosure([state] {
    if (state->controller.Failed()) {
        // 处理失败
        return;
    }

    // 使用 state->response
});

stub.Login(
    &state->controller,
    &state->request,
    &state->response,
    done);
```

不安全示例：

```cpp
void submit()
{
    Request request;
    Response response;
    Controller controller;

    stub.Login(&controller, &request, &response, done);
} // 函数返回后对象已经销毁，但异步请求可能仍在运行
```

### 12.3 Future API

Future API 通过 `FutureCallState` 持有：

- request；
- response；
- controller；
- promise；
- done closure。

提交后，外部 request 不需要继续存活。

销毁 future 不等价于取消 RPC。请求仍由内部 state、pending call 和 closure 保持到某个终止路径完成。

### 12.4 Start / Stop 规则

调用方应遵守：

1. 确认 `start()` 成功后再提交 RPC。
2. 调用 `stop()` 前停止创建新的外部提交任务。
3. 不从 callback worker 直接调用 `pool.stop()`。
4. `stop()` 返回后可以重新 `start()`，但不得继续使用旧 channel 的直接引用。
5. pool 析构前同样满足第 2、3 条。
6. 不将 pool 析构与其成员函数调用并发执行。

---

## 13. 错误语义与可观测性

| 事件                    | 调用方可见结果                          | 当前日志行为            | 建议指标                     |
| --------------------- | -------------------------------- | ----------------- | ------------------------ |
| timeout               | controller：`rpc call timeout`    | 不记录               | timeout count            |
| 已完成/未知 response     | 不再修改已完成 call                     | DEBUG：pending 不存在 | completed-or-unknown response count |
| 写失败                   | 当前 pending 批量失败：`send rpc error` | channel 错误日志      | send failure count       |
| reader 失败             | 当前 pending 按 reader 错误失败         | reader/channel 日志 | connection loss count    |
| pool 已停止              | `RpcChannel is stopped`          | 通常无专门日志           | rejected submit count    |
| 无可用 channel           | `no available rpc channel`       | 通常无专门日志           | unavailable submit count |
| repair 尝试             | 尝试替换不可用 channel                  | 可选 INFO           | repair attempt count     |
| repair 丢弃 replacement | replacement 被 stop               | 可选 DEBUG          | stale repair count       |

压力测试不应只依赖日志判断正确性。至少断言：

```text
submitted == completed
final_inflight == 0
duplicate_done == 0
success + failed == completed
max_observed_inflight <= configured_max_inflight
```

场景型测试还应验证预期事件确实发生，例如：

- timeout 数非零；
- disconnect 数非零；
- repair 尝试数非零，并且后续请求能够恢复完成；
- 已完成/未知 response 数非零；
- stop 拒绝提交数非零。

---

## 14. 测试证据与 CI 分类

| 风险                     | 主要测试                                                                       | 关键断言                      | 标签            |
| ---------------------- | -------------------------------------------------------------------------- | ------------------------- | ------------- |
| 长时间异步提交                | `tests/stress/async_sustained_stress.cc`                                   | 完成计数、inflight 上限、无重复 done | `stress`      |
| 断连与 replacement        | `tests/stress/disconnect_repair_stress.cc`                                 | 请求收束、断连和 repair 计数        | `stress`      |
| submit / stop / repair | `tests/stress/submit_stop_race_stress.cc`                                  | 多轮启停、drain、无重复 done       | `stress`      |
| timeout 与迟到响应          | `tests/stress/timeout_stress.cc`                                           | 多类结果出现、服务端完成响应、done once  | `stress`      |
| timeout manager 基础行为   | `tests/unit/rpc_timeout_manager_test.cc`                                   | start/stop、heap、到期回调、并发行为 | `unit`        |
| channel / pool 集成      | `tests/integration/rpc_channel_timeout_test.cc`、`rpc_channel_pool_test.cc` | timeout、连接池、stop、repair   | `integration` |
| pending add 与 stop      | `tests/integration/rpc_channel_race_test.cc`                                | 请求完成一次、pending 收束      | `race`        |
| response 与 timeout      | `tests/integration/rpc_channel_race_test.cc`                                | timeout 结果不被 response 覆盖、done once | `race` |
| repair 发布与 stop        | `tests/integration/rpc_channel_pool_race_test.cc`                           | stop 后不发布 replacement、replacement 已停止 | `race` |

推荐验证顺序：

```bash
# 普通快速回归
cmake -S . -B build-debug \
  -DCMAKE_BUILD_TYPE=Debug \
  -DMYRPC_BUILD_TESTS=ON

cmake --build build-debug -j
ctest --test-dir build-debug --output-on-failure

# 常规功能测试
cmake -S . -B build-debug \
  -DCMAKE_BUILD_TYPE=Debug \
  -DMYRPC_BUILD_TESTS=ON

cmake --build build-debug -j
ctest --test-dir build-debug -N -L unit --output-on-failure
ctest --test-dir build-debug -N -L integration --output-on-failure

# ASAN：确定性竞态测试
cmake -S . -B build-asan \
  -DCMAKE_BUILD_TYPE=Debug \
  -DMYRPC_BUILD_RACE_TESTS=ON \
  -DMYRPC_ENABLE_TEST_HOOKS=ON \
  -DMYRPC_ENABLE_ASAN=ON
cmake --build build-asan -j
ctest --test-dir build-asan \
  -L race \
  --output-on-failure \
  --parallel 1

# TSAN：确定性竞态测试
cmake -S . -B build-tsan \
  -DCMAKE_BUILD_TYPE=Debug \
  -DMYRPC_BUILD_RACE_TESTS=ON \
  -DMYRPC_ENABLE_TEST_HOOKS=ON \
  -DMYRPC_ENABLE_TSAN=ON
cmake --build build-tsan -j
ctest --test-dir build-tsan \
  -L race \
  --output-on-failure \
  --parallel 1

# 压力测试：各测试的 CTest 参数与 timeout 见 `tests/stress/CMakeLists.txt`；长时间测试默认可运行 300 秒。
cmake -S . -B build-stress \
  -DCMAKE_BUILD_TYPE=Debug \
  -DMYRPC_BUILD_STRESS_TESTS=ON
cmake --build build-stress -j
ctest --test-dir build-stress \
  -L stress \
  --output-on-failure \
  --parallel 1
```

---

## 15. Stress 结果解释

| 现象 | 严重性 | 含义 |
|---|---|---|
| `duplicate_done > 0` | 致命 | 唯一完成权被破坏 |
| `submitted != completed` | 致命 | 请求丢失、callback 悬挂或测试未收束 |
| `final_inflight != 0` | 高 | pending 或测试状态未完全清理 |
| `success + failed != completed` | 高 | 完成分类不完整或存在重复统计 |
| `max_observed_inflight > max_inflight` | 高 | 客户端限流失效 |
| timeout 数较高 | 结合场景 | 可能是预期延迟、队头阻塞或性能不足 |
| late response 日志 | 通常可接受 | timeout、stop 或断连先取得完成权 |

### 15.1 `timeout_stress` 的队头阻塞

测试服务端在每条 TCP 连接上通常串行执行：

```text
read request
  -> build response
  -> write response
  -> read next request
```

因此一个 deliberately-late 请求会阻塞同一连接后面的 fast 或 near-timeout 请求。

不能简单按服务端生成的请求类别比例推导客户端成功比例。正确断言应关注：

- 请求全部终止；
- 每个请求恰好完成一次；
- success、timeout 等预期类别均出现；
- 服务端最终构造全部 response；
- 迟到 response 不会修改已终止请求。

### 15.2 测试规模与 CTest 超时

大量 timeout 请求时，可用以下近似估算测试耗时下界：

```text
throughput upper bound ≈ max_inflight / timeout_seconds

minimum duration ≈ max_requests / throughput upper bound
```

例如：

```text
max_inflight = 16
timeout = 40 ms = 0.04 s

throughput upper bound ≈ 16 / 0.04 = 400 requests/s
```

若 `max_requests = 300000`，仅依赖 timeout 释放 inflight 的理论下界约为 750 秒，尚未计算 drain 和服务端清理。

因此应满足：

```text
CTest TIMEOUT > test duration + drain time + cleanup margin
```

短 CI stress 与 300 秒长 stress 应通过环境变量或命令行参数复用同一测试逻辑。

---

## 16. 代码级时序

### 16.1 正常异步调用

```text
调用方线程
  RpcChannelPool::CallMethod()
    enterCall()
      lifecycle_mutex_:
        require pool_state_ == kRunning
        active_calls_++

    pickChannel()
      acquire-load channels_snapshot_
      next_.fetch_add(relaxed)
      channel->isAvailable()

    MyRpcChannel::CallMethod()
      shared_from_this()
      allocate request_id
      encode request frame
      pending_.add(id, call)
      timeout_manager_.add(id, deadline)
      send_mutex_:
        WriteN(frame)

    leaveCall()
      lifecycle_mutex_:
        active_calls_--
        notify stop waiter

reader thread
  read and decode response frame
  call = pending_.take(id)
  if call == nullptr:
      当前统一忽略已完成、重复或未知 response
  else:
      parse response into call->response
      update controller if needed
      finishCall(call)

callback worker
  done->Run()
```

需要区分两个完成概念：

- `leaveCall()`：提交函数退出；
- `finishCall()`：RPC 获得最终结果。

因此：

```text
active_calls_ == 0
```

只表示没有线程停留在 pool 提交函数中，不表示 pending 表为空，也不表示所有 callback 已执行。

### 16.2 Timeout 与已完成 response

```text
调用方线程
  pending_.add(id, call)
  timeout_manager_.add(id, deadline)
  wait send_mutex_ / WriteN

timeout worker
  deadline reached
  -> onRpcTimeout(id)
  -> call = pending_.take(id)
       success: timeout owns completion
       nullptr: another path already completed call
  -> set controller timeout error
  -> finishCall(call)

reader later receives response
  -> pending_.take(id) == nullptr
  -> ignore without touching response/controller
```

### 16.3 写失败或 reader failure

```text
WriteN failure
  -> markPendingFailed("send rpc error")
     state_ = kStopping
     running_ = false
     shutdown(socket)
     calls = pending_.failAllAndStopAccepting()
  -> for each call:
       finishCallWithError(call)

reader failure
  -> failFromReaderThread(reason)
     -> markPendingFailed(reason)
     -> release/detach current reader handle as implemented
  -> finish all removed calls outside locks
```

### 16.4 Pool stop

```text
pool owner thread
  RpcChannelPool::stop()
    reject callback-worker invocation

    lifecycle_mutex_:
      pool_state_ = kStopping

    repair_mutex_:
      old_snapshot = channels_snapshot_
      channels_snapshot_ = empty

    for channel in old_snapshot:
      channel->stop()
        shared_from_this()
        markPendingFailed("stop() is called")
        cleanupStoppedConnection()
          running_ = false
          shutdown(socket)
          join reader
          send_mutex_:
            close(socket)
        timeout_manager_.stop()
        state_ = kStopped
        finish removed pending calls outside locks

    lifecycle_mutex_:
      wait active_calls_ == 0

    callback_executor_->stop()
      drain queued callbacks
      join worker

    lifecycle_mutex_:
      pool_state_ = kStopped
```

---

## 17. 已知限制与后续改进

1. **Timeout 注释与代码统一**  
   确认正式契约为“timeout 覆盖等待 send mutex 与 socket 写入”，同步修改头文件、README 和测试名称。

2. **普通异步 API 的对象所有权**  
   当前只保存外部裸指针。应在公共 API 文档中明确生命周期要求；若未来改进，可引入框架持有的调用状态。

3. **Reader failure 的完整清理**  
   reader failure 后 timeout manager 的停止依赖 repair 或 pool stop。

4. **Repair 与 stop 的完成屏障**  
   当前依赖 snapshot 比较阻止 stale publication，但 stop 不等待 repair 调用退出。若需要更强契约，可增加 repair 活跃计数或 generation token。

5. **Pending 不存在的可观测性**  
   迟到 response、重复 response 和未知 request id 当前共享同一日志分支；因此只能按“已完成/未知 response”计数。若需要精确诊断，可保留有界终态记录：已超时或断连清理的 request id 记 DEBUG 并计数，从未注册或重复的 request id 记 WARN。

6. **Lazy timeout item**  
   正常完成后 timeout heap 中的条目在 deadline 前不会删除。应通过容量测试确认高 QPS、长 timeout 场景下的内存上界。

7. **异步取消语义**  
   当前 Future 和 callback API 均不支持显式取消。销毁 future 或 closure 不会取消已经提交的请求。

8. **析构并发契约**  
   pool 和 channel 的析构不支持与外部成员函数调用并发。

---

## 18. 确定性竞态测试

### 18.1 Pending add vs Stop

交错：

```text
CallMethod:
  pending_.add() 完成
  timeout_manager_.add() 尚未执行
  ---- hook pause ----

stop:
  markPendingFailed()
  failAllAndStopAccepting()

resume CallMethod
```

断言：

- 请求最终完成一次；
- timeout 注册失败路径不能再次修改 controller；
- pending 最终为空；
- 无 use-after-free。

### 18.2 Response ownership vs Timeout

交错：

```text
reader:
  response frame 已解析
  pending_.take() 尚未执行
  ---- hook pause ----

timeout:
  pending_.take() 成功并完成 call

resume reader
```

断言：

- reader 的 `take()` 返回空；
- reader 不解析到用户 response；
- controller 保持 timeout 结果；
- `done` 只执行一次。

### 18.3 Repair publication vs Stop

交错：

```text
repair:
  创建 replacement 完成
  发布 snapshot 前暂停

stop:
  摘除 snapshot
  stop old channels
  返回或进入最终收束

resume repair
```

断言：

- replacement 不得在 stop 后发布；
- replacement 必须自行 stop；
- stop 返回后 snapshot 为空；
- 所有 channel 均不可用且已停止。


---

## 19. 并发修改检查清单

### 所有权与生命周期

- [ ] 明确新对象的 owner。
- [ ] 明确使用裸指针、`unique_ptr`、`shared_ptr` 或 `weak_ptr` 的原因。
- [ ] 明确对象可能在哪些线程释放最后一个引用。
- [ ] 析构是否可能发生在 reader、timeout 或 callback worker。
- [ ] 外部调用方需要维持哪些对象的生命周期。

### 线程

- [ ] 明确线程创建者。
- [ ] 明确退出条件。
- [ ] 明确 join、detach 或自持有规则。
- [ ] 检查 self-join。
- [ ] 检查线程退出后是否仍可能访问 owner。

### 共享状态

- [ ] 每个共享字段由 mutex、atomic 或单线程所有权保护。
- [ ] atomic 的内存序与发布语义明确。
- [ ] 复合状态不依赖多个无关联 atomic 拼接。
- [ ] snapshot 发布后保持不可变。

### 锁与阻塞

- [ ] 写出锁顺序。
- [ ] 检查不存在反向获取路径。
- [ ] 不在持锁状态执行用户 callback。
- [ ] 不在持有全局生命周期锁时进行网络阻塞。
- [ ] 不在持有元数据锁时 join。
- [ ] close 与 ReadN/WriteN 的并发关系明确。

### 请求完成

- [ ] 每条终止路径先取得完成权，再修改 response/controller。
- [ ] `done` 恰好执行一次。
- [ ] 同步 waiter 恰好被唤醒并能观察终态。
- [ ] 迟到 response 不修改已经完成的调用。
- [ ] timeout、stop、disconnect 和 response 的竞争均有测试。

### 验证

- [ ] Debug 常规测试通过。
- [ ] ASAN 核心测试通过。
- [ ] TSAN 并发测试通过。
- [ ] 短 stress 在 CI 中运行。
- [ ] 关键交错有 hook/barrier 确定性测试。
- [ ] 长 stress 的 duration 与 CTest timeout 匹配。
