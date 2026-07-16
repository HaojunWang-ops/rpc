# my_rpc 性能基准

本目录包含 `my_rpc` 的客户端性能基准程序，用于观察以下因素对吞吐、延迟和客户端 CPU 开销的影响：

- 同步调用与异步调用方式；
- 客户端连接池大小；
- 客户端提交线程数；
- 最大在途请求数；
- Future API；
- 请求 payload 大小。

所有基准程序均依赖仓库中的演示 `UserService`：

- `Login`：小请求、小响应，用于同步调用、异步 callback 和 Future 基准；
- `Register`：payload 基准将指定大小的数据写入 `RegisterRequest.password` 字段。

这些结果主要用于比较同一机器、同一构建和同一服务端配置下的相对趋势。本地 loopback 结果不代表跨机器网络环境中的实际性能，也不应直接视为生产性能上限。

## 构建

建议使用 Release 构建性能基准：

```bash
cmake -S . -B build-benchmark \
  -DCMAKE_BUILD_TYPE=Release \
  -DMYRPC_BUILD_BENCHMARKS=ON

cmake --build build-benchmark -j
```

基准目标由 [`benchmarks/CMakeLists.txt`](CMakeLists.txt) 注册，生成在：

```text
build-benchmark/benchmarks/
```

Debug、ASAN 和 TSAN 构建适合功能与并发问题排查，不适合作为正式性能数据。

## 运行准备

先启动 provider：

```bash
./build-benchmark/examples/provider warn
```

当前示例 provider 在 `examples/provider_main.cc` 中使用：

```cpp
RpcProvider provider(4);
```

因此结果中的 `Provider Workers` 默认记录为 `4`。该字段只是测试环境标签，客户端基准参数不会修改服务端 worker 数。比较不同服务端线程数时，需要分别启动对应配置的 provider。

默认连接地址为：

```text
127.0.0.1:8000
```

默认 RPC timeout 为 `3000 ms`，当前基准程序不提供 timeout 命令行参数。

`warmup_count` 只用于预热，不计入正式阶段的请求数、吞吐和延迟统计。

## 基准程序

| 程序 | 覆盖内容 | 默认扫描维度 | 位置参数 |
| --- | --- | --- | --- |
| `rpc_sync_single_benchmark` | 单 TCP channel、同步串行 `Login` | 单配置 | `[ip] [port] [request_count] [warmup_count]` |
| `rpc_pool_size_benchmark` | 固定客户端线程数，比较不同连接池大小下的同步 `Login` | pool size：`1, 2, 4, 8, 16` | `[ip] [port] [total_requests] [client_threads] [warmup_count]` |
| `rpc_sync_threads_benchmark` | 固定连接池大小，比较不同客户端线程数下的同步 `Login` | client threads：`1, 2, 4, 8, 16, 32` | `[ip] [port] [total_requests] [pool_size] [warmup_count]` |
| `rpc_async_callback_benchmark` | callback 异步 `Login`，比较不同最大在途请求数 | max inflight：`10, 50, 100, 200, 500, 1000` | `[ip] [port] [total_requests] [pool_size] [client_threads] [warmup_count]` |
| `rpc_future_benchmark` | Future 风格 `Login`，比较不同最大在途请求数 | max inflight：`10, 50, 100, 200, 500, 1000` | `[ip] [port] [total_requests] [pool_size] [client_threads] [warmup_count]` |
| `rpc_payload_size_benchmark` | 同步 `Register`，比较请求 payload 大小的影响 | payload：`16 B, 128 B, 1 KiB, 4 KiB, 16 KiB, 64 KiB, 256 KiB, 1 MiB` | `[ip] [port] [total_requests] [pool_size] [client_threads] [warmup_count]` |

## 运行示例

单 channel 同步串行小请求：

```bash
./build-benchmark/benchmarks/rpc_sync_single_benchmark \
  127.0.0.1 8000 10000 1000
```

固定 `8` 个客户端线程，扫描连接池大小：

```bash
./build-benchmark/benchmarks/rpc_pool_size_benchmark \
  127.0.0.1 8000 100000 8 1000
```

固定连接池大小为 `4`，扫描客户端线程数：

```bash
./build-benchmark/benchmarks/rpc_sync_threads_benchmark \
  127.0.0.1 8000 100000 4 1000
```

callback 异步调用，连接池大小 `4`、提交线程数 `4`，扫描最大在途请求数：

```bash
./build-benchmark/benchmarks/rpc_async_callback_benchmark \
  127.0.0.1 8000 100000 4 4 1000
```

Future 调用，连接池大小 `4`、客户端线程数 `4`，扫描最大在途请求数：

```bash
./build-benchmark/benchmarks/rpc_future_benchmark \
  127.0.0.1 8000 100000 4 4 1000
```

payload 基准，连接池大小 `4`、客户端线程数 `4`：

```bash
./build-benchmark/benchmarks/rpc_payload_size_benchmark \
  127.0.0.1 8000 100000 4 4 1000
```

## 输出格式

`rpc_sync_single_benchmark` 使用 key/value 格式：

```text
mode=sync_single_channel
ip=127.0.0.1
port=8000
warmup_count=1000
request_count=10000
completed=10000
success=10000
failed=0
error_rate=0%
elapsed_seconds=...
qps=...
p50_latency_ms=...
p90_latency_ms=...
p99_latency_ms=...
max_latency_ms=...
client_cpu_percent=...
```

其他基准程序输出 Markdown 表格，可直接复制到测试报告中。

## 指标口径

- `Requests` / `request_count`：正式压测阶段提交的请求数，不包含 warmup。
- `Completed` / `completed`：正式阶段已经终止的请求数。
- `Success` / `success`：RPC controller 未失败，并且业务响应中的 `success == true`。
- `Failed` / `failed`：RPC 错误、timeout、连接错误、响应解析失败，或业务响应中的 `success != true`。
- `QPS`：正式阶段完成请求数除以该阶段的墙钟时间。
- `p50` / `p90` / `p99` / `Max`：客户端侧端到端延迟。
- `Error Rate` / `error_rate`：`Failed / (Success + Failed)`。
- `Client CPU` / `client_cpu_percent`：客户端进程 CPU 时间除以墙钟时间；多线程运行时可能超过 `100%`。
- `MiB/s`：仅 payload 基准输出，按 `payload bytes × completed requests / elapsed time` 计算。

`MiB/s` 仅表示应用层请求 payload 吞吐，不包含：

- RPC frame header；
- Protobuf 字段与编码开销；
- TCP/IP 协议开销；
- 响应流量。

延迟统计起止点：

- 同步基准：从调用 `stub.Login()` 或 `stub.Register()` 到调用返回；
- callback 基准：从提交异步 RPC 到对应 callback 开始执行；
- Future 基准：从提交 Future RPC 到基准程序观察到对应 future ready。

不同 API 的完成观察方式并不完全相同，因此 callback 与 Future 数据适合用于趋势比较，不宜把微小差异直接解释为 API 本身的确定性能差距。

## 测试方法与复现要求

比较两组数据时，应固定：

- 代码提交；
- 编译器及版本；
- CMake 构建类型和编译参数；
- Protobuf 版本；
- provider worker 数；
- 客户端日志级别；
- CPU governor 和系统负载；
- 测试地址与端口；
- warmup 和正式请求数。

建议遵守以下规则：

1. 一次只运行一个客户端基准，避免多个进程竞争同一个 provider。
2. 正式记录前至少试跑一次，确认没有 RPC 失败或 timeout。
3. 对重要结论重复运行多轮，记录中位数或均值以及波动范围。
4. 比较前确认 provider 已稳定启动，且没有残留客户端或端口冲突。
5. payload 达到 `1 MiB` 时，先用较小的 `total_requests` 试跑，避免单次运行时间和内存流量过大。
6. 保留原始输出，不只保存二次整理后的图表。

## 基准结果（2026-07-12）

以下数据来自一次本机 loopback 试跑，只用于记录当前机器和当前代码状态下的表现。由于没有重复轮次和置信区间，不应根据单次结果作过强结论。

测试环境：

- 记录时间：`2026-07-12 15:26:32 CST（UTC+8）`
- 系统：`Linux vm 6.17.0-35-generic #35~24.04.1-Ubuntu SMP PREEMPT_DYNAMIC Tue May 26 19:30:42 UTC 2 x86_64`
- 构建：`cmake -S . -B build-benchmark -DCMAKE_BUILD_TYPE=Release -DMYRPC_BUILD_BENCHMARKS=ON`
- 服务端：`./build-benchmark/examples/provider warn`
- 服务地址：`127.0.0.1:8000`
- Provider workers：`4`
- 参数：未额外传入命令行参数，使用各程序默认值。

## 性能分析

以下分析基于单次本机 loopback 结果。它适合解释当前实现的相对趋势，但不能替代多轮重复测试、跨机器测试或与其他 RPC 框架的同条件对比。

### 1. 单连接能够承载多个并发请求

同步单线程、单 channel 串行调用为：

```text
1506.92 QPS
p50 = 0.488 ms
p99 = 4.011 ms
```

固定 `8` 个调用线程、pool size 为 `1` 时，同一条 TCP 连接达到：

```text
4943 QPS
p50 = 1.329 ms
p99 = 5.953 ms
```

吞吐提高约 `3.28×`。这说明单个 channel 并不是“一次只能等待一个请求返回”：多个调用线程可以先后完成写入，然后同时在 pending 表中等待不同 `request_id` 的 response，reader 再按 `request_id` 匹配完成。

这组数据验证了客户端请求复用机制确实有效，但也展示了典型权衡：

- 并发在途请求提高了连接利用率；
- 更多请求同时排队，使中位延迟从 `0.488 ms` 上升到 `1.329 ms`；
- 吞吐提升不是线性的，因为发送锁、reader、服务端调度和回调唤醒仍有共享开销。

### 2. 小请求场景下，增加连接数没有带来收益

固定 `8` 个客户端线程时：

| Pool Size | QPS | 相对 pool=1 | p50 | p99 |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 4943 | 基线 | 1.329 ms | 5.953 ms |
| 2 | 4759 | -3.7% | 1.388 ms | 6.063 ms |
| 4 | 4028 | -18.5% | 1.656 ms | 7.073 ms |
| 8 | 3894 | -21.2% | 1.746 ms | 7.521 ms |
| 16 | 3952 | -20.0% | 1.698 ms | 7.058 ms |

在当前环境下，pool size 为 `1` 的吞吐最高，连接数增加后 QPS 反而下降。

较合理的解释是：

- 单连接已经能够复用多个在途请求，连接数不是主要限制；
- 每个新增 channel 都会增加 socket、reader 线程和调度开销；
- 小包 loopback 下，额外连接带来的并行收益不足以抵消线程切换、缓存和连接管理成本；
- provider 只有 `4` 个业务 worker，继续增加客户端连接不等于增加服务端处理能力。

但这只是单轮数据，尚不能断言“连接池越小越好”。要确认该结论，需要重复多轮并分别扫描 provider worker 数、客户端线程数和跨机器网络延迟。

就当前结果而言，对于小请求同步调用，pool size `1–2` 已足够；pool size `8–16` 没有体现收益。

### 3. 增加同步调用线程能够继续提高吞吐，但延迟成本明显

固定 pool size 为 `4` 时：

```text
client threads: 1  -> 32
QPS:            1573 -> 7181
```

吞吐提高约 `4.57×`，但线程数增加了 `32×`，扩展效率逐步下降。

与此同时：

```text
p50: 0.472 ms -> 3.938 ms，约 8.34×
p99: 3.923 ms -> 12.613 ms，约 3.22×
```

说明更多调用线程主要通过增加在途请求、隐藏等待时间来提升吞吐，而不是降低单次调用成本。线程数越高，请求越容易在发送锁、socket、provider worker 或调度队列中等待。

本次测试在 `32` 个客户端线程时 QPS 仍在上升，尚未观察到明确吞吐峰值；但延迟已经明显恶化。因此合理配置不应只取最大 QPS，还应根据延迟目标选择：

- 低延迟优先：`1–4` 个调用线程；
- 吞吐优先且允许更高尾延迟：`16–32` 个调用线程。

`Client CPU` 只统计客户端进程，不包含 provider，因此不能仅凭客户端约 `101%` CPU 判断系统瓶颈位置。

### 4. 异步在途数越大，吞吐越高，但收益递减

callback 异步基准中，max inflight 从 `10` 增加到 `1000`：

```text
QPS: 3935 -> 21606，约 5.49×
p50: 1.953 ms -> 20.623 ms，约 10.56×
p99: 8.987 ms -> 96.598 ms，约 10.75×
```

在途数增加了 `100×`，吞吐只提高约 `5.5×`，说明高并发阶段已经进入明显的排队区间。

分段观察：

| Max Inflight | QPS | 相比前一档 | p99 |
| ---: | ---: | ---: | ---: |
| 10 | 3935 | — | 8.987 ms |
| 50 | 6804 | +72.9% | 23.068 ms |
| 100 | 9080 | +33.4% | 33.317 ms |
| 200 | 14012 | +54.3% | 43.571 ms |
| 500 | 18225 | +30.1% | 61.023 ms |
| 1000 | 21606 | +18.6% | 96.598 ms |

尤其从 `500` 增加到 `1000`：

- QPS 只增加 `18.6%`；
- p99 增加约 `58.3%`。

因此 `1000` 并不是无条件更优。按本次结果：

- `100–200`：吞吐和延迟较均衡；
- `500`：更偏向吞吐；
- `1000`：已经出现明显排队膨胀，只适合不敏感于尾延迟的场景。

这符合排队系统的一般特征：并发度提高可以填满处理流水线，但超过有效并行能力后，请求主要是在队列中等待。

### 5. Future 在低在途数下表现较好，高在途数下尾延迟显著恶化

callback 与 Future 对比：

| Max Inflight | Future QPS 相对 callback | Future p99 相对 callback |
| ---: | ---: | ---: |
| 10 | +10.1% | -38.7% |
| 50 | +22.3% | -29.7% |
| 100 | +19.2% | -21.6% |
| 200 | -2.5% | +10.9% |
| 500 | -8.2% | +73.8% |
| 1000 | -13.0% | +201.2% |

结果呈现明显分界：

- max inflight `10–100`：Future 的 QPS 更高，p99 更低；
- 从 `200` 开始：callback 逐渐占优；
- max inflight `1000`：Future QPS 比 callback 低约 `13%`，p99 达到 `290.908 ms`，约为 callback 的 `3.01×`。

Future 在 `10–500` 档位的客户端 CPU 也明显高于 callback，差距约为 `21%–62%`。这可能来自：

- promise/future 状态分配与同步；
- Future 结果收集方式；
- benchmark 对 future ready 的观察顺序；
- 更高的对象管理和唤醒开销。

但当前不能直接把差距全部归因于 Future API 实现。如果 benchmark 按提交顺序依次 `get()`，某个较慢 future 可能阻塞后续已经 ready 的结果被消费，从而放大统计到的尾延迟。要确认真正的框架开销，需要检查或增加：

- ready-order 消费；
- completion queue；
- 每个 future 的实际 promise set 时间；
- 只测 promise/future 包装开销的微基准。

现有数据能够支持的结论是：当前 Future 基准在高在途数下扩展性和尾延迟弱于 callback 基准，但原因仍需进一步拆分。

### 6. 小 payload 由固定 RPC 开销主导，大 payload 逐渐受字节处理能力限制

payload 从 `16 B` 增长到 `1 KiB` 时：

```text
QPS 维持在 3935–4065
p50 维持在 0.799–0.846 ms
```

说明在 `1 KiB` 以内，主要成本不是 payload 字节数，而是每次 RPC 固定开销，例如：

- Protobuf 对象和 frame 处理；
- pending-call 管理；
- socket 系统调用；
- 线程调度与唤醒；
- 服务端任务分发。

从 `4 KiB` 开始，QPS 逐渐下降，而应用层 payload 吞吐继续上升：

| Payload | QPS | MiB/s | p50 | p99 |
| ---: | ---: | ---: | ---: | ---: |
| 4 KiB | 3259 | 12.73 | 0.942 ms | 5.304 ms |
| 16 KiB | 2854 | 44.60 | 1.078 ms | 6.005 ms |
| 64 KiB | 2039 | 127.44 | 1.614 ms | 6.926 ms |
| 256 KiB | 820 | 205.08 | 4.329 ms | 14.055 ms |
| 1 MiB | 235 | 235.56 | 15.148 ms | 48.985 ms |

这表明固定成本正在被摊薄，瓶颈逐渐转向：

- Protobuf 序列化与解析；
- 用户态内存复制；
- frame 拼装；
- socket 读写；
- loopback 和内核缓冲区吞吐。

`256 KiB` 到 `1 MiB` 时，应用 payload 吞吐从约 `205 MiB/s` 增长到 `236 MiB/s`，增幅开始收窄，可以视为当前测试环境下接近大包处理平台期的迹象。

该 `MiB/s` 不包含响应、RPC header、Protobuf 编码和 TCP/IP 开销，因此不能等价为线路吞吐。

### 7. 当前实现展示了较清楚的性能特征

从这组数据可以归纳出当前客户端的几个特征：

1. **支持单连接多请求复用。**  
   单 channel 在多调用线程下吞吐明显高于串行基线。

2. **小包场景不是靠增加连接数扩展。**  
   当前环境中，多连接增加了开销，没有提高吞吐。

3. **同步多线程和异步高在途都能提高吞吐。**  
   代价是更高的排队延迟，尤其是 p99。

4. **callback 的高并发扩展性优于当前 Future 基准。**  
   Future 在低并发下不差，但高在途时尾延迟增长更快。

5. **小 payload 主要受固定 RPC 开销影响。**  
   大 payload 则逐渐受序列化、复制和 socket 吞吐限制。

6. **当前最高 QPS 不是最佳工作点。**  
   callback 在 inflight `1000` 时达到约 `21.6k QPS`，但 p99 已接近 `100 ms`；更合理的工作点取决于延迟目标。

### 8. 还不能从现有数据得出的结论

当前数据不足以证明：

- `my_rpc` 比 gRPC、brpc 或其他框架更快或更慢；
- pool size `1` 在所有机器和网络环境中都最优；
- Future API 本身必然比 callback 慢；
- provider、client、网络或 Protobuf 中哪一项是最终瓶颈；
- 单次运行中的小幅差异具有统计意义。

要形成更可信的性能结论，下一轮应补充：

1. 每个配置运行 `5–10` 轮，报告中位数和波动范围；
2. 分别记录 client 与 provider CPU；
3. 使用 `perf record` / flame graph 定位热点；
4. 扫描 provider worker 数；
5. 增加跨机器网络测试；
6. 分离 codec、transport、provider dispatch 和 callback/Future 包装的微基准；
7. 记录 context switch、系统调用次数和内存分配次数；
8. 对 Future 使用不同结果消费策略进行对照。

原始结果如下。



### rpc_sync_single_benchmark

```text
mode=sync_single_channel
ip=127.0.0.1
port=8000
warmup_count=1000
request_count=10000
completed=10000
success=10000
failed=0
error_rate=0%
elapsed_seconds=6.63606
qps=1506.92
p50_latency_ms=0.488
p90_latency_ms=1.119
p99_latency_ms=4.011
max_latency_ms=42.153
client_cpu_percent=33.0296
```

### rpc_pool_size_benchmark

```text
pool_size_scaling_benchmark
ip=127.0.0.1
port=8000
total_requests=100000
client_threads=8
warmup_count=1000
```

| Mode | Client Threads | Provider Workers | Pool Size | Requests | Success | Failed | QPS | p50 | p90 | p99 | Max | Error Rate | Client CPU |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| sync pool | 8 | 4 | 1 | 100000 | 100000 | 0 | 4943 | 1.329 ms | 2.609 ms | 5.953 ms | 54.905 ms | 0% | 71.8993% |
| sync pool | 8 | 4 | 2 | 100000 | 100000 | 0 | 4759 | 1.388 ms | 2.748 ms | 6.063 ms | 61.858 ms | 0% | 77.9904% |
| sync pool | 8 | 4 | 4 | 100000 | 100000 | 0 | 4028 | 1.656 ms | 3.22 ms | 7.073 ms | 52.566 ms | 0% | 80.9544% |
| sync pool | 8 | 4 | 8 | 100000 | 100000 | 0 | 3894 | 1.746 ms | 3.421 ms | 7.521 ms | 63.438 ms | 0% | 83.7469% |
| sync pool | 8 | 4 | 16 | 100000 | 100000 | 0 | 3952 | 1.698 ms | 3.306 ms | 7.058 ms | 54.986 ms | 0% | 81.3652% |

### rpc_sync_threads_benchmark

```text
client_threads_scaling_benchmark
ip=127.0.0.1
port=8000
total_requests=100000
fixed_pool_size=4
warmup_count=1000
```

| Mode | Pool Size | Client Threads | Provider Workers | Requests | Success | Failed | QPS | p50 | p90 | p99 | Max | Error Rate | Client CPU |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| sync pool | 4 | 1 | 4 | 100000 | 100000 | 0 | 1573 | 0.472 ms | 1.066 ms | 3.923 ms | 43.499 ms | 0% | 37.9571% |
| sync pool | 4 | 2 | 4 | 100000 | 100000 | 0 | 2545 | 0.557 ms | 1.406 ms | 4.227 ms | 48.809 ms | 0% | 55.4681% |
| sync pool | 4 | 4 | 4 | 100000 | 100000 | 0 | 3462 | 0.881 ms | 2.014 ms | 5.041 ms | 47.949 ms | 0% | 71.9619% |
| sync pool | 4 | 8 | 4 | 100000 | 100000 | 0 | 4022 | 1.653 ms | 3.171 ms | 6.98 ms | 60.268 ms | 0% | 79.2832% |
| sync pool | 4 | 16 | 4 | 100000 | 100000 | 0 | 5448 | 2.523 ms | 4.851 ms | 10.576 ms | 41.898 ms | 0% | 84.3422% |
| sync pool | 4 | 32 | 4 | 100000 | 100000 | 0 | 7181 | 3.938 ms | 7.176 ms | 12.613 ms | 68.575 ms | 0% | 101.132% |

### rpc_async_callback_benchmark

```text
async_callback_benchmark
ip=127.0.0.1
port=8000
total_requests=100000
fixed_pool_size=4
client_threads=4
warmup_count=1000
```

| Mode | Pool Size | Client Threads | Max Inflight | Provider Workers | Requests | Success | Failed | QPS | p50 | p90 | p99 | Max | Error Rate | Client CPU |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| async callback | 4 | 4 | 10 | 4 | 100000 | 100000 | 0 | 3935 | 1.953 ms | 4.131 ms | 8.987 ms | 49.179 ms | 0% | 85.2159% |
| async callback | 4 | 4 | 50 | 4 | 100000 | 100000 | 0 | 6804 | 5.723 ms | 11.414 ms | 23.068 ms | 53.595 ms | 0% | 77.6233% |
| async callback | 4 | 4 | 100 | 4 | 100000 | 100000 | 0 | 9080 | 7.958 ms | 17.229 ms | 33.317 ms | 70.638 ms | 0% | 81.4567% |
| async callback | 4 | 4 | 200 | 4 | 100000 | 100000 | 0 | 14012 | 9.342 ms | 21.019 ms | 43.571 ms | 56.047 ms | 0% | 92.0093% |
| async callback | 4 | 4 | 500 | 4 | 100000 | 100000 | 0 | 18225 | 14.874 ms | 35.216 ms | 61.023 ms | 82.295 ms | 0% | 96.7654% |
| async callback | 4 | 4 | 1000 | 4 | 100000 | 100000 | 0 | 21606 | 20.623 ms | 43.39 ms | 96.598 ms | 140.57 ms | 0% | 110.439% |

### rpc_future_benchmark

```text
future_benchmark
ip=127.0.0.1
port=8000
total_requests=100000
fixed_pool_size=4
client_threads=4
warmup_count=1000
```

| Mode | Pool Size | Client Threads | Max Inflight | Provider Workers | Requests | Success | Failed | QPS | p50 | p90 | p99 | Max | Error Rate | Client CPU |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| future | 4 | 4 | 10 | 4 | 100000 | 100000 | 0 | 4334 | 1.657 ms | 3.166 ms | 5.51 ms | 54.449 ms | 0% | 127.243% |
| future | 4 | 4 | 50 | 4 | 100000 | 100000 | 0 | 8321 | 5.122 ms | 9.525 ms | 16.218 ms | 73.633 ms | 0% | 125.508% |
| future | 4 | 4 | 100 | 4 | 100000 | 100000 | 0 | 10822 | 7.731 ms | 14.509 ms | 26.135 ms | 80.052 ms | 0% | 128.924% |
| future | 4 | 4 | 200 | 4 | 100000 | 100000 | 0 | 13668 | 12.631 ms | 22.49 ms | 48.314 ms | 179.041 ms | 0% | 132.218% |
| future | 4 | 4 | 500 | 4 | 100000 | 100000 | 0 | 16737 | 25.096 ms | 50.656 ms | 106.084 ms | 253.147 ms | 0% | 117.297% |
| future | 4 | 4 | 1000 | 4 | 100000 | 100000 | 0 | 18791 | 33.386 ms | 120.997 ms | 290.908 ms | 585.986 ms | 0% | 97.2741% |

### rpc_payload_size_benchmark

```text
payload_size_benchmark
ip=127.0.0.1
port=8000
total_requests=100000
pool_size=4
client_threads=4
warmup_count=1000
```

| Mode | Payload | Pool Size | Client Threads | Provider Workers | Requests | Success | Failed | QPS | MiB/s | p50 | p90 | p99 | Max | Error Rate | Client CPU |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| sync payload | 16 B | 4 | 4 | 4 | 100000 | 100000 | 0 | 3990 | 0.0608909 | 0.813 ms | 1.688 ms | 3.941 ms | 67.02 ms | 0% | 79.3782% |
| sync payload | 128 B | 4 | 4 | 4 | 100000 | 100000 | 0 | 3935 | 0.480359 | 0.846 ms | 1.667 ms | 3.438 ms | 64.802 ms | 0% | 84.9578% |
| sync payload | 1 KiB | 4 | 4 | 4 | 100000 | 100000 | 0 | 4065 | 3.9701 | 0.799 ms | 1.623 ms | 3.554 ms | 58.359 ms | 0% | 88.884% |
| sync payload | 4 KiB | 4 | 4 | 4 | 100000 | 100000 | 0 | 3259 | 12.7314 | 0.942 ms | 2.161 ms | 5.304 ms | 51.603 ms | 0% | 74.0661% |
| sync payload | 16 KiB | 4 | 4 | 4 | 100000 | 100000 | 0 | 2854 | 44.6034 | 1.078 ms | 2.411 ms | 6.005 ms | 68.205 ms | 0% | 70.0397% |
| sync payload | 64 KiB | 4 | 4 | 4 | 100000 | 100000 | 0 | 2039 | 127.441 | 1.614 ms | 3.275 ms | 6.926 ms | 57.526 ms | 0% | 69.8556% |
| sync payload | 256 KiB | 4 | 4 | 4 | 100000 | 100000 | 0 | 820 | 205.084 | 4.329 ms | 7.953 ms | 14.055 ms | 74.342 ms | 0% | 60.802% |
| sync payload | 1 MiB | 4 | 4 | 4 | 100000 | 100000 | 0 | 235 | 235.556 | 15.148 ms | 26.65 ms | 48.985 ms | 192.024 ms | 0% | 58.3038% |