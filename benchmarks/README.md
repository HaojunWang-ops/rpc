# my_rpc Benchmarks

本目录包含 `my_rpc` 的客户端侧性能基准程序，用于观察同步调用、连接池大小、客户端线程数、异步在途请求数、future API 和请求体大小对吞吐与延迟的影响。

所有 benchmark 都依赖仓库里的演示 `UserService`：

- `Login`：小请求/小响应，用于同步、异步 callback 和 future 基准。
- `Register`：payload benchmark 会把指定大小的数据放入 `RegisterRequest.password` 字段。

## 构建

建议使用 Release 构建 benchmark：

```bash
cmake -S . -B build-benchmark -DCMAKE_BUILD_TYPE=Release -DMYRPC_BUILD_BENCHMARKS=ON
cmake --build build-benchmark -j
```

benchmark 目标由 `benchmarks/CMakeLists.txt` 注册，生成在：

```text
build-benchmark/benchmarks/
```

## 运行前准备

先启动 provider：

```bash
./build-benchmark/examples/provider warn
```

默认 provider 在 `examples/provider_main.cc` 中使用：

```cpp
RpcProvider provider(4);
```

因此 benchmark 输出中的 `Provider Workers` 默认是 `4`，只是结果标签；benchmark 命令行参数不会修改服务端 worker 数。如果要比较不同服务端 worker 数，需要启动对应配置的 provider。

所有 benchmark 默认连接：

```text
127.0.0.1:8000
```

所有 benchmark 默认 RPC timeout 为 `3000 ms`，当前没有命令行参数修改 timeout。`warmup_count` 只用于预热，不计入最终统计。

## Benchmark 列表

| 程序 | 覆盖内容 | 默认扫描维度 | 参数 |
|---|---|---|---|
| `rpc_sync_single_benchmark` | 单 TCP channel、同步串行 `Login` | 无，单配置 | `[ip] [port] [request_count] [warmup_count]` |
| `rpc_pool_size_benchmark` | 固定客户端线程数，比较同步 `Login` 在不同连接池大小下的表现 | pool size: `1, 2, 4, 8, 16` | `[ip] [port] [total_requests] [client_threads] [warmup_count]` |
| `rpc_sync_threads_benchmark` | 固定连接池大小，比较同步 `Login` 在不同客户端线程数下的表现 | client threads: `1, 2, 4, 8, 16, 32` | `[ip] [port] [total_requests] [pool_size] [warmup_count]` |
| `rpc_async_callback_benchmark` | 异步 callback 风格 `Login`，比较最大在途请求数 | max inflight: `10, 50, 100, 200, 500, 1000` | `[ip] [port] [total_requests] [pool_size] [client_threads] [warmup_count]` |
| `rpc_future_benchmark` | future 风格 `Login`，比较最大在途请求数 | max inflight: `10, 50, 100, 200, 500, 1000` | `[ip] [port] [total_requests] [pool_size] [client_threads] [warmup_count]` |
| `rpc_payload_size_benchmark` | 同步 `Register`，比较请求 payload 大小对吞吐与延迟的影响 | payload: `16 B, 128 B, 1 KiB, 4 KiB, 16 KiB, 64 KiB, 256 KiB, 1 MiB` | `[ip] [port] [total_requests] [pool_size] [client_threads] [warmup_count]` |

## 示例命令

单 channel 同步串行小包：

```bash
./build-benchmark/benchmarks/rpc_sync_single_benchmark 127.0.0.1 8000 10000 1000
```

固定 `8` 个客户端线程，扫描连接池大小：

```bash
./build-benchmark/benchmarks/rpc_pool_size_benchmark 127.0.0.1 8000 100000 8 1000
```

固定连接池大小为 `4`，扫描客户端线程数：

```bash
./build-benchmark/benchmarks/rpc_sync_threads_benchmark 127.0.0.1 8000 100000 4 1000
```

异步 callback，连接池大小 `4`，提交线程数 `4`，扫描最大在途请求数：

```bash
./build-benchmark/benchmarks/rpc_async_callback_benchmark 127.0.0.1 8000 100000 4 4 1000
```

future API，连接池大小 `4`，客户端线程数 `4`，扫描最大在途请求数：

```bash
./build-benchmark/benchmarks/rpc_future_benchmark 127.0.0.1 8000 100000 4 4 1000
```

payload 大小测试，连接池大小 `4`，客户端线程数 `4`，扫描请求体大小：

```bash
./build-benchmark/benchmarks/rpc_payload_size_benchmark 127.0.0.1 8000 100000 4 4 1000
```

## 输出格式

`rpc_sync_single_benchmark` 输出 key/value：

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

其他 benchmark 输出 Markdown 表格，方便直接复制到报告或 README 中。

## 指标说明

- `Requests` / `request_count`：压测阶段请求数，不包含预热请求。
- `Success` / `success`：RPC controller 未失败，并且业务响应 `success == true` 的请求数。
- `Failed` / `failed`：RPC 失败、超时、连接失败、响应解析失败或业务响应 `success != true` 的请求数。
- `QPS`：完成请求数除以压测阶段墙钟时间。
- `p50` / `p90` / `p99` / `Max`：客户端侧端到端延迟。
- `Error Rate` / `error_rate`：`Failed / (Success + Failed)`。
- `Client CPU` / `client_cpu_percent`：客户端进程 CPU 时间除以墙钟时间，多线程时可能超过 `100%`。
- `MiB/s`：仅 `rpc_payload_size_benchmark` 输出，按请求 payload 字节数乘以完成请求数计算，不包含 RPC header、protobuf 编码开销和响应流量。

延迟统计口径：

- 同步 benchmark：从调用 `stub.Login()` 或 `stub.Register()` 到返回。
- 异步 callback benchmark：从提交异步调用到 callback 运行。
- future benchmark：从提交 future 调用到 future ready 并被消费。

## 使用建议

- 用 Release 构建观察性能趋势；Debug/ASAN/TSAN 构建更适合查问题，不适合作为性能结果。
- 固定机器、CPU governor、provider worker 数、日志级别和端口占用情况后再比较数据。
- 一次只运行一个 benchmark，避免多个客户端同时压同一个 provider。
- 本地 loopback 结果主要反映框架开销和本机调度，不等价于跨机器网络性能。
- payload benchmark 的 `1 MiB` 请求会显著放大内存复制、protobuf 序列化和 socket 写入成本，建议先用较小 `total_requests` 试跑。

## 本次测试结果（2026-07-12）

本次结果是一次本机 loopback 试跑，仅用于记录当前机器和当前代码状态下的表现。

- 记录时间：`2026-07-12 15:26:32 CST`
- 系统：`Linux vm 6.17.0-35-generic #35~24.04.1-Ubuntu SMP PREEMPT_DYNAMIC Tue May 26 19:30:42 UTC 2 x86_64`
- 构建：`cmake -S . -B build-benchmark -DCMAKE_BUILD_TYPE=Release -DMYRPC_BUILD_BENCHMARKS=ON`
- 服务端：`./build-benchmark/examples/provider warn`
- 服务地址：`127.0.0.1:8000`
- Provider workers：`4`
- Benchmark 参数：未额外传参，使用各程序默认值。

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
