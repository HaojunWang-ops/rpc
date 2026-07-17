# my_rpc 协作指南

这是一个 C++17 RPC 框架项目。修改生命周期、timeout、repair、stop 或 callback 行为前，先阅读 `README.md`、`docs/concurrency.md` 和 `docs/testing.md`。

## 构建与测试

Debug unit、integration 与确定性竞态测试：

```bash
cmake -S . -B build-debug \
  -DCMAKE_BUILD_TYPE=Debug \
  -DMYRPC_BUILD_TESTS=ON \
  -DMYRPC_BUILD_RACE_TESTS=ON \
  -DMYRPC_ENABLE_TEST_HOOKS=ON
cmake --build build-debug -j
ctest --test-dir build-debug -L unit --output-on-failure
ctest --test-dir build-debug -L integration --output-on-failure
ctest --test-dir build-debug -L race --output-on-failure
```

ASAN 确定性竞态测试：

```bash
cmake -S . -B build-asan \
  -DCMAKE_BUILD_TYPE=Debug \
  -DMYRPC_BUILD_RACE_TESTS=ON \
  -DMYRPC_ENABLE_TEST_HOOKS=ON \
  -DMYRPC_ENABLE_ASAN=ON
cmake --build build-asan -j
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 \
ctest --test-dir build-asan -L race --output-on-failure
```

TSAN 确定性竞态测试：

```bash
sudo sysctl -w vm.mmap_rnd_bits=28

cmake -S . -B build-tsan \
  -DCMAKE_BUILD_TYPE=Debug \
  -DMYRPC_BUILD_RACE_TESTS=ON \
  -DMYRPC_ENABLE_TEST_HOOKS=ON \
  -DMYRPC_ENABLE_TSAN=ON
cmake --build build-tsan -j
TSAN_OPTIONS=halt_on_error=1:second_deadlock_stack=1 \
ctest --test-dir build-tsan -L race --output-on-failure
```

默认测试命令有意不运行 stress 测试。使用 `-DMYRPC_BUILD_STRESS_TESTS=ON` 构建，并串行执行；其 CTest 参数定义在 `tests/stress/CMakeLists.txt`。

ASAN 与 TSAN 互斥，应使用独立构建目录。本地 TSAN 的 `sysctl` 命令会修改主机地址随机化策略，仅在报告 `unexpected memory mapping` 的环境中需要；GitHub Actions CI 不需要此设置。

## 工程规则

- 优先使用小范围、局部补丁。
- 除非明确要求，不重写大型模块。
- 除非必要，不修改公共接口。
- 除非明确要求，不修改线路协议格式。
- 修改并发逻辑时，说明对象所有权、生命周期前提、锁顺序和 atomic 内存序假设。
- 修改 RPC 错误路径时，确保 `done` callback 恰好执行一次。
- 修改 C++ 代码后，运行最小相关构建和测试命令。
- 测试失败时，报告准确命令和最小可能原因。
- 公共契约、CMake option、测试标签或生命周期行为变化时，同步更新 `README.md`、`docs/concurrency.md` 或 `docs/testing.md`。
