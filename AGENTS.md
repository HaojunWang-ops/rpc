# AGENTS.md
This is a C++17 RPC framework project.

## build
Debug build:
```bash
cmake -S . -B build-debug -DCMAKE_BUILD_TYPE=Debug
cmake --build build-debug -j
```
Run tests:
```bash
ctest --test-dir build-debug --output-on-failure
```
ASAN build:
```bash
cmake -S . -B build-asan -DCMAKE_BUILD_TYPE=Debug -DMYRPC_ENABLE_ASAN=ON
cmake --build build-asan -j
ctest --test-dir build-asan --output-on-failure
```
TSAN build:
```bash
cmake -S . -B build-tsan -DCMAKE_BUILD_TYPE=Debug -DMYRPC_ENABLE_TSAN=ON
cmake --build build-tsan -j
ctest --test-dir build-tsan --output-on-failure
```

## Enginerring rules
+ Prefer small, local patches.
+ Do not rewrite large modules unless explicitly requested.
+ Do not change public interfaces unless necessary.
+ Do not change protocol format unless explicitly requested.
+ For concurrency changes, explain:
    + object ownership
    + lifetime assumptions
    + lock ordering
    + atomic memory order assumptions
+ For RPC error-path changes, ensure done callbacks are called exactly once.
+ After modifying C++ code, run the smallest relevant build/test command.
+ If tests fail, report the exact failing command and smallest suspected cause.
