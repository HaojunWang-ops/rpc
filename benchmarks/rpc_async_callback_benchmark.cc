#include "rpc_channel_pool.h"
#include "rpc_controller.h"
#include "rpc_closure.h"

#include "user.pb.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <sys/resource.h>
#include <thread>
#include <vector>

namespace
{

double getProcessCpuSeconds()
{
    struct rusage usage;
    if (::getrusage(RUSAGE_SELF, &usage) != 0)
    {
        return 0.0;
    }

    double user =
        static_cast<double>(usage.ru_utime.tv_sec) +
        static_cast<double>(usage.ru_utime.tv_usec) / 1000000.0;

    double sys =
        static_cast<double>(usage.ru_stime.tv_sec) +
        static_cast<double>(usage.ru_stime.tv_usec) / 1000000.0;

    return user + sys;
}

int64_t percentile(const std::vector<int64_t>& sorted, double p)
{
    if (sorted.empty())
    {
        return 0;
    }

    size_t index = static_cast<size_t>(
        std::ceil(p / 100.0 * static_cast<double>(sorted.size())));

    if (index == 0)
    {
        index = 1;
    }

    index -= 1;

    if (index >= sorted.size())
    {
        index = sorted.size() - 1;
    }

    return sorted[index];
}

double usToMs(int64_t us)
{
    return static_cast<double>(us) / 1000.0;
}

struct AsyncBenchOptions
{
    std::string ip = "127.0.0.1";
    uint16_t port = 8000;

    int pool_size = 4;
    int provider_workers = 4;
    int client_threads = 4;

    int total_requests = 100000;
    int warmup_count = 1000;
    int timeout_ms = 3000;

    std::vector<int> concurrency_list{10, 50, 100, 200, 500, 1000};
};

struct AsyncBenchResult
{
    int pool_size = 0;
    int client_threads = 0;
    int concurrency = 0;
    int provider_workers = 0;
    int total_requests = 0;

    int64_t success_count = 0;
    int64_t failed_count = 0;

    double elapsed_seconds = 0.0;
    double qps = 0.0;
    double error_rate = 0.0;

    double p50_ms = 0.0;
    double p90_ms = 0.0;
    double p99_ms = 0.0;
    double max_ms = 0.0;

    double client_cpu_percent = 0.0;
};

struct AsyncCallState
{
    demo::LoginRequest request;
    demo::LoginResponse response;
    SimpleRpcController controller;

    std::chrono::steady_clock::time_point start;
};

struct AsyncBenchContext
{
    std::mutex mutex;
    std::condition_variable cv;

    int64_t submitted = 0;
    int64_t completed = 0;
    int64_t inflight = 0;

    int64_t success = 0;
    int64_t failed = 0;

    int64_t total_requests = 100000;
    int64_t max_inflight = 100;

    std::vector<int64_t> latencies_us;
};

bool submitOneAsyncCall(demo::UserService_Stub& stub,
                        AsyncBenchContext& ctx)
{
    auto state = std::make_shared<AsyncCallState>();

    state->request.set_name("haojun");
    state->request.set_password("123456");
    state->start = std::chrono::steady_clock::now();

    auto done = SendResponseClosure([state, &ctx]() {
        auto end = std::chrono::steady_clock::now();

        int64_t latency_us =
            std::chrono::duration_cast<std::chrono::microseconds>(
                end - state->start)
                .count();

        bool ok =
            !state->controller.Failed() &&
            state->response.success();

        {
            std::lock_guard<std::mutex> lock(ctx.mutex);

            if (ok)
            {
                ++ctx.success;
            }
            else
            {
                ++ctx.failed;
            }

            ctx.latencies_us.push_back(latency_us);

            --ctx.inflight;
            ++ctx.completed;
        }

        ctx.cv.notify_all();
    });

    stub.Login(&state->controller,
               &state->request,
               &state->response,
               done);

    return true;
}

bool reserveOneInflightSlot(AsyncBenchContext& ctx)
{
    std::unique_lock<std::mutex> lock(ctx.mutex);

    ctx.cv.wait(lock, [&ctx]() {
        return ctx.submitted >= ctx.total_requests ||
               ctx.inflight < ctx.max_inflight;
    });

    if (ctx.submitted >= ctx.total_requests)
    {
        return false;
    }

    ++ctx.submitted;
    ++ctx.inflight;

    return true;
}

void waitAllCompleted(AsyncBenchContext& ctx)
{
    std::unique_lock<std::mutex> lock(ctx.mutex);

    ctx.cv.wait(lock, [&ctx]() {
        return ctx.completed >= ctx.total_requests;
    });
}

void warmup(RpcChannelPool& pool, int warmup_count)
{
    if (warmup_count <= 0)
    {
        return;
    }

    demo::UserService_Stub stub(&pool);

    AsyncBenchContext ctx;
    ctx.total_requests = warmup_count;
    ctx.max_inflight = 10;
    ctx.latencies_us.reserve(static_cast<size_t>(warmup_count));

    while (reserveOneInflightSlot(ctx))
    {
        submitOneAsyncCall(stub, ctx);
    }

    waitAllCompleted(ctx);
}

AsyncBenchResult runAsyncOneConfig(const AsyncBenchOptions& options,
                                   int concurrency)
{
    RpcChannelPool pool(options.ip, options.port, options.pool_size);
    pool.setTimeoutMs(options.timeout_ms);

    AsyncBenchResult result;
    result.pool_size = options.pool_size;
    result.client_threads = options.client_threads;
    result.concurrency = concurrency;
    result.provider_workers = options.provider_workers;
    result.total_requests = options.total_requests;

    if (!pool.start())
    {
        std::cerr << "async pool start failed\n";
        result.failed_count = options.total_requests;
        result.error_rate = 100.0;
        return result;
    }

    warmup(pool, options.warmup_count);

    AsyncBenchContext ctx;
    ctx.max_inflight = concurrency;
    ctx.total_requests = options.total_requests;
    ctx.latencies_us.reserve(static_cast<size_t>(options.total_requests));

    double cpu_start = getProcessCpuSeconds();
    auto wall_start = std::chrono::steady_clock::now();

    std::vector<std::thread> threads;
    threads.reserve(static_cast<size_t>(options.client_threads));

    for (int t = 0; t < options.client_threads; ++t)
    {
        threads.emplace_back([&pool, &ctx]() {
            demo::UserService_Stub stub(&pool);

            while (reserveOneInflightSlot(ctx))
            {
                submitOneAsyncCall(stub, ctx);
            }
        });
    }

    for (auto& thread : threads)
    {
        thread.join();
    }

    waitAllCompleted(ctx);

    auto wall_end = std::chrono::steady_clock::now();
    double cpu_end = getProcessCpuSeconds();

    pool.stop();

    {
        std::lock_guard<std::mutex> lock(ctx.mutex);
        result.success_count = ctx.success;
        result.failed_count = ctx.failed;
    }

    int64_t completed = result.success_count + result.failed_count;

    result.elapsed_seconds =
        std::chrono::duration<double>(wall_end - wall_start).count();

    if (completed > 0 && result.elapsed_seconds > 0.0)
    {
        result.qps =
            static_cast<double>(completed) / result.elapsed_seconds;

        result.error_rate =
            static_cast<double>(result.failed_count) /
            static_cast<double>(completed) * 100.0;
    }

    std::vector<int64_t> sorted_latencies;
    {
        std::lock_guard<std::mutex> lock(ctx.mutex);
        sorted_latencies = ctx.latencies_us;
    }

    std::sort(sorted_latencies.begin(), sorted_latencies.end());

    result.p50_ms = usToMs(percentile(sorted_latencies, 50.0));
    result.p90_ms = usToMs(percentile(sorted_latencies, 90.0));
    result.p99_ms = usToMs(percentile(sorted_latencies, 99.0));
    result.max_ms =
        sorted_latencies.empty() ? 0.0 : usToMs(sorted_latencies.back());

    double cpu_seconds = cpu_end - cpu_start;
    if (result.elapsed_seconds > 0.0)
    {
        result.client_cpu_percent =
            cpu_seconds / result.elapsed_seconds * 100.0;
    }

    return result;
}

AsyncBenchOptions parseArgs(int argc, char* argv[])
{
    AsyncBenchOptions options;

    if (argc >= 2)
    {
        options.ip = argv[1];
    }

    if (argc >= 3)
    {
        options.port = static_cast<uint16_t>(std::stoi(argv[2]));
    }

    if (argc >= 4)
    {
        options.total_requests = std::stoi(argv[3]);
    }

    if (argc >= 5)
    {
        options.pool_size = std::stoi(argv[4]);
    }

    if (argc >= 6)
    {
        options.client_threads = std::stoi(argv[5]);
    }

    if (argc >= 7)
    {
        options.warmup_count = std::stoi(argv[6]);
    }

    return options;
}

void printMarkdownHeader()
{
    std::cout
        << "| Mode | Pool Size | Client Threads | Max Inflight | Provider Workers | Requests | "
        << "Success | Failed | QPS | p50 | p90 | p99 | Max | Error Rate | Client CPU |\n";

    std::cout
        << "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n";
}

void printMarkdownRow(const AsyncBenchResult& r)
{
    std::cout
        << "| async callback | "
        << r.pool_size << " | "
        << r.client_threads << " | "
        << r.concurrency << " | "
        << r.provider_workers << " | "
        << r.total_requests << " | "
        << r.success_count << " | "
        << r.failed_count << " | "
        << static_cast<int64_t>(r.qps) << " | "
        << r.p50_ms << " ms | "
        << r.p90_ms << " ms | "
        << r.p99_ms << " ms | "
        << r.max_ms << " ms | "
        << r.error_rate << "% | "
        << r.client_cpu_percent << "% |\n";
}

} // namespace

int main(int argc, char* argv[])
{
    AsyncBenchOptions options = parseArgs(argc, argv);

    std::cout << "async_callback_benchmark\n";
    std::cout << "ip=" << options.ip << "\n";
    std::cout << "port=" << options.port << "\n";
    std::cout << "total_requests=" << options.total_requests << "\n";
    std::cout << "fixed_pool_size=" << options.pool_size << "\n";
    std::cout << "client_threads=" << options.client_threads << "\n";
    std::cout << "warmup_count=" << options.warmup_count << "\n\n";

    printMarkdownHeader();

    for (int concurrency : options.concurrency_list)
    {
        AsyncBenchResult result = runAsyncOneConfig(options, concurrency);
        printMarkdownRow(result);
    }

    return 0;
}