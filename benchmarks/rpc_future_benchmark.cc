#include "rpc_channel_pool.h"

#include "user.pb.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <future>
#include <iostream>
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

const google::protobuf::MethodDescriptor* loginMethod()
{
    return demo::UserService::descriptor()->FindMethodByName("Login");
}

demo::LoginRequest makeLoginRequest()
{
    demo::LoginRequest request;
    request.set_name("haojun");
    request.set_password("123456");
    return request;
}

struct FutureBenchOptions
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

struct FutureBenchResult
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

struct FutureCall
{
    std::future<RpcFutureResult<demo::LoginResponse>> future;
    std::chrono::steady_clock::time_point start;
};

struct ThreadRunResult
{
    int64_t success_count = 0;
    int64_t failed_count = 0;
    std::vector<int64_t> latencies_us;
};

void submitFutureCall(RpcChannelPool& pool, std::vector<FutureCall>& inflight)
{
    FutureCall call;
    call.start = std::chrono::steady_clock::now();
    call.future = pool.CallMethodFuture<demo::LoginResponse>(
        loginMethod(),
        makeLoginRequest());

    inflight.push_back(std::move(call));
}

size_t nextReadyFutureIndex(std::vector<FutureCall>& inflight)
{
    while (true)
    {
        for (int i = 0; i < inflight.size(); i++)
        {
            if (inflight[i].future.wait_for(std::chrono::milliseconds(0)) == std::future_status::ready)
            {
                return i;
            }
        }

        std::this_thread::yield();
    }
}

FutureCall takeFutureCall(std::vector<FutureCall>& inflight, size_t index)
{
    FutureCall call = std::move(inflight[index]);

    if (index + 1 != inflight.size())
    {
        inflight[index] = std::move(inflight.back());
    }

    inflight.pop_back();
    return call;
}

void finishFutureCall(FutureCall call, ThreadRunResult& result)
{
    auto future_result = call.future.get();
    auto end = std::chrono::steady_clock::now();

    int64_t latency_us =
        std::chrono::duration_cast<std::chrono::microseconds>(
            end - call.start)
            .count();

    result.latencies_us.push_back(latency_us);

    if (future_result.ok && future_result.response.success())
    {
        ++result.success_count;
    }
    else
    {
        ++result.failed_count;
    }
}

ThreadRunResult runFutureCalls(RpcChannelPool& pool,
                               int request_count,
                               int max_inflight)
{
    ThreadRunResult result;
    result.latencies_us.reserve(static_cast<size_t>(request_count));

    if (request_count <= 0)
    {
        return result;
    }

    if (max_inflight <= 0)
    {
        max_inflight = 1;
    }

    std::vector<FutureCall> inflight;
    inflight.reserve(static_cast<size_t>(max_inflight));

    int submitted = 0;
    int completed = 0;

    while (completed < request_count)
    {
        while (submitted < request_count &&
               static_cast<int>(inflight.size()) < max_inflight)
        {
            submitFutureCall(pool, inflight);
            ++submitted;
        }

        size_t ready_index = nextReadyFutureIndex(inflight);
        finishFutureCall(takeFutureCall(inflight, ready_index), result);
        ++completed;
    }

    return result;
}

void warmup(RpcChannelPool& pool, int warmup_count)
{
    if (warmup_count <= 0)
    {
        return;
    }

    runFutureCalls(pool, warmup_count, 10);
}

int boundedThreadCount(int configured_threads, int total_requests, int concurrency)
{
    int threads = std::max(1, configured_threads);
    threads = std::min(threads, std::max(1, total_requests));
    threads = std::min(threads, std::max(1, concurrency));
    return threads;
}

FutureBenchResult runFutureOneConfig(const FutureBenchOptions& options,
                                     int concurrency)
{
    RpcChannelPool pool(options.ip, options.port, options.pool_size);
    pool.setTimeoutMs(options.timeout_ms);

    int client_threads = boundedThreadCount(
        options.client_threads,
        options.total_requests,
        concurrency);

    FutureBenchResult result;
    result.pool_size = options.pool_size;
    result.client_threads = client_threads;
    result.concurrency = concurrency;
    result.provider_workers = options.provider_workers;
    result.total_requests = options.total_requests;

    if (!pool.start())
    {
        std::cerr << "future pool start failed\n";
        result.failed_count = options.total_requests;
        result.error_rate = 100.0;
        return result;
    }

    warmup(pool, options.warmup_count);

    std::atomic<int64_t> success_count{0};
    std::atomic<int64_t> failed_count{0};

    std::vector<std::vector<int64_t>> thread_latencies(
        static_cast<size_t>(client_threads));

    int requests_per_thread = options.total_requests / client_threads;
    int request_remainder = options.total_requests % client_threads;
    int inflight_per_thread = concurrency / client_threads;
    int inflight_remainder = concurrency % client_threads;

    double cpu_start = getProcessCpuSeconds();
    auto wall_start = std::chrono::steady_clock::now();

    std::vector<std::thread> threads;
    threads.reserve(static_cast<size_t>(client_threads));

    for (int t = 0; t < client_threads; ++t)
    {
        int request_count = requests_per_thread + (t < request_remainder ? 1 : 0);
        int max_inflight = inflight_per_thread + (t < inflight_remainder ? 1 : 0);

        if (max_inflight <= 0)
        {
            max_inflight = 1;
        }

        threads.emplace_back([&pool,
                              &thread_latencies,
                              &success_count,
                              &failed_count,
                              t,
                              request_count,
                              max_inflight]() {
            ThreadRunResult local = runFutureCalls(
                pool,
                request_count,
                max_inflight);

            success_count.fetch_add(local.success_count,
                                    std::memory_order_relaxed);
            failed_count.fetch_add(local.failed_count,
                                   std::memory_order_relaxed);

            thread_latencies[static_cast<size_t>(t)] =
                std::move(local.latencies_us);
        });
    }

    for (auto& thread : threads)
    {
        thread.join();
    }

    auto wall_end = std::chrono::steady_clock::now();
    double cpu_end = getProcessCpuSeconds();

    pool.stop();

    result.success_count = success_count.load(std::memory_order_relaxed);
    result.failed_count = failed_count.load(std::memory_order_relaxed);

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

    std::vector<int64_t> latencies;
    latencies.reserve(static_cast<size_t>(completed));

    for (auto& local : thread_latencies)
    {
        latencies.insert(latencies.end(), local.begin(), local.end());
    }

    std::sort(latencies.begin(), latencies.end());

    result.p50_ms = usToMs(percentile(latencies, 50.0));
    result.p90_ms = usToMs(percentile(latencies, 90.0));
    result.p99_ms = usToMs(percentile(latencies, 99.0));
    result.max_ms =
        latencies.empty() ? 0.0 : usToMs(latencies.back());

    double cpu_seconds = cpu_end - cpu_start;
    if (result.elapsed_seconds > 0.0)
    {
        result.client_cpu_percent =
            cpu_seconds / result.elapsed_seconds * 100.0;
    }

    return result;
}

FutureBenchOptions parseArgs(int argc, char* argv[])
{
    FutureBenchOptions options;

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

void printMarkdownRow(const FutureBenchResult& r)
{
    std::cout
        << "| future | "
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
    FutureBenchOptions options = parseArgs(argc, argv);

    std::cout << "future_benchmark\n";
    std::cout << "ip=" << options.ip << "\n";
    std::cout << "port=" << options.port << "\n";
    std::cout << "total_requests=" << options.total_requests << "\n";
    std::cout << "fixed_pool_size=" << options.pool_size << "\n";
    std::cout << "client_threads=" << options.client_threads << "\n";
    std::cout << "warmup_count=" << options.warmup_count << "\n\n";

    printMarkdownHeader();

    for (int concurrency : options.concurrency_list)
    {
        FutureBenchResult result = runFutureOneConfig(options, concurrency);
        printMarkdownRow(result);
    }

    return 0;
}
