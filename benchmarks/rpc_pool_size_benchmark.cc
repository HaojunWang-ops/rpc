#include "rpc_channel_pool.h"
#include "rpc_controller.h"

#include "user.pb.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
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

        double user = usage.ru_utime.tv_sec + usage.ru_utime.tv_usec / 1000000.0;
        double sys = usage.ru_stime.tv_sec + usage.ru_stime.tv_usec / 1000000.0;
        return user + sys;
    }

    int64_t percentile(const std::vector<int64_t> &sorted, double p)
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

    struct BenchOptions
    {
        std::string ip = "127.0.0.1";
        uint16_t port = 8000;

        int client_threads = 8;
        int provider_workers = 4; // 只用于输出说明，真正 worker 数由 provider 启动参数决定

        int warmup_count = 1000;
        int total_requests = 100000;
        int timeout_ms = 3000;

        std::vector<int> pool_sizes{1, 2, 4, 8, 16};
    };

    struct BenchResult
    {
        int pool_size = 0;
        int client_threads = 0;
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

    bool doLoginOnce(demo::UserService_Stub &stub)
    {
        demo::LoginRequest request;
        demo::LoginResponse response;
        SimpleRpcController controller;

        request.set_name("haojun");
        request.set_password("123456");

        stub.Login(&controller, &request, &response, nullptr);

        return !controller.Failed() && response.success();
    }

    void warmup(RpcChannelPool &pool, int warmup_count)
    {
        demo::UserService_Stub stub(&pool);

        for (int i = 0; i < warmup_count; ++i)
        {
            doLoginOnce(stub);
        }
    }

    BenchResult runOnePoolSize(const BenchOptions &options, int pool_size)
    {
        RpcChannelPool pool(options.ip, options.port, pool_size);

        pool.setTimeoutMs(options.timeout_ms);
        
        if (!pool.start())
        {
            std::cerr << "pool start failed, pool_size=" << pool_size << "\n";

            BenchResult failed;
            failed.pool_size = pool_size;
            failed.client_threads = options.client_threads;
            failed.provider_workers = options.provider_workers;
            failed.total_requests = options.total_requests;
            failed.failed_count = options.total_requests;
            failed.error_rate = 100.0;
            return failed;
        }

        warmup(pool, options.warmup_count);

        std::atomic<int64_t> success_count{0};
        std::atomic<int64_t> failed_count{0};

        std::vector<std::vector<int64_t>> thread_latencies(
            static_cast<size_t>(options.client_threads));

        int calls_per_thread = options.total_requests / options.client_threads;
        int remainder = options.total_requests % options.client_threads;

        double cpu_start = getProcessCpuSeconds();
        auto wall_start = std::chrono::steady_clock::now();

        std::vector<std::thread> threads;
        threads.reserve(static_cast<size_t>(options.client_threads));

        for (int t = 0; t < options.client_threads; ++t)
        {
            int calls = calls_per_thread + (t < remainder ? 1 : 0);

            threads.emplace_back([&pool,
                                  &thread_latencies,
                                  &success_count,
                                  &failed_count,
                                  t,
                                  calls]()
                                 {
            demo::UserService_Stub stub(&pool);
            auto& local_latencies = thread_latencies[static_cast<size_t>(t)];
            local_latencies.reserve(static_cast<size_t>(calls));

            for (int i = 0; i < calls; ++i)
            {
                auto start = std::chrono::steady_clock::now();

                bool ok = doLoginOnce(stub);

                auto end = std::chrono::steady_clock::now();

                int64_t latency_us =
                    std::chrono::duration_cast<std::chrono::microseconds>(
                        end - start).count();

                local_latencies.push_back(latency_us);

                if (ok)
                {
                    success_count.fetch_add(1, std::memory_order_relaxed);
                }
                else
                {
                    failed_count.fetch_add(1, std::memory_order_relaxed);
                }
            } });
        }

        for (auto &th : threads)
        {
            th.join();
        }

        auto wall_end = std::chrono::steady_clock::now();
        double cpu_end = getProcessCpuSeconds();

        pool.stop();

        BenchResult result;
        result.pool_size = pool_size;
        result.client_threads = options.client_threads;
        result.provider_workers = options.provider_workers;
        result.total_requests = options.total_requests;

        result.success_count = success_count.load(std::memory_order_relaxed);
        result.failed_count = failed_count.load(std::memory_order_relaxed);

        int64_t completed = result.success_count + result.failed_count;

        result.elapsed_seconds =
            std::chrono::duration<double>(wall_end - wall_start).count();

        if (completed > 0 && result.elapsed_seconds > 0.0)
        {
            result.qps = static_cast<double>(completed) / result.elapsed_seconds;
            result.error_rate =
                static_cast<double>(result.failed_count) /
                static_cast<double>(completed) * 100.0;
        }

        std::vector<int64_t> latencies;
        latencies.reserve(static_cast<size_t>(completed));

        for (auto &local : thread_latencies)
        {
            latencies.insert(latencies.end(), local.begin(), local.end());
        }

        std::sort(latencies.begin(), latencies.end());

        result.p50_ms = usToMs(percentile(latencies, 50.0));
        result.p90_ms = usToMs(percentile(latencies, 90.0));
        result.p99_ms = usToMs(percentile(latencies, 99.0));
        result.max_ms = latencies.empty() ? 0.0 : usToMs(latencies.back());

        double cpu_seconds = cpu_end - cpu_start;
        if (result.elapsed_seconds > 0.0)
        {
            result.client_cpu_percent =
                cpu_seconds / result.elapsed_seconds * 100.0;
        }

        return result;
    }

    BenchOptions parseArgs(int argc, char *argv[])
    {
        BenchOptions options;

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
            options.client_threads = std::stoi(argv[4]);
        }

        if (argc >= 6)
        {
            options.warmup_count = std::stoi(argv[5]);
        }

        return options;
    }

    void printMarkdownHeader()
    {
        std::cout
            << "| Mode | Client Threads | Provider Workers | Pool Size | Requests | "
            << "Success | Failed | QPS | p50 | p90 | p99 | Max | Error Rate | Client CPU |\n";

        std::cout
            << "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n";
    }

    void printMarkdownRow(const BenchResult &r)
    {
        std::cout
            << "| sync pool | "
            << r.client_threads << " | "
            << r.provider_workers << " | "
            << r.pool_size << " | "
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

int main(int argc, char *argv[])
{
    BenchOptions options = parseArgs(argc, argv);

    std::cout << "pool_size_scaling_benchmark\n";
    std::cout << "ip=" << options.ip << "\n";
    std::cout << "port=" << options.port << "\n";
    std::cout << "total_requests=" << options.total_requests << "\n";
    std::cout << "client_threads=" << options.client_threads << "\n";
    std::cout << "warmup_count=" << options.warmup_count << "\n\n";

    printMarkdownHeader();

    for (int pool_size : options.pool_sizes)
    {
        BenchResult result = runOnePoolSize(options, pool_size);
        printMarkdownRow(result);
    }

    return 0;
}