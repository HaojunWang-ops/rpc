#include "rpc_channel_pool.h"
#include "rpc_controller.h"

#include "user.pb.h"

#include <iostream>
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

std::string makePayload(size_t size_bytes)
{
    return std::string(size_bytes, 'x');
}

struct PayloadBenchOptions
{
    std::string ip = "127.0.0.1";
    uint16_t port = 8000;

    int pool_size = 4;
    int client_threads = 4;
    int provider_workers = 4;

    int total_requests = 100000;
    int warmup_count = 1000;
    int timeout_ms = 3000;

    std::vector<size_t> payload_sizes{
        16,
        128,
        1024,
        4 * 1024,
        16 * 1024,
        64 * 1024,
        256 * 1024,
        1024 * 1024
    };
};

struct PayloadBenchResult
{
    size_t payload_size = 0;

    int pool_size = 0;
    int client_threads = 0;
    int provider_workers = 0;
    int total_requests = 0;

    int64_t success_count = 0;
    int64_t failed_count = 0;

    double elapsed_seconds = 0.0;
    double qps = 0.0;
    double throughput_mib_s = 0.0;
    double error_rate = 0.0;

    double p50_ms = 0.0;
    double p90_ms = 0.0;
    double p99_ms = 0.0;
    double max_ms = 0.0;

    double client_cpu_percent = 0.0;
};

bool doRegisterOnce(demo::UserService_Stub& stub,
                 const std::string& payload)
{
    demo::RegisterRequest request;
    demo::RegisterResponse response;
    SimpleRpcController controller;

    request.set_name("haojun");
    request.set_password(payload);

    stub.Register(&controller, &request, &response, nullptr);

    if (controller.Failed())
    {
        std::cout << "1" << " ";
        std::cout << controller.error_text() << std::endl;
    }
    return !controller.Failed() && response.success();
}

void warmup(RpcChannelPool& pool,
            int warmup_count,
            const std::string& payload)
{
    if (warmup_count <= 0)
    {
        return;
    }

    demo::UserService_Stub stub(&pool);

    for (int i = 0; i < warmup_count; ++i)
    {
        doRegisterOnce(stub, payload);
    }
}

PayloadBenchResult runOnePayloadSize(const PayloadBenchOptions& options,
                                     size_t payload_size)
{
    PayloadBenchResult result;
    result.payload_size = payload_size;
    result.pool_size = options.pool_size;
    result.client_threads = options.client_threads;
    result.provider_workers = options.provider_workers;
    result.total_requests = options.total_requests;

    std::string payload = makePayload(payload_size);

    RpcChannelPool pool(options.ip, options.port, options.pool_size);
    pool.setTimeoutMs(options.timeout_ms);

    if (!pool.start())
    {
        std::cerr << "pool start failed, payload_size=" << payload_size << "\n";
        result.failed_count = options.total_requests;
        result.error_rate = 100.0;
        return result;
    }

    warmup(pool, options.warmup_count, payload);

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
                              &payload,
                              &thread_latencies,
                              &success_count,
                              &failed_count,
                              t,
                              calls]() {
            demo::UserService_Stub stub(&pool);

            auto& local_latencies =
                thread_latencies[static_cast<size_t>(t)];

            local_latencies.reserve(static_cast<size_t>(calls));

            for (int i = 0; i < calls; ++i)
            {
                auto start = std::chrono::steady_clock::now();

                bool ok = doRegisterOnce(stub, payload);

                auto end = std::chrono::steady_clock::now();

                int64_t latency_us =
                    std::chrono::duration_cast<std::chrono::microseconds>(
                        end - start)
                        .count();

                local_latencies.push_back(latency_us);

                if (ok)
                {
                    success_count.fetch_add(1, std::memory_order_relaxed);
                }
                else
                {
                    failed_count.fetch_add(1, std::memory_order_relaxed);
                }
            }
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

        double payload_mib =
            static_cast<double>(payload_size) *
            static_cast<double>(completed) /
            1024.0 / 1024.0;

        result.throughput_mib_s =
            payload_mib / result.elapsed_seconds;
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
    result.max_ms = latencies.empty() ? 0.0 : usToMs(latencies.back());

    double cpu_seconds = cpu_end - cpu_start;
    if (result.elapsed_seconds > 0.0)
    {
        result.client_cpu_percent =
            cpu_seconds / result.elapsed_seconds * 100.0;
    }

    return result;
}

PayloadBenchOptions parseArgs(int argc, char* argv[])
{
    PayloadBenchOptions options;

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

std::string formatPayloadSize(size_t bytes)
{
    if (bytes >= 1024 * 1024)
    {
        return std::to_string(bytes / (1024 * 1024)) + " MiB";
    }

    if (bytes >= 1024)
    {
        return std::to_string(bytes / 1024) + " KiB";
    }

    return std::to_string(bytes) + " B";
}

void printMarkdownHeader()
{
    std::cout
        << "| Mode | Payload | Pool Size | Client Threads | Provider Workers | Requests | "
        << "Success | Failed | QPS | MiB/s | p50 | p90 | p99 | Max | Error Rate | Client CPU |\n";

    std::cout
        << "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n";
}

void printMarkdownRow(const PayloadBenchResult& r)
{
    std::cout
        << "| sync payload | "
        << formatPayloadSize(r.payload_size) << " | "
        << r.pool_size << " | "
        << r.client_threads << " | "
        << r.provider_workers << " | "
        << r.total_requests << " | "
        << r.success_count << " | "
        << r.failed_count << " | "
        << static_cast<int64_t>(r.qps) << " | "
        << r.throughput_mib_s << " | "
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
    PayloadBenchOptions options = parseArgs(argc, argv);

    std::cout << "payload_size_benchmark\n";
    std::cout << "ip=" << options.ip << "\n";
    std::cout << "port=" << options.port << "\n";
    std::cout << "total_requests=" << options.total_requests << "\n";
    std::cout << "pool_size=" << options.pool_size << "\n";
    std::cout << "client_threads=" << options.client_threads << "\n";
    std::cout << "warmup_count=" << options.warmup_count << "\n\n";

    printMarkdownHeader();

    for (size_t payload_size : options.payload_sizes)
    {
        PayloadBenchResult result = runOnePayloadSize(options, payload_size);
        printMarkdownRow(result);
    }

    return 0;
}