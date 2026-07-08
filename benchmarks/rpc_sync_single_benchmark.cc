#include "rpc_channel.h"
#include "rpc_controller.h"

#include "user.pb.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <sys/resource.h>
#include <vector>
#include <cmath>

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
        int warmup_count = 1000;
        int request_count = 10000;
        int timeout_ms = 3000;
    };

    struct BenchResult
    {
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

    void printUsage(const char *program)
    {
        std::cerr
            << "Usage: " << program
            << " [ip] [port] [request_count] [warmup_count]\n"
            << "Example:\n"
            << "  " << program << " 127.0.0.1 8000 10000 1000\n";
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
            options.request_count = std::stoi(argv[3]);
        }

        if (argc >= 5)
        {
            options.warmup_count = std::stoi(argv[4]);
        }

        return options;
    }

    bool doLoginOnce(demo::UserService_Stub &stub,
                     const demo::LoginRequest &request,
                     int timeout_ms)
    {
        demo::LoginResponse response;
        SimpleRpcController controller;

        stub.Login(&controller, &request, &response, nullptr);

        return !controller.Failed() && response.success();
    }

    BenchResult runBenchmark(demo::UserService_Stub &stub,
                             const BenchOptions &options)
    {
        demo::LoginRequest request;
        request.set_name("haojun");
        request.set_password("123456");

        for (int i = 0; i < options.warmup_count; ++i)
        {
            doLoginOnce(stub, request, options.timeout_ms);
        }

        std::vector<int64_t> latencies_us;
        latencies_us.reserve(options.request_count);

        BenchResult result;

        double cpu_start = getProcessCpuSeconds();
        auto wall_start = std::chrono::steady_clock::now();

        for (int i = 0; i < options.request_count; ++i)
        {
            auto start = std::chrono::steady_clock::now();

            bool ok = doLoginOnce(stub, request, options.timeout_ms);

            auto end = std::chrono::steady_clock::now();

            int64_t latency_us =
                std::chrono::duration_cast<std::chrono::microseconds>(
                    end - start)
                    .count();

            latencies_us.push_back(latency_us);

            if (ok)
            {
                ++result.success_count;
            }
            else
            {
                ++result.failed_count;
            }
        }

        auto wall_end = std::chrono::steady_clock::now();
        double cpu_end = getProcessCpuSeconds();

        result.elapsed_seconds =
            std::chrono::duration<double>(wall_end - wall_start).count();

        int64_t completed = result.success_count + result.failed_count;

        if (completed > 0 && result.elapsed_seconds > 0.0)
        {
            result.qps =
                static_cast<double>(completed) / result.elapsed_seconds;

            result.error_rate =
                static_cast<double>(result.failed_count) /
                static_cast<double>(completed) * 100.0;
        }

        std::sort(latencies_us.begin(), latencies_us.end());

        result.p50_ms = usToMs(percentile(latencies_us, 50.0));
        result.p90_ms = usToMs(percentile(latencies_us, 90.0));
        result.p99_ms = usToMs(percentile(latencies_us, 99.0));
        result.max_ms =
            latencies_us.empty() ? 0.0 : usToMs(latencies_us.back());

        double cpu_seconds = cpu_end - cpu_start;

        if (result.elapsed_seconds > 0.0)
        {
            result.client_cpu_percent =
                cpu_seconds / result.elapsed_seconds * 100.0;
        }

        return result;
    }

    void printResult(const BenchOptions &options, const BenchResult &result)
    {
        int64_t completed = result.success_count + result.failed_count;

        std::cout << "mode=sync_single_channel\n";
        std::cout << "ip=" << options.ip << "\n";
        std::cout << "port=" << options.port << "\n";
        std::cout << "warmup_count=" << options.warmup_count << "\n";
        std::cout << "request_count=" << options.request_count << "\n";
        std::cout << "completed=" << completed << "\n";
        std::cout << "success=" << result.success_count << "\n";
        std::cout << "failed=" << result.failed_count << "\n";
        std::cout << "error_rate=" << result.error_rate << "%\n";
        std::cout << "elapsed_seconds=" << result.elapsed_seconds << "\n";
        std::cout << "qps=" << result.qps << "\n";
        std::cout << "p50_latency_ms=" << result.p50_ms << "\n";
        std::cout << "p90_latency_ms=" << result.p90_ms << "\n";
        std::cout << "p99_latency_ms=" << result.p99_ms << "\n";
        std::cout << "max_latency_ms=" << result.max_ms << "\n";
        std::cout << "client_cpu_percent=" << result.client_cpu_percent << "\n";
    }

} // namespace

int main(int argc, char *argv[])
{
    BenchOptions options = parseArgs(argc, argv);

    auto channel = MyRpcChannel::create(options.ip, options.port, nullptr);
    if (!channel)
    {
        std::cerr << "create channel failed\n";
        return 1;
    }

    channel->setTimeoutMs(options.timeout_ms);

    if (!channel->start())
    {
        std::cerr << "channel start failed\n";
        return 1;
    }

    demo::UserService_Stub stub(channel.get());

    BenchResult result = runBenchmark(stub, options);

    channel->stop();

    printResult(options, result);

    return 0;
}