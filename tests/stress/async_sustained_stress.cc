#include "rpc_channel_pool.h"
#include "rpc_closure.h"
#include "rpc_controller.h"

#include "user.pb.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <sys/resource.h>
#include <thread>
#include <vector>

namespace
{

using Clock = std::chrono::steady_clock;

struct StressOptions
{
    std::string ip = "127.0.0.1";
    uint16_t port = 8000;

    int duration_seconds = 300;
    int pool_size = 4;
    int client_threads = 4;
    int max_inflight = 1000;
    int timeout_ms = 3000;
    int report_interval_seconds = 5;

    int64_t max_requests = 0;
};

struct StressCallState
{
    demo::LoginRequest request;
    demo::LoginResponse response;
    SimpleRpcController controller;

    std::atomic<bool> finished{false};
};

struct StressContext
{
    std::mutex mutex;
    std::condition_variable cv;

    int64_t submitted = 0;
    int64_t completed = 0;
    int64_t inflight = 0;

    int64_t success = 0;
    int64_t failed = 0;
    int64_t duplicate_done = 0;
    int64_t local_submit_failed = 0;

    int64_t max_observed_inflight = 0;

    int64_t max_inflight = 1000;
    int64_t max_requests = 0;

    std::atomic<bool> abort_requested{false};
};

struct StressSnapshot
{
    int64_t submitted = 0;
    int64_t completed = 0;
    int64_t inflight = 0;

    int64_t success = 0;
    int64_t failed = 0;
    int64_t duplicate_done = 0;
    int64_t local_submit_failed = 0;

    int64_t max_observed_inflight = 0;
};

double getProcessCpuSeconds()
{
    struct rusage usage;
    if (::getrusage(RUSAGE_SELF, &usage) != 0)
    {
        return 0.0;
    }

    const double user_seconds =
        static_cast<double>(usage.ru_utime.tv_sec) +
        static_cast<double>(usage.ru_utime.tv_usec) / 1000000.0;

    const double system_seconds =
        static_cast<double>(usage.ru_stime.tv_sec) +
        static_cast<double>(usage.ru_stime.tv_usec) / 1000000.0;

    return user_seconds + system_seconds;
}

StressSnapshot snapshotStats(StressContext& ctx)
{
    std::lock_guard<std::mutex> lock(ctx.mutex);

    StressSnapshot snapshot;
    snapshot.submitted = ctx.submitted;
    snapshot.completed = ctx.completed;
    snapshot.inflight = ctx.inflight;

    snapshot.success = ctx.success;
    snapshot.failed = ctx.failed;
    snapshot.duplicate_done = ctx.duplicate_done;
    snapshot.local_submit_failed = ctx.local_submit_failed;

    snapshot.max_observed_inflight = ctx.max_observed_inflight;
    return snapshot;
}


bool reserveOneInflightSlot(
    StressContext& ctx,
    Clock::time_point deadline)
{
    std::unique_lock<std::mutex> lock(ctx.mutex);

    while (true)
    {
        if (ctx.abort_requested.load(std::memory_order_acquire))
        {
            return false;
        }

        if (ctx.max_requests > 0 &&
            ctx.submitted >= ctx.max_requests)
        {
            return false;
        }

        if (Clock::now() >= deadline)
        {
            return false;
        }

        if (ctx.inflight < ctx.max_inflight)
        {
            ++ctx.submitted;
            ++ctx.inflight;

            ctx.max_observed_inflight =
                std::max(ctx.max_observed_inflight, ctx.inflight);

            return true;
        }

        ctx.cv.wait_until(lock, deadline, [&ctx]() {
            return ctx.abort_requested.load(std::memory_order_acquire) ||
                   ctx.inflight < ctx.max_inflight ||
                   (ctx.max_requests > 0 &&
                    ctx.submitted >= ctx.max_requests);
        });
    }
}


void recordLocalSubmitFailure(StressContext& ctx)
{
    {
        std::lock_guard<std::mutex> lock(ctx.mutex);

        ++ctx.local_submit_failed;
        ++ctx.failed;
        ++ctx.completed;

        if (ctx.inflight > 0)
        {
            --ctx.inflight;
        }
        else
        {
            ctx.abort_requested.store(true, std::memory_order_release);
        }
    }

    ctx.cv.notify_all();
}

void recordCompletion(StressContext& ctx, bool success)
{
    {
        std::lock_guard<std::mutex> lock(ctx.mutex);

        if (success)
        {
            ++ctx.success;
        }
        else
        {
            ++ctx.failed;
        }

        ++ctx.completed;

        if (ctx.inflight > 0)
        {
            --ctx.inflight;
        }
        else
        {
            ctx.abort_requested.store(true, std::memory_order_release);
        }
    }

    ctx.cv.notify_all();
}


void recordDuplicateDone(StressContext& ctx)
{
    {
        std::lock_guard<std::mutex> lock(ctx.mutex);
        ++ctx.duplicate_done;
        ctx.abort_requested.store(true, std::memory_order_release);
    }

    ctx.cv.notify_all();
}

bool submitOneAsyncCall(
    demo::UserService_Stub& stub,
    StressContext& ctx)
{
    auto state = std::make_shared<StressCallState>();
    state->request.set_name("haojun");
    state->request.set_password("123456");

    auto done = SendResponseClosure([state, &ctx](){
        bool expected = false;
        if (!state->finished.compare_exchange_strong(expected, true, std::memory_order_acq_rel))
        {
            recordDuplicateDone(ctx);
            return;
        }

        bool ok = !state->controller.Failed() && state->response.success();
        recordCompletion(ctx, ok);
    });

    stub.Login(&state->controller, &state->request, &state->response, done);

    return true;
}

bool waitUntilDrained(
    StressContext& ctx,
    std::chrono::milliseconds timeout)
{
    std::unique_lock<std::mutex> lock(ctx.mutex);

    return ctx.cv.wait_for(lock, timeout, [&ctx]() {
        return ctx.completed == ctx.submitted &&
               ctx.inflight == 0;
    });
}

void runReporter(
    StressContext& ctx,
    std::atomic<bool>& reporter_stop,
    Clock::time_point start_time,
    int report_interval_seconds)
{
    int64_t previous_completed = 0;
    auto previous_time = start_time;

    while (!reporter_stop.load(std::memory_order_acquire))
    {
        std::this_thread::sleep_for(
            std::chrono::seconds(report_interval_seconds));

        const auto now = Clock::now();
        const StressSnapshot snapshot = snapshotStats(ctx);

        const double total_seconds =
            std::chrono::duration<double>(now - start_time).count();

        const double interval_seconds =
            std::chrono::duration<double>(now - previous_time).count();

        const int64_t interval_completed =
            snapshot.completed - previous_completed;

        const double cumulative_qps =
            total_seconds > 0.0
                ? static_cast<double>(snapshot.completed) / total_seconds
                : 0.0;

        const double interval_qps =
            interval_seconds > 0.0
                ? static_cast<double>(interval_completed) / interval_seconds
                : 0.0;

        std::cout
            << "[stress]"
            << " elapsed=" << std::fixed << std::setprecision(1)
            << total_seconds << "s"
            << " submitted=" << snapshot.submitted
            << " completed=" << snapshot.completed
            << " inflight=" << snapshot.inflight
            << " success=" << snapshot.success
            << " failed=" << snapshot.failed
            << " duplicate_done=" << snapshot.duplicate_done
            << " interval_qps=" << interval_qps
            << " cumulative_qps=" << cumulative_qps
            << '\n';

        previous_completed = snapshot.completed;
        previous_time = now;
    }
}

bool startsWith(const std::string& value, const std::string& prefix)
{
    return value.rfind(prefix, 0) == 0;
}

std::string valueOf(const std::string& arg, const std::string& prefix)
{
    return arg.substr(prefix.size());
}

StressOptions parseArgs(int argc, char* argv[])
{
    StressOptions options;

    for (int i = 1; i < argc; ++i)
    {
        const std::string arg = argv[i];
        try
        {
            if (startsWith(arg, "--ip="))
            {
                options.ip = valueOf(arg, "--ip=");
            }
            else if (startsWith(arg, "--port="))
            {
                options.port = static_cast<uint16_t>(
                    std::stoi(valueOf(arg, "--port=")));
            }
            else if (startsWith(arg, "--duration-seconds="))
            {
                options.duration_seconds = std::stoi(
                    valueOf(arg, "--duration-seconds="));
            }
            else if (startsWith(arg, "--pool_size="))
            {
                options.pool_size = std::stoi(valueOf(arg, "--pool_size="));
            }
            else if (startsWith(arg, "--client_threads="))
            {
                options.client_threads = std::stoi(
                    valueOf(arg, "--client_threads="));
            }
            else if (startsWith(arg, "--max-inflight="))
            {
                options.max_inflight = std::stoi(
                    valueOf(arg, "--max-inflight="));
            }
            else if (startsWith(arg, "--timeout-ms="))
            {
                options.timeout_ms = std::stoi(valueOf(arg, "--timeout-ms="));
            }
            else if (startsWith(arg, "--report-interval-seconds="))
            {
                options.report_interval_seconds = std::stoi(
                    valueOf(arg, "--report-interval-seconds="));
            }
            else if (startsWith(arg, "--max-requests="))
            {
                options.max_requests = std::stoll(
                    valueOf(arg, "--max-requests="));
            }
            else
            {
                std::cerr << "unknown argument: " << arg << '\n';
            }
        }
        catch (const std::exception& ex)
        {
            std::cerr << "invalid argument: " << arg
                      << ", error: " << ex.what() << '\n';
            throw;
        }
    }

    return options;
}

bool validateOptions(const StressOptions& options)
{
    if (options.port == 0)
    {
        std::cerr << "port must be greater than 0\n";
        return false;
    }

    if (options.duration_seconds <= 0)
    {
        std::cerr << "duration_seconds must be greater than 0\n";
        return false;
    }

    if (options.pool_size <= 0)
    {
        std::cerr << "pool_size must be greater than 0\n";
        return false;
    }

    if (options.client_threads <= 0)
    {
        std::cerr << "client_threads must be greater than 0\n";
        return false;
    }

    if (options.max_inflight <= 0)
    {
        std::cerr << "max_inflight must be greater than 0\n";
        return false;
    }

    if (options.timeout_ms <= 0)
    {
        std::cerr << "timeout_ms must be greater than 0\n";
        return false;
    }

    if (options.report_interval_seconds <= 0)
    {
        std::cerr << "report_interval_seconds must be greater than 0\n";
        return false;
    }

    if (options.max_requests < 0)
    {
        std::cerr << "max_requests must not be negative\n";
        return false;
    }

    return true;
}

void printOptions(const StressOptions& options)
{
    std::cout
        << "async_sustained_stress\n"
        << "ip=" << options.ip << '\n'
        << "port=" << options.port << '\n'
        << "duration_seconds=" << options.duration_seconds << '\n'
        << "pool_size=" << options.pool_size << '\n'
        << "client_threads=" << options.client_threads << '\n'
        << "max_inflight=" << options.max_inflight << '\n'
        << "timeout_ms=" << options.timeout_ms << '\n'
        << "report_interval_seconds="
        << options.report_interval_seconds << '\n'
        << "max_requests=" << options.max_requests << "\n\n";
}

bool validateFinalResult(
    const StressContext& ctx,
    const StressSnapshot& result)
{
    bool passed = true;

    if (result.submitted != result.completed)
    {
        std::cerr
            << "FAILED: submitted != completed: "
            << result.submitted << " != "
            << result.completed << '\n';
        passed = false;
    }

    if (result.inflight != 0)
    {
        std::cerr
            << "FAILED: final inflight is not zero: "
            << result.inflight << '\n';
        passed = false;
    }

    if (result.success + result.failed != result.completed)
    {
        std::cerr
            << "FAILED: success + failed != completed\n";
        passed = false;
    }

    if (result.duplicate_done != 0)
    {
        std::cerr
            << "FAILED: duplicate done count="
            << result.duplicate_done << '\n';
        passed = false;
    }

    if (result.max_observed_inflight > ctx.max_inflight)
    {
        std::cerr
            << "FAILED: max_observed_inflight="
            << result.max_observed_inflight
            << " exceeds max_inflight="
            << ctx.max_inflight << '\n';
        passed = false;
    }

    if (result.submitted == 0)
    {
        std::cerr << "FAILED: no RPC was submitted\n";
        passed = false;
    }

    return passed;
}

} // namespace

int main(int argc, char* argv[])
{
    StressOptions options;

    try
    {
        options = parseArgs(argc, argv);
    }
    catch (const std::exception& ex)
    {
        std::cerr << "invalid arguments: " << ex.what() << '\n';
        return 1;
    }

    if (!validateOptions(options))
    {
        return 1;
    }

    printOptions(options);

    RpcChannelPool pool(
        options.ip,
        options.port,
        options.pool_size);

    pool.setTimeoutMs(options.timeout_ms);

    if (!pool.start())
    {
        std::cerr << "RpcChannelPool::start() failed\n";
        return 1;
    }

    StressContext ctx;
    ctx.max_inflight = options.max_inflight;
    ctx.max_requests = options.max_requests;

    const auto wall_start = Clock::now();
    const auto deadline =
        wall_start + std::chrono::seconds(options.duration_seconds);

    const double cpu_start = getProcessCpuSeconds();

    std::atomic<bool> reporter_stop{false};

    std::thread reporter(
        runReporter,
        std::ref(ctx),
        std::ref(reporter_stop),
        wall_start,
        options.report_interval_seconds);

    std::vector<std::thread> producers;
    producers.reserve(
        static_cast<size_t>(options.client_threads));

    for (int i = 0; i < options.client_threads; ++i)
    {
        producers.emplace_back([&pool, &ctx, deadline]() {
            demo::UserService_Stub stub(&pool);

            while (reserveOneInflightSlot(ctx, deadline))
            {
                bool submitted = false;

                try
                {
                    submitted = submitOneAsyncCall(stub, ctx);
                }
                catch (const std::exception& ex)
                {
                    std::cerr
                        << "submitOneAsyncCall exception: "
                        << ex.what() << '\n';
                }
                catch (...)
                {
                    std::cerr
                        << "submitOneAsyncCall unknown exception\n";
                }

                if (!submitted)
                {
                    recordLocalSubmitFailure(ctx);
                }

                if (ctx.abort_requested.load(
                        std::memory_order_acquire))
                {
                    break;
                }
            }
        });
    }

    for (auto& producer : producers)
    {
        producer.join();
    }

    /*
     * 停止提交后，先给正常 RPC timeout/response 一段 drain 时间。
     *
     * 这里至少给 10 秒，也至少给 timeout 的两倍。
     */
    const auto normal_drain_timeout =
        std::chrono::milliseconds(
            std::max(10000, options.timeout_ms * 2));

    bool drained =
        waitUntilDrained(ctx, normal_drain_timeout);

    if (!drained)
    {
        const StressSnapshot before_stop = snapshotStats(ctx);

        std::cerr
            << "normal drain timed out:"
            << " submitted=" << before_stop.submitted
            << " completed=" << before_stop.completed
            << " inflight=" << before_stop.inflight
            << "; forcing pool.stop()\n";

        /*
         * 从 main/owner 线程执行 stop。
         * stop 应该失败完成尚未结束的 pending calls。
         */
        pool.stop();

        drained = waitUntilDrained(
            ctx,
            std::chrono::seconds(10));
    }
    else
    {
        pool.stop();
    }

    reporter_stop.store(true, std::memory_order_release);
    ctx.cv.notify_all();

    if (reporter.joinable())
    {
        reporter.join();
    }

    const auto wall_end = Clock::now();
    const double cpu_end = getProcessCpuSeconds();

    const StressSnapshot result = snapshotStats(ctx);

    const double elapsed_seconds =
        std::chrono::duration<double>(
            wall_end - wall_start)
            .count();

    const double qps =
        elapsed_seconds > 0.0
            ? static_cast<double>(result.completed) /
                  elapsed_seconds
            : 0.0;

    const double client_cpu_percent =
        elapsed_seconds > 0.0
            ? (cpu_end - cpu_start) /
                  elapsed_seconds * 100.0
            : 0.0;

    std::cout
        << "\nfinal_result\n"
        << "submitted=" << result.submitted << '\n'
        << "completed=" << result.completed << '\n'
        << "success=" << result.success << '\n'
        << "failed=" << result.failed << '\n'
        << "local_submit_failed="
        << result.local_submit_failed << '\n'
        << "duplicate_done="
        << result.duplicate_done << '\n'
        << "final_inflight="
        << result.inflight << '\n'
        << "max_observed_inflight="
        << result.max_observed_inflight << '\n'
        << "elapsed_seconds="
        << elapsed_seconds << '\n'
        << "qps=" << qps << '\n'
        << "client_cpu_percent="
        << client_cpu_percent << "%\n"
        << "drained="
        << (drained ? "true" : "false") << '\n';

    const bool invariants_ok =
        validateFinalResult(ctx, result);

    if (!drained)
    {
        std::cerr
            << "FAILED: pending calls did not drain after pool.stop()\n";
        return 2;
    }

    if (!invariants_ok)
    {
        return 2;
    }

    std::cout << "PASSED\n";
    return 0;
}
