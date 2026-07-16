#include "rpc_channel_pool.h"
#include "rpc_closure.h"
#include "rpc_controller.h"
#include "tcpserver.h"

#include "user.pb.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace
{

using Clock = std::chrono::steady_clock;

enum class ResponseDelay
{
    kFast,
    kNearTimeout,
    kLate,
};

struct StressOptions
{
    std::string ip = "127.0.0.1";
    int duration_seconds = 60;
    int pool_size = 16;
    int client_threads = 4;
    int max_inflight = 16;
    int timeout_ms = 40;
    int64_t max_requests = 3000;
};

struct ServerStats
{
    std::mutex mutex;
    std::condition_variable cv;
    int64_t fast = 0;
    int64_t near_timeout = 0;
    int64_t late = 0;
    int64_t completed = 0;
    int timeout_ms = 40;
};

ServerStats g_server_stats;

struct CallState
{
    demo::LoginRequest request;
    demo::LoginResponse response;
    SimpleRpcController controller;
    ResponseDelay expected_delay = ResponseDelay::kFast;
    std::atomic<bool> done_called{false};
};

struct StressContext
{
    std::mutex mutex;
    std::condition_variable cv;

    int64_t submitted = 0;
    int64_t completed = 0;
    int64_t inflight = 0;
    int64_t success = 0;
    int64_t timeout = 0;
    int64_t other_failure = 0;
    int64_t duplicate_done = 0;
    int64_t local_submit_failure = 0;
    int64_t fast_success = 0;
    int64_t near_timeout_success = 0;
    int64_t late_timeout = 0;
    int64_t max_observed_inflight = 0;

    int64_t max_inflight = 16;
    int64_t max_requests = 3000;
    std::atomic<bool> abort_requested{false};
};

struct StressSnapshot
{
    int64_t submitted = 0;
    int64_t completed = 0;
    int64_t inflight = 0;
    int64_t success = 0;
    int64_t timeout = 0;
    int64_t other_failure = 0;
    int64_t duplicate_done = 0;
    int64_t local_submit_failure = 0;
    int64_t fast_success = 0;
    int64_t near_timeout_success = 0;
    int64_t late_timeout = 0;
    int64_t max_observed_inflight = 0;
};

bool startsWith(const std::string &value, const std::string &prefix)
{
    return value.rfind(prefix, 0) == 0;
}

std::string valueOf(const std::string &value, const std::string &prefix)
{
    return value.substr(prefix.size());
}

ResponseDelay responseDelayFor(int64_t request_number)
{
    const int bucket = static_cast<int>(request_number % 10);
    if (bucket < 2)
    {
        return ResponseDelay::kLate;
    }
    if (bucket < 4)
    {
        return ResponseDelay::kNearTimeout;
    }
    return ResponseDelay::kFast;
}

StressOptions parseArgs(int argc, char *argv[])
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
            else if (startsWith(arg, "--duration-seconds="))
            {
                options.duration_seconds = std::stoi(valueOf(arg, "--duration-seconds="));
            }
            else if (startsWith(arg, "--pool_size="))
            {
                options.pool_size = std::stoi(valueOf(arg, "--pool_size="));
            }
            else if (startsWith(arg, "--client_threads="))
            {
                options.client_threads = std::stoi(valueOf(arg, "--client_threads="));
            }
            else if (startsWith(arg, "--max-inflight="))
            {
                options.max_inflight = std::stoi(valueOf(arg, "--max-inflight="));
            }
            else if (startsWith(arg, "--timeout-ms="))
            {
                options.timeout_ms = std::stoi(valueOf(arg, "--timeout-ms="));
            }
            else if (startsWith(arg, "--max-requests="))
            {
                options.max_requests = std::stoll(valueOf(arg, "--max-requests="));
            }
            else if (startsWith(arg, "--port=") ||
                     startsWith(arg, "--disconnect-interval-ms="))
            {
                // The stress server owns its ephemeral port; the shared CTest options are ignored.
            }
            else
            {
                std::cerr << "unknown argument: " << arg << '\n';
            }
        }
        catch (const std::exception &ex)
        {
            std::cerr << "invalid argument: " << arg << ", error: " << ex.what() << '\n';
            throw;
        }
    }
    return options;
}

bool validateOptions(const StressOptions &options)
{
    return options.duration_seconds > 0 && options.pool_size > 0 &&
           options.client_threads > 0 && options.max_inflight > 0 &&
           options.timeout_ms > 3 && options.max_requests > 0;
}

StressSnapshot snapshot(StressContext &ctx)
{
    std::lock_guard<std::mutex> lock(ctx.mutex);
    return {ctx.submitted, ctx.completed, ctx.inflight, ctx.success, ctx.timeout,
            ctx.other_failure, ctx.duplicate_done, ctx.local_submit_failure,
            ctx.fast_success, ctx.near_timeout_success, ctx.late_timeout,
            ctx.max_observed_inflight};
}

bool reserveSubmission(StressContext &ctx, Clock::time_point deadline)
{
    std::unique_lock<std::mutex> lock(ctx.mutex);
    while (true)
    {
        if (ctx.abort_requested.load(std::memory_order_acquire) ||
            ctx.submitted >= ctx.max_requests || Clock::now() >= deadline)
        {
            return false;
        }

        if (ctx.inflight < ctx.max_inflight)
        {
            ++ctx.submitted;
            ++ctx.inflight;
            ctx.max_observed_inflight = std::max(ctx.max_observed_inflight, ctx.inflight);
            return true;
        }

        ctx.cv.wait_until(lock, deadline, [&ctx]() {
            return ctx.abort_requested.load(std::memory_order_acquire) ||
                   ctx.submitted >= ctx.max_requests ||
                   ctx.inflight < ctx.max_inflight;
        });
    }
}

void recordCompletion(StressContext &ctx, ResponseDelay expected_delay,
                      bool failed, const std::string &error_text, bool response_success)
{
    std::lock_guard<std::mutex> lock(ctx.mutex);
    ++ctx.completed;

    if (!failed && response_success)
    {
        ++ctx.success;
        if (expected_delay == ResponseDelay::kFast)
        {
            ++ctx.fast_success;
        }
        else if (expected_delay == ResponseDelay::kNearTimeout)
        {
            ++ctx.near_timeout_success;
        }
    }
    else if (failed && error_text == "rpc call timeout")
    {
        ++ctx.timeout;
        if (expected_delay == ResponseDelay::kLate)
        {
            ++ctx.late_timeout;
        }
    }
    else
    {
        ++ctx.other_failure;
    }

    if (ctx.inflight == 0)
    {
        ctx.abort_requested.store(true, std::memory_order_release);
    }
    else
    {
        --ctx.inflight;
    }
    ctx.cv.notify_all();
}

void recordDuplicateDone(StressContext &ctx)
{
    std::lock_guard<std::mutex> lock(ctx.mutex);
    ++ctx.duplicate_done;
    ctx.abort_requested.store(true, std::memory_order_release);
    ctx.cv.notify_all();
}

void recordLocalSubmitFailure(StressContext &ctx)
{
    std::lock_guard<std::mutex> lock(ctx.mutex);
    ++ctx.local_submit_failure;
    ++ctx.other_failure;
    ++ctx.completed;
    if (ctx.inflight > 0)
    {
        --ctx.inflight;
    }
    else
    {
        ctx.abort_requested.store(true, std::memory_order_release);
    }
    ctx.cv.notify_all();
}

bool waitUntilDrained(StressContext &ctx, std::chrono::milliseconds timeout)
{
    std::unique_lock<std::mutex> lock(ctx.mutex);
    return ctx.cv.wait_for(lock, timeout, [&ctx]() {
        return ctx.completed == ctx.submitted && ctx.inflight == 0;
    });
}

bool waitForServerResponses(int64_t expected, std::chrono::milliseconds timeout)
{
    std::unique_lock<std::mutex> lock(g_server_stats.mutex);
    return g_server_stats.cv.wait_for(lock, timeout, [expected]() {
        return g_server_stats.completed >= expected;
    });
}

bool submitOne(demo::UserService_Stub &stub, StressContext &ctx, int64_t request_number)
{
    // request 序号决定服务端延迟类别：fast、near-timeout 或 late。
    auto state = std::make_shared<CallState>();
    state->request.set_name(std::to_string(request_number));
    state->request.set_password("timeout-stress");
    state->expected_delay = responseDelayFor(request_number);

    auto done = SendResponseClosure([state, &ctx]() {
        bool expected = false;
        if (!state->done_called.compare_exchange_strong(
                expected, true, std::memory_order_acq_rel))
        {
            recordDuplicateDone(ctx);
            return;
        }

        recordCompletion(ctx, state->expected_delay, state->controller.Failed(),
                         state->controller.ErrorText(), state->response.success());
    });

    stub.Login(&state->controller, &state->request, &state->response, done);
    return true;
}

bool validateResult(const StressOptions &options, const StressSnapshot &result,
                    bool drained, bool server_completed_all)
{
    bool passed = true;
    if (result.submitted != options.max_requests || result.submitted != result.completed ||
        result.inflight != 0)
    {
        std::cout << result.submitted << " " << result.completed << " " << result.inflight << "\n";
        std::cerr << "FAILED: request accounting did not drain exactly\n";
        passed = false;
    }
    if (result.success + result.timeout + result.other_failure != result.completed)
    {
        std::cerr << "FAILED: completion categories do not add up\n";
        passed = false;
    }
    if (result.duplicate_done != 0 || result.other_failure != 0 ||
        result.max_observed_inflight > options.max_inflight)
    {
        std::cerr << "FAILED: duplicate callback, unexpected failure, or inflight overflow\n";
        passed = false;
    }
    if (result.fast_success == 0 || result.near_timeout_success == 0 ||
        result.timeout == 0 || result.late_timeout == 0)
    {
        std::cerr << "FAILED: fast, near-timeout, and late timeout outcomes were not all observed\n";
        passed = false;
    }
    if (!drained || !server_completed_all)
    {
        std::cerr << "FAILED: client or server did not drain\n";
        passed = false;
    }
    return passed;
}

} // namespace

std::string buildLoginResponseBody(const myrpc::RpcHeader &, const std::string &request_body)
{
    demo::LoginRequest request;
    if (!request.ParseFromString(request_body))
    {
        return {};
    }

    const ResponseDelay delay = responseDelayFor(std::stoll(request.name()));
    const int timeout_ms = g_server_stats.timeout_ms;
    if (delay == ResponseDelay::kNearTimeout)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(timeout_ms * 3 / 4));
    }
    else if (delay == ResponseDelay::kLate)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(timeout_ms * 3));
    }

    {
        std::lock_guard<std::mutex> lock(g_server_stats.mutex);
        if (delay == ResponseDelay::kFast)
        {
            ++g_server_stats.fast;
        }
        else if (delay == ResponseDelay::kNearTimeout)
        {
            ++g_server_stats.near_timeout;
        }
        else
        {
            ++g_server_stats.late;
        }
        ++g_server_stats.completed;
    }
    g_server_stats.cv.notify_all();

    demo::LoginResponse response;
    response.set_code(0);
    response.set_message("login success");
    response.set_success(true);
    return response.SerializeAsString();
}

int main(int argc, char *argv[])
{
    StressOptions options;
    try
    {
        options = parseArgs(argc, argv);
    }
    catch (const std::exception &)
    {
        return 1;
    }

    if (!validateOptions(options))
    {
        std::cerr << "invalid stress options\n";
        return 1;
    }

    {
        std::lock_guard<std::mutex> lock(g_server_stats.mutex);
        g_server_stats.fast = 0;
        g_server_stats.near_timeout = 0;
        g_server_stats.late = 0;
        g_server_stats.completed = 0;
        g_server_stats.timeout_ms = options.timeout_ms;
    }

    ControlledTcpServer server(0, buildLoginResponseBody);
    if (!server.start())
    {
        std::cerr << "ControlledTcpServer::start() failed\n";
        return 1;
    }

    RpcChannelPool pool(options.ip, server.port(), options.pool_size);
    pool.setTimeoutMs(options.timeout_ms);
    if (!pool.start())
    {
        std::cerr << "RpcChannelPool::start() failed\n";
        server.stop();
        return 1;
    }

    StressContext ctx;
    ctx.max_inflight = options.max_inflight;
    ctx.max_requests = options.max_requests;
    const auto deadline = Clock::now() + std::chrono::seconds(options.duration_seconds);
    std::atomic<int64_t> next_request_number{0};

    std::vector<std::thread> submitters;
    submitters.reserve(static_cast<size_t>(options.client_threads));
    for (int i = 0; i < options.client_threads; ++i)
    {
        submitters.emplace_back([&]() {
            demo::UserService_Stub stub(&pool);
            while (reserveSubmission(ctx, deadline))
            {
                const int64_t request_number =
                    next_request_number.fetch_add(1, std::memory_order_relaxed);
                try
                {
                    submitOne(stub, ctx, request_number);
                }
                catch (const std::exception &ex)
                {
                    std::cerr << "submit exception: " << ex.what() << '\n';
                    recordLocalSubmitFailure(ctx);
                }
                catch (...)
                {
                    std::cerr << "submit unknown exception\n";
                    recordLocalSubmitFailure(ctx);
                }
            }
        });
    }

    for (auto &submitter : submitters)
    {
        submitter.join();
    }

    // 先等客户端完成，再等服务端写完所有响应，才能观察迟到 response 的处理。
    const bool drained = waitUntilDrained(
        ctx, std::chrono::milliseconds(std::max(10000, options.timeout_ms * 4)));
    const StressSnapshot result = snapshot(ctx);
    const bool server_completed_all = waitForServerResponses(
        result.submitted, std::chrono::seconds(60));

    // Let the connection loops write and the client reader consume late frames
    // before pool.stop() closes the sockets.
    if (server_completed_all)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(options.timeout_ms * 2));
    }

    pool.stop();
    server.stop();

    int64_t server_fast = 0;
    int64_t server_near_timeout = 0;
    int64_t server_late = 0;
    int64_t server_completed = 0;
    {
        std::lock_guard<std::mutex> lock(g_server_stats.mutex);
        server_fast = g_server_stats.fast;
        server_near_timeout = g_server_stats.near_timeout;
        server_late = g_server_stats.late;
        server_completed = g_server_stats.completed;
    }

    std::cout << "timeout_stress\n"
              << "submitted=" << result.submitted << '\n'
              << "completed=" << result.completed << '\n'
              << "success=" << result.success << '\n'
              << "timeout=" << result.timeout << '\n'
              << "other_failure=" << result.other_failure << '\n'
              << "duplicate_done=" << result.duplicate_done << '\n'
              << "final_inflight=" << result.inflight << '\n'
              << "fast_success=" << result.fast_success << '\n'
              << "near_timeout_success=" << result.near_timeout_success << '\n'
              << "late_timeout=" << result.late_timeout << '\n'
              << "server_fast_responses=" << server_fast << '\n'
              << "server_near_timeout_responses=" << server_near_timeout << '\n'
              << "server_late_responses=" << server_late << '\n'
              << "server_completed_responses=" << server_completed << '\n'
              << "drained=" << (drained ? "true" : "false") << '\n';

    return validateResult(options, result, drained, server_completed_all) ? 0 : 2;
}
