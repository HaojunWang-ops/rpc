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

    struct StressOptions
    {
        std::string ip = "127.0.0.1";
        uint16_t port = 8000;
        int duration_seconds = 30;
        int pool_size = 4;
        int client_threads = 4;
        int max_inflight = 1000;
        int timeout_ms = 3000;
        int stop_delay_milliseconds = 100;
        int disconnect_interval_milliseconds = 10;
        int repair_interval_milliseconds = 10;
        int rounds = 20;
    };

    struct CallState
    {
        demo::LoginRequest request;
        demo::LoginResponse response;
        SimpleRpcController controller;
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
        int64_t failed = 0;
        int64_t duplicate_done = 0;
        int64_t local_submit_failed = 0;
        int64_t disconnect_attempts = 0;
        int64_t repair_attempts = 0;

        int64_t max_inflight = 1000;
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
        int64_t disconnect_attempts = 0;
        int64_t repair_attempts = 0;
    };

    bool startsWith(const std::string &value, const std::string &prefix)
    {
        return value.rfind(prefix, 0) == 0;
    }

    std::string valueOf(const std::string &value, const std::string &prefix)
    {
        return value.substr(prefix.size());
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
                else if (startsWith(arg, "--port="))
                {
                    options.port = static_cast<uint16_t>(std::stoi(valueOf(arg, "--port=")));
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
                else if (startsWith(arg, "--stop-delay-ms="))
                {
                    options.stop_delay_milliseconds = std::stoi(valueOf(arg, "--stop-delay-ms="));
                }
                else if (startsWith(arg, "--stop-rounds="))
                {
                    options.rounds = std::stoi(valueOf(arg, "--stop-rounds="));
                }
                else if (startsWith(arg, "--disconnect-interval-ms="))
                {
                    options.disconnect_interval_milliseconds =
                        std::stoi(valueOf(arg, "--disconnect-interval-ms="));
                }
                else if (startsWith(arg, "--repair-interval-ms="))
                {
                    options.repair_interval_milliseconds =
                        std::stoi(valueOf(arg, "--repair-interval-ms="));
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
        return options.duration_seconds > 0 &&
               options.pool_size > 0 &&
               options.client_threads > 0 &&
               options.max_inflight > 0 &&
               options.timeout_ms > 0 &&
               options.stop_delay_milliseconds >= 0 &&
               options.disconnect_interval_milliseconds >= 0 &&
               options.repair_interval_milliseconds >= 0 &&
               options.rounds > 0;
    }

    StressSnapshot snapshot(StressContext &ctx)
    {
        std::lock_guard<std::mutex> lock(ctx.mutex);
        return {ctx.submitted, ctx.completed, ctx.inflight, ctx.success, ctx.failed,
                ctx.duplicate_done, ctx.local_submit_failed, ctx.disconnect_attempts,
                ctx.repair_attempts};
    }

    bool reserveSubmission(StressContext &ctx,
                           std::atomic<bool> &round_stop,
                           Clock::time_point deadline)
    {
        std::unique_lock<std::mutex> lock(ctx.mutex);
        while (true)
        {
            if (ctx.abort_requested.load(std::memory_order_acquire) ||
                round_stop.load(std::memory_order_acquire) ||
                Clock::now() >= deadline)
            {
                return false;
            }

            if (ctx.inflight < ctx.max_inflight)
            {
                ++ctx.submitted;
                ++ctx.inflight;
                return true;
            }

            ctx.cv.wait_until(lock, deadline, [&ctx, &round_stop]()
                              { return ctx.abort_requested.load(std::memory_order_acquire) ||
                                       round_stop.load(std::memory_order_acquire) ||
                                       ctx.inflight < ctx.max_inflight; });
        }
    }

    void recordCompletion(StressContext &ctx, bool success)
    {
        std::lock_guard<std::mutex> lock(ctx.mutex);

        ++ctx.completed;
        if (success)
        {
            ++ctx.success;
        }
        else
        {
            ++ctx.failed;
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
        ctx.cv.notify_all();
    }

    bool waitUntilDrained(StressContext &ctx, std::chrono::milliseconds timeout)
    {
        std::unique_lock<std::mutex> lock(ctx.mutex);
        return ctx.cv.wait_for(lock, timeout, [&ctx]()
                               { return ctx.completed == ctx.submitted && ctx.inflight == 0; });
    }

    bool submitOne(demo::UserService_Stub &stub, StressContext &ctx)
    {
        auto state = std::make_shared<CallState>();
        state->request.set_name("submit-stop-race");
        state->request.set_password("123456");

        auto done = SendResponseClosure([state, &ctx]()
                                        {
        bool expected = false;
        if (!state->done_called.compare_exchange_strong(
                expected, true, std::memory_order_acq_rel))
        {
            recordDuplicateDone(ctx);
            return;
        }

        recordCompletion(ctx, !state->controller.Failed() && state->response.success()); });

        stub.Login(&state->controller, &state->request, &state->response, done);
        return true;
    }

    void runDisconnect(StressContext &ctx,
                       std::atomic<bool> &disconnect_stop,
                       ControlledTcpServer &server,
                       int disconnect_interval_milliseconds)
    {
        while (!disconnect_stop.load(std::memory_order_acquire))
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(disconnect_interval_milliseconds));

            if (disconnect_stop.load(std::memory_order_acquire))
            {
                break;
            }

            server.closeOneConnection();

            std::lock_guard<std::mutex> lock(ctx.mutex);
            ++ctx.disconnect_attempts;
        }
    }

    void runRepair(StressContext &ctx,
                   std::atomic<bool> &repair_stop,
                   RpcChannelPool &pool,
                   int repair_interval_milliseconds)
    {
        while (!repair_stop.load(std::memory_order_acquire))
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(repair_interval_milliseconds));

            if (repair_stop.load(std::memory_order_acquire))
            {
                break;
            }

            // repair 与本轮 stop 并发；计数只表示尝试发生。
            pool.repairDeadChannels();

            std::lock_guard<std::mutex> lock(ctx.mutex);
            ++ctx.repair_attempts;
        }
    }

    bool validateResult(const StressSnapshot &result, bool drained)
    {
        bool passed = true;
        if (result.submitted != result.completed)
        {
            std::cerr << "FAILED: submitted != completed\n";
            passed = false;
        }
        if (result.inflight != 0)
        {
            std::cerr << "FAILED: final inflight != 0\n";
            passed = false;
        }
        if (result.success + result.failed != result.completed)
        {
            std::cerr << "FAILED: success + failed != completed\n";
            passed = false;
        }
        if (result.duplicate_done != 0)
        {
            std::cerr << "FAILED: duplicate done callbacks=" << result.duplicate_done << '\n';
            passed = false;
        }
        if (!drained)
        {
            std::cerr << "FAILED: calls did not drain\n";
            passed = false;
        }

        return passed;
    }

} // namespace

std::string buildLoginResponseBody(const myrpc::RpcHeader &, const std::string &)
{
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

    ControlledTcpServer server(0, buildLoginResponseBody);
    if (!server.start())
    {
        std::cerr << "ControlledTcpServer::start() failed\n";
        return 1;
    }

    StressContext ctx;
    ctx.max_inflight = options.max_inflight;
    const auto deadline = Clock::now() + std::chrono::seconds(options.duration_seconds);

    RpcChannelPool pool(options.ip, server.port(), options.pool_size);
    pool.setTimeoutMs(options.timeout_ms);

    int completed_rounds = 0;
    bool drained = true;
    for (int round = 0; round < options.rounds && Clock::now() < deadline; ++round)
    {
        if (!pool.start())
        {
            std::cerr << "RpcChannelPool::start() failed in round " << round << '\n';
            drained = false;
            break;
        }

        std::atomic<bool> round_stop{false};
        std::vector<std::thread> submitters;
        submitters.reserve(static_cast<size_t>(options.client_threads));
        for (int i = 0; i < options.client_threads; ++i)
        {
            submitters.emplace_back([&pool, &ctx, &round_stop, deadline]() {
                demo::UserService_Stub stub(&pool);
                while (reserveSubmission(ctx, round_stop, deadline))
                {
                    try
                    {
                        submitOne(stub, ctx);
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

        std::thread disconnect_thread(
            runDisconnect, std::ref(ctx), std::ref(round_stop), std::ref(server),
            options.disconnect_interval_milliseconds);
        std::thread repair_thread(
            runRepair, std::ref(ctx), std::ref(round_stop), std::ref(pool),
            options.repair_interval_milliseconds);

        // 在提交、断连和 repair 都运行时触发 stop，扩大生命周期竞争窗口。
        std::this_thread::sleep_for(
            std::chrono::milliseconds(options.stop_delay_milliseconds));

        pool.stop();

        round_stop.store(true, std::memory_order_release);
        ctx.cv.notify_all();
        disconnect_thread.join();
        repair_thread.join();
        for (auto &submitter : submitters)
        {
            submitter.join();
        }

        // stop 返回后，每一轮都必须收束本轮已接受的请求。
        const bool round_drained = waitUntilDrained(
            ctx, std::chrono::milliseconds(std::max(10000, options.timeout_ms * 2)));
        if (!round_drained)
        {
            std::cerr << "FAILED: calls did not drain in round " << round << '\n';
            drained = false;
            break;
        }

        ++completed_rounds;
    }

    const StressSnapshot result = snapshot(ctx);
    server.stop();

    std::cout << "submit_stop_race_stress\n"
              << "rounds_completed=" << completed_rounds << '\n'
              << "submitted=" << result.submitted << '\n'
              << "completed=" << result.completed << '\n'
              << "success=" << result.success << '\n'
              << "failed=" << result.failed << '\n'
              << "local_submit_failed=" << result.local_submit_failed << '\n'
              << "duplicate_done=" << result.duplicate_done << '\n'
              << "final_inflight=" << result.inflight << '\n'
              << "disconnect_attempts=" << result.disconnect_attempts << '\n'
              << "repair_attempts=" << result.repair_attempts << '\n'
              << "drained=" << (drained ? "true" : "false") << '\n';

    return validateResult(result, drained) ? 0 : 2;
}
