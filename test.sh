#!/usr/bin/env bash

for i in $(seq 1 20); do
    # 1. 清理上次的临时文件
    rm -f /tmp/rpc_pool_hang.log /tmp/gdb_bt.txt

    # 2. 后台启动测试，输出重定向
    ./build/tests/unit/rpc_channel_pool_test \
        --gtest_filter=RpcChannelPoolTest.ConcurrentPickAndRepairShouldNotCrash \
        > /tmp/rpc_pool_hang.log 2>&1 &
    pid=$!

    # 3. 等待 3 秒，给测试挂起的机会
    sleep 3

    # 4. 检查进程是否仍然存活（kill -0 只检测是否存在，不发信号）
    if kill -0 $pid 2>/dev/null; then
        # 还活着 → 认为发生了“挂起”
        echo "HANG run=$i pid=$pid"

        # 用 gdb 抓取所有线程堆栈
        gdb -q -batch \
            -ex "set pagination off" \
            -ex "info threads" \
            -ex "thread apply all bt" \
            -p $pid > /tmp/gdb_bt.txt 2>&1

        # 终止进程（先软后硬）
        kill -TERM $pid 2>/dev/null || true
        sleep 1
        kill -KILL $pid 2>/dev/null || true

        # 输出日志和堆栈片段
        echo '--- LOG ---'
        sed -n '1,160p' /tmp/rpc_pool_hang.log
        echo '--- BT ---'
        sed -n '1,420p' /tmp/gdb_bt.txt
        echo -n '-- grep --'
        grep -n "readerInLoop\|finishCallWithError\|repairDeadChannels\|stop\|closeSocket" /tmp/gdb_bt.txt

        # 挂起事件已捕获，退出整个循环（不再继续测试）
        exit 0
    else
        # 进程已退出（正常结束或崩溃），等待其退出码
        wait $pid
        st=$?
        echo "run $i exited $st"
    fi
done

echo no hang