cmake_minimum_required(VERSION 3.16)

project(mini_muduo LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(MINI_MUDUO_BUILD_EXAMPLES "Build mini_muduo examples" OFF)
option(MINI_MUDUO_BUILD_TESTS "Build mini_muduo tests" OFF)

option(MINI_MUDUO_ENABLE_ASAN "Enable AddressSanitizer" OFF)
option(MINI_MUDUO_ENABLE_TSAN "Enable ThreadSanitizer" OFF)

if (MINI_MUDUO_ENABLE_ASAN AND MINI_MUDUO_ENABLE_TSAN)
    message(FATAL_ERROR "ASan and TSan cannot be enabled at the same time")
endif()

find_package(Threads REQUIRED)

add_library(mini_muduo STATIC
    base/AsyncLogging.cc
    base/LogFile.cc
    base/LogStream.cc
    base/Condition.cc
    base/CountDownLatch.cc
    base/CurrentThread.cc
    base/Exception.cc
    base/Logging.cc
    base/FileUtil.cc
    base/Thread.cc
    base/Timestamp.cc

    net/Acceptor.cc
    net/Buffer.cc
    net/Channel.cc
    net/EventLoop.cc
    net/EventLoopThread.cc
    net/EventLoopThreadPool.cc
    net/InetAddress.cc
    net/Poller.cc
    net/poller/DefaultPoller.cc
    net/poller/EPollPoller.cc
    net/poller/PollPoller.cc
    net/Socket.cc
    net/SocketsOps.cc
    net/TcpServer.cc
    net/TcpConnection.cc
    net/Timer.cc
    net/TimerQueue.cc
)

add_library(mini_muduo::mini_muduo ALIAS mini_muduo)

target_include_directories(mini_muduo
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/base
        ${CMAKE_CURRENT_SOURCE_DIR}/net
)

target_link_libraries(mini_muduo
    PUBLIC
        Threads::Threads
)

target_compile_options(mini_muduo
    PRIVATE
        -Wall
        -Wextra
        -Wpedantic
        -Wno-unused-parameter
)

function(enable_sanitizers target_name)
    if (MINI_MUDUO_ENABLE_ASAN)
        target_compile_options(${target_name}
            PUBLIC
                -fsanitize=address
                -fno-omit-frame-pointer
                -O0
                -g
        )
        target_link_options(${target_name}
            PUBLIC
                -fsanitize=address
        )
    endif()

    if (MINI_MUDUO_ENABLE_TSAN)
        target_compile_options(${target_name}
            PUBLIC
                -fsanitize=thread
                -fno-omit-frame-pointer
                -O1
                -g
        )
        target_link_options(${target_name}
            PUBLIC
                -fsanitize=thread
        )
    endif()
endfunction()

enable_sanitizers(mini_muduo)

if (MINI_MUDUO_BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()

if (MINI_MUDUO_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()