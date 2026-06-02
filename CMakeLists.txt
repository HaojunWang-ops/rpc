cmake_minimum_required(VERSION 3.16)

project(myrpc LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(MYRPC_BUILD_EXAMPLES "Build examples" ON)
option(MYRPC_ENABLE_ASAN "Enable AddressSanitizer" OFF)
option(MYRPC_ENABLE_TSAN "Enable ThreadSanitizer" OFF)

if (MYRPC_ENABLE_ASAN AND MYRPC_ENABLE_TSAN)
    message (FATAL_ERROR "Asan and Tsan cannot be enabled at the same time")
endif()

find_package(Threads REQUIRED)
find_package(Protobuf REQUIRED)

add_subdirectory(third_party/mini_muduo)

set(MYRPC_PROTO_FILES
    ${CMAKE_CURRENT_SOURCE_DIR}/proto/rpc_header.proto
)

# 生成生成 C++ 源文件（.pb.cc 和 .pb.h），结果存入 PROTO_SRCS 和 PROTO_HDRS 变量
protobuf_generate_cpp(MYRPC_PROTO_SRCS MYRPC_PROTO_HDRS ${MYRPC_PROTO_FILES})

add_library(myrpc STATIC
    src/rpc_provider.cc
    src/rpc_channel.cc
    src/rpc_controller.cc

    ${MYRPC_PROTO_HDRS}
    ${MYRPC_PROTO_SRCS}
)

target_include_directories(myrpc
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include/myrpc
        ${CMAKE_CURRENT_BINARY_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/third_party/mini_muduo
        ${CMAKE_CURRENT_SOURCE_DIR}   
)

target_link_libraries(myrpc
    PUBLIC
        mini_muduo::mini_muduo
        protobuf::libprotobuf
        Threads::Threads
)

if (MYRPC_ENABLE_ASAN)
    target_compile_options(myrpc PUBLIC -fsanitize=address -fno-omit-frame-pointer)
    target_link_options(myrpc PUBLIC -fsanitize=address)
endif()

if (MYRPC_ENABLE_TSAN)
    target_compile_options(myrpc PUBLIC -fsanitize=thread -fno-omit-frame-pointer)
    target_link_options(myrpc PUBLIC -fsanitize=thread)
endif()

if (MYRPC_BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()