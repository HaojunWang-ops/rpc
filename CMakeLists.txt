cmake_minimum_required(VERSION 3.16)

project(my_rpc LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
option(MYRPC_BUILD_EXAMPLES "Build examples" ON)
option(MYRPC_BUILD_TESTS "Build tests" ON)
option(MYRPC_ENABLE_ASAN "Enable AddressSanitizer" OFF)
option(MYRPC_ENABLE_TSAN "Enable ThreadSanitizer" OFF)

if (MYRPC_ENABLE_ASAN AND MYRPC_ENABLE_TSAN)
    message(FATAL_ERROR "ASan and TSan cannot be enabled at the same time")
endif()

find_package(Threads REQUIRED)
find_package(Protobuf REQUIRED)

# ============================================================
# Sanitizer helper
# ============================================================

function(myrpc_apply_sanitizers target_name)
    if (MYRPC_ENABLE_ASAN)
        target_compile_options(${target_name} PRIVATE
            -fsanitize=address
            -fno-omit-frame-pointer
            -g
        )
        target_link_options(${target_name} PRIVATE
            -fsanitize=address
        )
    endif()

    if (MYRPC_ENABLE_TSAN)
        target_compile_options(${target_name} PRIVATE
            -fsanitize=thread
            -fno-omit-frame-pointer
            -g
        )
        target_link_options(${target_name} PRIVATE
            -fsanitize=thread
        )
    endif()
endfunction()

# ============================================================
# mini_muduo / reactor
# ============================================================

set(MINI_MUDUO_ROOT
    "${CMAKE_CURRENT_SOURCE_DIR}/third_party/mini_muduo"
    CACHE PATH
    "Path to mini_muduo project"
)

set(MYRPC_NET_TARGET
    "mini_muduo"
    CACHE STRING
    "Network library CMake target name"
)

if (EXISTS "${MINI_MUDUO_ROOT}/CMakeLists.txt")
    add_subdirectory(
        ${MINI_MUDUO_ROOT}
        ${CMAKE_BINARY_DIR}/mini_muduo_build
    )
else()
    message(FATAL_ERROR "mini_muduo not found at ${MINI_MUDUO_ROOT}")
endif()

if (NOT TARGET ${MYRPC_NET_TARGET})
    message(FATAL_ERROR "Network target ${MYRPC_NET_TARGET} not found. Please set -DMYRPC_NET_TARGET=<target_name>")
endif()

# ============================================================
# Generated dirs
# ============================================================

set(MYRPC_GENERATED_DIR ${CMAKE_BINARY_DIR}/generated)
set(MYRPC_CORE_GENERATED_DIR ${MYRPC_GENERATED_DIR}/myrpc)
set(USER_GENERATED_DIR ${MYRPC_GENERATED_DIR}/user)

file(MAKE_DIRECTORY ${MYRPC_CORE_GENERATED_DIR})
file(MAKE_DIRECTORY ${USER_GENERATED_DIR})

# ============================================================
# rpc_header.proto -> myrpc_rpc_header_proto
# ============================================================

set(RPC_HEADER_PROTO ${CMAKE_CURRENT_SOURCE_DIR}/src/rpc_header.proto)
set(RPC_HEADER_PB_CC ${MYRPC_CORE_GENERATED_DIR}/rpc_header.pb.cc)
set(RPC_HEADER_PB_H  ${MYRPC_CORE_GENERATED_DIR}/rpc_header.pb.h)

add_custom_command(
    OUTPUT
        ${RPC_HEADER_PB_CC}
        ${RPC_HEADER_PB_H}
    COMMAND
        ${Protobuf_PROTOC_EXECUTABLE}
        --cpp_out=${MYRPC_CORE_GENERATED_DIR}
        -I ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${RPC_HEADER_PROTO}
    DEPENDS
        ${RPC_HEADER_PROTO}
    COMMENT
        "Generating rpc_header.pb.cc/h"
    VERBATIM
)

add_library(myrpc_protocol STATIC
    ${RPC_HEADER_PB_CC}
)

target_include_directories(myrpc_protocol PUBLIC
    ${MYRPC_CORE_GENERATED_DIR}
)

target_link_libraries(myrpc_protocol PUBLIC
    protobuf::libprotobuf
)

myrpc_apply_sanitizers(myrpc_protocol)

# ============================================================
# user.proto -> user_proto
# ============================================================

set(USER_PROTO ${CMAKE_CURRENT_SOURCE_DIR}/proto/user.proto)
set(USER_PB_CC ${USER_GENERATED_DIR}/user.pb.cc)
set(USER_PB_H  ${USER_GENERATED_DIR}/user.pb.h)

add_custom_command(
    OUTPUT
        ${USER_PB_CC}
        ${USER_PB_H}
    COMMAND
        ${Protobuf_PROTOC_EXECUTABLE}
        --cpp_out=${USER_GENERATED_DIR}
        -I ${CMAKE_CURRENT_SOURCE_DIR}/proto
        ${USER_PROTO}
    DEPENDS
        ${USER_PROTO}
    COMMENT
        "Generating user.pb.cc/h"
    VERBATIM
)

add_library(user_proto STATIC
    ${USER_PB_CC}
)

target_include_directories(user_proto PUBLIC
    ${USER_GENERATED_DIR}
)

target_link_libraries(user_proto PUBLIC
    protobuf::libprotobuf
)

myrpc_apply_sanitizers(user_proto)

# ============================================================
# myrpc_core
# ============================================================

add_library(myrpc_core STATIC
    src/rpc_channel.cc
    src/rpc_provider.cc
    src/rpc_controller.cc
    src/ThreadPool.cc
)

target_include_directories(myrpc_core PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include/myrpc
)

target_link_libraries(myrpc_core PUBLIC
    myrpc_protocol
    protobuf::libprotobuf
    Threads::Threads
    ${MYRPC_NET_TARGET}
)

myrpc_apply_sanitizers(myrpc_core)

# ============================================================
# user_service
# ============================================================

add_library(user_service STATIC
    services/user_services.cc
)

target_include_directories(user_service PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/services
)

target_link_libraries(user_service PUBLIC
    user_proto
    myrpc_core
)

myrpc_apply_sanitizers(user_service)

# ============================================================
# subdirectories
# ============================================================

if (MYRPC_BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()

if (MYRPC_BUILD_TESTS)
    add_subdirectory(tests)
endif()

#[[
third_party/mini_muduo
        │
        ▼
   mini_muduo
        │
        ▼
   myrpc_core ◄──────── myrpc_protocol ◄──── src/rpc_header.proto
        ▲                     │
        │                     ▼
        │                protobuf
        │
        ├──────────── Threads::Threads
        │
        ▼
   user_service ◄──── user_proto ◄──── proto/user.proto
        │
        ▼
   provider_main

]]