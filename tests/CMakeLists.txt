# tests/CMakeLists.txt

# ============================================================
# target = test_protocol
# ============================================================

set(TEST_GENERATED_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated)
file(MAKE_DIRECTORY ${TEST_GENERATED_DIR})

set(TEST_GHOST_PROTO ${CMAKE_CURRENT_SOURCE_DIR}/proto/ghost_service.proto)
set(TEST_GHOST_PB_CC ${TEST_GENERATED_DIR}/ghost_service.pb.cc)
set(TEST_GHOST_PB_H  ${TEST_GENERATED_DIR}/ghost_service.pb.h)

add_custom_command(
    OUTPUT
        ${TEST_GHOST_PB_CC}
        ${TEST_GHOST_PB_H}
    COMMAND
        ${Protobuf_PROTOC_EXECUTABLE}
        --cpp_out=${TEST_GENERATED_DIR}
        -I ${CMAKE_CURRENT_SOURCE_DIR}/proto
        -I ${PROJECT_SOURCE_DIR}/proto
        ${TEST_GHOST_PROTO}
    DEPENDS
        ${TEST_GHOST_PROTO}
    COMMENT
        "Generating ghost_service.pb.cc/h"
    VERBATIM
)

add_library(test_protocol STATIC
    ${TEST_GHOST_PB_CC}
    ${TEST_GHOST_PB_H}
)

target_include_directories(test_protocol PUBLIC
    ${TEST_GENERATED_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/proto
    ${PROJECT_SOURCE_DIR}/examples
)

target_link_libraries(test_protocol PUBLIC
    protobuf::libprotobuf
    user_proto
)

myrpc_apply_sanitizers(test_protocol)


# ============================================================
# target = tcpserver_core
# ============================================================

set(TCPSERVER_DIR ${CMAKE_CURRENT_SOURCE_DIR}/integration/tcpserver_for_test)

add_library(tcpserver_core STATIC
    ${TCPSERVER_DIR}/tcpserver.cc
)

target_include_directories(tcpserver_core PUBLIC
    ${TCPSERVER_DIR}
)

target_link_libraries(tcpserver_core PUBLIC
    myrpc_core
    user_proto
    Threads::Threads
)

myrpc_apply_sanitizers(tcpserver_core)


# ============================================================
# GTest
# ============================================================

find_package(GTest REQUIRED)
include(GoogleTest)


# ============================================================
# helper
# ============================================================

function(add_myrpc_test target_name source_file output_subdir)
    if (EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/${source_file}")
        add_executable(${target_name}
            ${source_file}
        )

        if (NOT "${output_subdir}" STREQUAL "")
            set_target_properties(${target_name} PROPERTIES
                RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/${output_subdir}"
            )
        endif()

        target_include_directories(${target_name} PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}
            ${TEST_GENERATED_DIR}
            ${PROJECT_SOURCE_DIR}/examples
        )

        target_link_libraries(${target_name} PRIVATE
            myrpc_core
            user_proto
            tcpserver_core
            Threads::Threads
            GTest::gtest_main
            ${ARGN}
        )

        myrpc_apply_sanitizers(${target_name})

        gtest_discover_tests(${target_name}
            DISCOVERY_MODE PRE_TEST
        )

        if (MYRPC_ENABLE_TEST_HOOKS)
            target_compile_definitions(${target_name} PRIVATE MYRPC_ENABLE_TEST_HOOKS)
        endif()                  
    else()
        message(WARNING "Test source not found: ${source_file}, skip target ${target_name}")
    endif()
endfunction()

if (MYRPC_BUILD_TESTS)
    add_myrpc_test(rpc_channel_pool_test integration/rpc_channel_pool_test.cc integration)
    add_myrpc_test(threadpool_test unit/threadpool_test.cc unit)
    add_myrpc_test(callback_executor_test unit/callback_executor_test.cc unit)
    add_myrpc_test(pending_call_manager_test unit/pending_call_manager_test.cc unit)
    add_myrpc_test(rpc_codec_test unit/rpc_codec_test.cc unit)
    add_myrpc_test(rpc_provider_test unit/rpc_provider_test.cc unit)
    add_myrpc_test(rpc_timeout_manager_test unit/rpc_timeout_manager_test.cc unit)
    add_myrpc_test(rpc_transport_test unit/rpc_transport_test.cc unit)

    add_myrpc_test(bad_packet_test integration/bad_packet_test.cc integration)
    add_myrpc_test(batch_async integration/batch_async.cc integration)
    add_myrpc_test(connection_lost integration/connection_lost.cc integration)
    add_myrpc_test(multithread_call integration/multithread_call.cc integration)
    add_myrpc_test(normal_async integration/normal_async.cc integration)
    add_myrpc_test(not_connection integration/not_connection.cc integration)
    add_myrpc_test(rpc_channel_timeout_test integration/rpc_channel_timeout_test.cc integration)
    add_myrpc_test(request_1000_test integration/request1000_test.cc integration)
    add_myrpc_test(service_not_found integration/service_not_found.cc integration test_protocol)
    add_myrpc_test(service_not_found_test integration/service_not_found_test.cc integration)
    add_myrpc_test(test_tcp_fragment integration/test_tcp_fragment.cc integration)
    add_myrpc_test(future_test integration/future_test.cc integration)
    add_myrpc_test(rpc_channel_race_test integration/rpc_channel_race_test.cc integration)
    if (MYRPC_ENABLE_TEST_HOOKS)
        target_compile_definitions(rpc_channel_race_test
                                PRIVATE
                                    MYRPC_ENABLE_TEST_HOOKS    
        )
    endif()                           
endif()


if(MYRPC_BUILD_STRESS_TESTS)
    add_subdirectory(stress)
endif()

