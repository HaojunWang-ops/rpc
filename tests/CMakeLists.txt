# tests/CMakeLists.txt

## ====== target = test_protocol
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
)

target_link_libraries(test_protocol PUBLIC
    protobuf::libprotobuf
)

myrpc_apply_sanitizers(test_protocol)

# =========

# ========= target = tcpserver

set(TCPSERVER_DIR ${CMAKE_CURRENT_SOURCE_DIR}/integration/tcpserver_for_test)

add_library(tcpserver_core STATIC
    ${TCPSERVER_DIR}/tcpserver.cc    
)

target_link_libraries(tcpserver_core
    PUBLIC
    myrpc_core
    user_proto
    Threads::Threads
)

target_include_directories(tcpserver_core PUBLIC
    ${TCPSERVER_DIR}
)

myrpc_apply_sanitizers(tcpserver_core)
# =========
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

        target_link_libraries(${target_name} PRIVATE
            myrpc_core
            Threads::Threads
            ${ARGN}
        )

        myrpc_apply_sanitizers(${target_name})
    else()
        message(WARNING "Test source not found: ${source_file}, skip target ${target_name}")
    endif()
endfunction()

function(add_gtest_test TARGET_NAME SOURCE_FILE OUTPUT_SUBDIR)
    if (NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/${SOURCE_FILE}")
        return()
    endif()

    find_package(GTest REQUIRED)
    add_executable(${TARGET_NAME} ${SOURCE_FILE})
    target_link_libraries(${TARGET_NAME} PRIVATE
        myrpc_core
        user_proto
        tcpserver_core
        GTest::gtest_main
    )
    include(GoogleTest)
    gtest_discover_tests(${TARGET_NAME})
    set_target_properties(${TARGET_NAME} PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/${OUTPUT_SUBDIR}"
    )

    myrpc_apply_sanitizers(${TARGET_NAME})
endfunction()

add_gtest_test(rpc_channel_pool_test1 unit/rpc_channel_pool_test1.cc unit)
add_gtest_test(rpc_channel_pool_test2 unit/rpc_channel_pool_test2.cc unit)
add_gtest_test(rpc_channel_pool_test3 unit/rpc_channel_pool_test3.cc unit)

add_myrpc_test(bad_packet_test
    integration/bad_packet_test.cc
    integration
)

add_myrpc_test(service_not_found_test
    integration/service_not_found_test.cc
    integration
    user_proto
)

add_myrpc_test(threadpool_test
    unit/threadpool_test.cc
    unit
)

add_myrpc_test(request_1000_test
    integration/request1000_test.cc
    integration
    user_proto
)

add_myrpc_test(test_tcp_fragment
    integration/test_tcp_fragment.cc
    integration
    user_proto
)

add_myrpc_test(batch_async
    integration/batch_async.cc
    integration
    user_proto
)

add_myrpc_test(multithread_call
    integration/multithread_call.cc
    integration
    user_proto
)

add_myrpc_test(normal_async
    integration/normal_async.cc
    integration
    user_proto
)

if (EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/integration/service_not_found.cc)
    find_package(GTest REQUIRED)    

    add_executable(service_not_found
        integration/service_not_found.cc
    )

    target_link_libraries(service_not_found
        PRIVATE
            myrpc_core
            test_protocol
            GTest::gtest_main
    )

    include(GoogleTest)
    gtest_discover_tests(service_not_found)

    set_target_properties(service_not_found PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/integration"
    )

    myrpc_apply_sanitizers(service_not_found)
endif()

if (EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/integration/connection_lost.cc)
    find_package(GTest REQUIRED)    

    add_executable(connection_lost
        integration/connection_lost.cc
    )

    target_link_libraries(connection_lost
        PRIVATE
            myrpc_core
            user_proto
            Threads::Threads
            GTest::gtest_main
    )

    include(GoogleTest)
    gtest_discover_tests(connection_lost)

    set_target_properties(connection_lost PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/integration"
    )

    myrpc_apply_sanitizers(connection_lost)
endif()

if (EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/integration/not_connection.cc)
    find_package(GTest REQUIRED)    

    add_executable(not_connection
        integration/not_connection.cc
    )

    target_link_libraries(not_connection
        PRIVATE
            myrpc_core
            user_proto
            GTest::gtest_main
    )

    include(GoogleTest)
    gtest_discover_tests(not_connection)

    set_target_properties(not_connection PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/integration"
    )

    myrpc_apply_sanitizers(not_connection)
endif()


add_gtest_test(rpc_channel_pool_test integration/rpc_channel_pool_test.cc integration)