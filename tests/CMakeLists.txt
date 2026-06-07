# tests/CMakeLists.txt

if (EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/consumer_test.cc)
    add_executable(consumer_test
        consumer_test.cc
    )

    target_link_libraries(consumer_test PRIVATE
        myrpc_core
        user_proto
        Threads::Threads
    )

    myrpc_apply_sanitizers(consumer_test)
endif()


if (EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/concurrent_test.cc)
    add_executable(concurrent_test
        concurrent_test.cc
    )

    target_link_libraries(concurrent_test PRIVATE
        myrpc_core
        user_proto
        Threads::Threads
    )

    myrpc_apply_sanitizers(concurrent_test)
endif()


if (EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/bad_packet_test.cc)
    add_executable(bad_packet_test
        bad_packet_test.cc
    )

    target_link_libraries(bad_packet_test PRIVATE
        myrpc_core
        Threads::Threads
    )

    myrpc_apply_sanitizers(bad_packet_test)
endif()

if (EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/service_not_found_test.cc)
    add_executable(service_not_found_test
        service_not_found_test.cc
    )

    target_link_libraries(service_not_found_test PRIVATE
        myrpc_core
        user_proto
        Threads::Threads
    )

    myrpc_apply_sanitizers(service_not_found_test)
endif()

if (EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/threadpool_test.cc)
    add_executable(threadpool_test
        threadpool_test.cc
    )

    target_link_libraries(threadpool_test PRIVATE
        myrpc_core
        Threads::Threads
    )

    myrpc_apply_sanitizers(threadpool_test)
endif()

if (EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/1000request_test.cc)
    add_executable(1000request_test
        1000request_test.cc
    )

    target_link_libraries(1000request_test PRIVATE
        myrpc_core
        Threads::Threads
        user_proto
    )

    myrpc_apply_sanitizers(1000request_test)
endif()

if (EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/test_tcp_fragment.cc)
    add_executable(test_tcp_fragment
        test_tcp_fragment.cc
    )

    target_link_libraries(test_tcp_fragment PRIVATE
        myrpc_core
        Threads::Threads
        user_proto
    )

    myrpc_apply_sanitizers(test_tcp_fragment)
endif()