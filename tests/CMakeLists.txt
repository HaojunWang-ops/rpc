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