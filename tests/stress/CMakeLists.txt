find_package(Threads REQUIRED)

function(add_myrpc_stress_program target source)
    add_executable(${target}
        ${source}
    )

    target_link_libraries(${target}
        PRIVATE
            myrpc_core
            user_proto
            protobuf::libprotobuf
            Threads::Threads
            tcpserver_core
    )

    set(test_args
        --ip=127.0.0.1
        --port=8000
        --duration-seconds=300
        --pool_size=4
        --client_threads=4
        --max-inflight=1000
        --disconnect-interval-ms=100
    )

    if (${target} STREQUAL "timeout_stress")
        list(APPEND test_args
            --pool_size=16
            --max-inflight=1000
            --timeout-ms=40
            --max-requests=300000
        )
    endif()

    add_test(
        NAME ${target}
        COMMAND ${target} ${test_args}
    )

    set_tests_properties(${target}
        PROPERTIES
            LABELS "stress"
            TIMEOUT 120
    )
endfunction()

add_myrpc_stress_program(
    async_sustained_stress
    async_sustained_stress.cc
)

add_myrpc_stress_program(
    disconnect_repair_stress
    disconnect_repair_stress.cc
)

add_myrpc_stress_program(
    submit_stop_race_stress
    submit_stop_race_stress.cc
)

add_myrpc_stress_program(
    timeout_stress
    timeout_stress.cc
)
