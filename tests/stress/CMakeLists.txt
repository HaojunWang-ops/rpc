find_package(Threads REQUIRED)

function(add_myrpc_stress_program target source ctest_timeout test_labels)
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

    add_test(
        NAME ${target}
        COMMAND ${target} ${ARGN}
    )

    set_tests_properties(${target}
        PROPERTIES
            LABELS "${test_labels}"
            TIMEOUT ${ctest_timeout}
    )

    myrpc_apply_sanitizers(${target})
endfunction()

add_myrpc_stress_program(
    async_sustained_stress
    async_sustained_stress.cc
    360
    "stress"
    --ip=127.0.0.1
    --port=8000
    --duration-seconds=300
    --pool_size=4
    --client_threads=4
    --max-inflight=1000
    --timeout-ms=3000
    --report-interval-seconds=5
    --max-requests=0
)

add_myrpc_stress_program(
    disconnect_repair_stress
    disconnect_repair_stress.cc
    360
    "stress"
    --ip=127.0.0.1
    --port=8000
    --duration-seconds=300
    --pool_size=4
    --client_threads=4
    --max-inflight=1000
    --timeout-ms=3000
    --report-interval-seconds=5
    --max-requests=0
    --disconnect-interval-ms=100
)

add_myrpc_stress_program(
    submit_stop_race_stress
    submit_stop_race_stress.cc
    90
    "stress"
    --ip=127.0.0.1
    --port=8000
    --duration-seconds=30
    --pool_size=4
    --client_threads=4
    --max-inflight=1000
    --timeout-ms=3000
    --stop-delay-ms=100
    --disconnect-interval-ms=10
    --repair-interval-ms=10
    --stop-rounds=20
)

add_myrpc_stress_program(
    timeout_stress
    timeout_stress.cc
    90
    "stress"
    --ip=127.0.0.1
    --duration-seconds=60
    --pool_size=16
    --client_threads=4
    --max-inflight=16
    --timeout-ms=40
    --max-requests=3000
)
