function(add_myrpc_benchmark target_name source_file)
    if (EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/${source_file}")
        add_executable(${target_name}
            ${source_file}
        )

        target_include_directories(${target_name} PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}
        )

        target_link_libraries(${target_name} PRIVATE
            myrpc_core
            user_proto
            Threads::Threads
        )
    else()
        message(WARNING "benchmark source not found: ${source_file}")
    endif()
endfunction()

add_myrpc_benchmark(rpc_sync_single_benchmark rpc_sync_single_benchmark.cc)
add_myrpc_benchmark(rpc_pool_size_benchmark rpc_pool_size_benchmark.cc)
add_myrpc_benchmark(rpc_sync_threads_benchmark rpc_sync_threads_benchmark.cc)
add_myrpc_benchmark(rpc_async_callback_benchmark rpc_async_callback_benchmark.cc)
