# examples/CMakeLists.txt

add_executable(provider
    provider_main.cc
)

target_link_libraries(provider PRIVATE
    myrpc_core
    user_service
)

myrpc_apply_sanitizers(provider)


add_executable(consumer
    consumer_main.cc
)

target_link_libraries(consumer PRIVATE
    myrpc_core
    user_proto
)

myrpc_apply_sanitizers(consumer)