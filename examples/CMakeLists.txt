set(EXAMPLE_PROTO_FILES
    ${CMAKE_CURRENT_SOURCE_DIR}/user.proto
)

protobuf_generate_cpp(EXAMPLE_PROTO_SRCS EXAMPLE_PROTO_HDRS ${EXAMPLE_PROTO_FILES})

add_library(example_proto STATIC
    ${EXAMPLE_PROTO_SRCS}
    ${EXAMPLE_PROTO_HDRS}
)

target_include_directories(example_proto
    PUBLIC
        ${CMAKE_CURRENT_BINARY_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(example_proto
    PUBLIC
        protobuf::libprotobuf
)

add_executable(provider
    provider_main.cc
)

target_link_libraries(provider
    PRIVATE
        myrpc
        example_proto
)

add_executable(consumer
    consumer_main.cc
)

target_link_libraries(consumer
    PRIVATE
        myrpc
        example_proto
)