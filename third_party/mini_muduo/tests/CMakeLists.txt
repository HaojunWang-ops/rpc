find_package(GTest REQUIRED)

add_executable(unit_tests
    Buffer_test.cc
    LogStream_test.cc
    Timestamp_test.cc
    InetAddress_test.cc    
)

target_link_libraries(unit_tests
    PRIVATE
    mini_muduo
    GTest::gtest
    GTest::gtest_main
)

target_include_directories(unit_tests
    PRIVATE
    ${PROJECT_SOURCE_DIR}
    ${PROJECT_SOURCE_DIR}/net
    ${PROJECT_SOURCE_DIR}/base
)

include(GoogleTest)
gtest_discover_tests(unit_tests)