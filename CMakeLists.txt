cmake_minimum_required(VERSION 3.1)

project("SomeKV-AEP")

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(BUILD_STATIC ${MAKE_TEST})

include_directories(
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/test
)

add_subdirectory(src)

add_subdirectory(thirdparty/glog-0.6.0)
add_subdirectory(thirdparty/abseil-cpp-20220623.0)

if(MAKE_TEST STREQUAL "ON")
    add_subdirectory(test)
endif()