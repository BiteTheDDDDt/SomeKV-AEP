cmake_minimum_required(VERSION 3.1)

project("SomeKV-AEP")

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include_directories(
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/test
)

add_subdirectory(src)

if(MAKE_TEST STREQUAL "ON")
    add_subdirectory(test)
endif()