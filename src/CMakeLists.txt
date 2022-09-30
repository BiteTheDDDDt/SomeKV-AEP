cmake_minimum_required(VERSION 3.1)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Ofast -std=c++17 -march=native -gdwarf-5 -Wall")

add_library(interface SHARED interface.cpp)

find_library(libpmem pmem)

target_link_libraries (interface glog::glog)
target_link_libraries (interface ${libpmem} -lpmem)

set(LIBRARY_OUTPUT_PATH ${CMAKE_SOURCE_DIR}/build)
