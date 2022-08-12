cmake_minimum_required(VERSION 3.1)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -std=c++17 -Wall")

add_library(interface SHARED interface.cpp)

find_library(libpmem pmem)

target_link_libraries (interface glog::glog)
target_link_libraries (interface absl::flat_hash_set)
target_link_libraries (interface ${libpmem})
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -lpmem")
set(LIBRARY_OUTPUT_PATH ${CMAKE_SOURCE_DIR}/build)
