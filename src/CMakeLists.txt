cmake_minimum_required(VERSION 3.1)
include_directories("${PROJECT_SOURCE_DIR}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Ofast -march=native -std=c++17 -Wall")

add_library(interface SHARED interface.cpp)

find_library(libpmem pmem)

target_link_libraries (interface glog::glog)
target_link_libraries (interface absl::flat_hash_set)
target_link_libraries (interface ${libpmem} -lpmem)
set(LIBRARY_OUTPUT_PATH ${CMAKE_SOURCE_DIR}/build)


#add_executable(test main.cpp)
#target_link_libraries (test glog::glog)
#target_link_libraries (test interface)

