cmake_minimum_required(VERSION 3.1)
include_directories("${PROJECT_SOURCE_DIR}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Ofast -march=native -std=c++17 -Wall -gdwarf-5")

message("BUILD_STATIC=${BUILD_STATIC}")

if(BUILD_STATIC STREQUAL "ON")
    add_library(interface STATIC interface.cpp)
elseif()
    add_library(interface SHARED interface.cpp)
    target_link_libraries(interface glog::glog)
    target_link_libraries(interface absl::flat_hash_set)
    target_link_libraries(interface ${libpmem} -lpmem)
endif()

find_library(libpmem pmem)

set(LIBRARY_OUTPUT_PATH ${CMAKE_SOURCE_DIR}/build)
