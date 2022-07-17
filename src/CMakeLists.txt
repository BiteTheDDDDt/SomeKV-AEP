cmake_minimum_required(VERSION 3.1)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -std=c++17")

add_library(interface SHARED interface.cpp)

set(LIBRARY_OUTPUT_PATH ${CMAKE_SOURCE_DIR}/build)
