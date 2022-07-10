cmake_minimum_required(VERSION 3.1)
project("interface")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -std=c++17")

# generate .so file
include_directories(inc)
add_library(interface
SHARED
        src/interface.cpp
)