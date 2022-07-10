cmake_minimum_required(VERSION 3.1)
project(SomeKV-AEP)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3")

add_library(intserface STATIC IMPORTED)
set_target_properties(intserface PROPERTIES IMPORTED_LOCATION
        ../../build/libinterface.so)

add_executable(SomeKV-AEP test_main.cpp)
target_link_libraries(SomeKV-AEP intserface)