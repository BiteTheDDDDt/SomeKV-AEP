cmake_minimum_required(VERSION 3.1)
project(SomeKV-AEP)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -std=c++17 -gdwarf-5")

add_library(intserface STATIC IMPORTED)
set_target_properties(intserface PROPERTIES IMPORTED_LOCATION ${CMAKE_SOURCE_DIR}/build/libinterface.so)

add_executable(SomeKV-AEP test_main.cpp)
target_link_libraries(SomeKV-AEP intserface)
target_link_libraries(SomeKV-AEP glog::glog)