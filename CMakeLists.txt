cmake_minimum_required(VERSION 3.10)
project(zero_copy_ringbuf)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable(zero_copy_demo main.cpp)

target_link_libraries(zero_copy_demo pthread)