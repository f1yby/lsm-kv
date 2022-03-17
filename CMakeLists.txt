cmake_minimum_required(VERSION 3.16)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

project(LSM_KV)
add_executable(LSM_KV lsm.cpp)
include_directories(include)
include_directories(third_party)
