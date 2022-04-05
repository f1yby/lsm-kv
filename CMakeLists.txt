cmake_minimum_required(VERSION 3.16)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
#set(ENABLE_PRECOMPILED_HEADERS OFF)

project(LSM_KV)
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,-z,stack-size=100000000")

include_directories(include)
include_directories(third_party)

add_executable(LSM_KV lsm.cpp)
add_executable(CORRECTNESS correctness.cc)
