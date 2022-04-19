cmake_minimum_required(VERSION 3.16)
project(LSM_KV)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
#set(ENABLE_PRECOMPILED_HEADERS OFF)


set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,-z,stack-size=100000000")



include_directories(include)
include_directories(third_party)
add_subdirectory(lib)
add_executable(LSM_KV main.cpp)
add_executable(CORRECTNESS correctness.cc)
add_executable(PERSISTENCE persistence.cc)
target_link_libraries(
        CORRECTNESS
        PUBLIC
        lsm
        utils
)
target_link_libraries(
        LSM_KV
        PUBLIC
        lsm
        utils
)
target_link_libraries(
        PERSISTENCE
        PUBLIC
        lsm
        utils
)