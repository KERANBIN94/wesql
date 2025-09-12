cmake_minimum_required(VERSION 3.10)

project(wesql)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

include_directories(src)

file(GLOB_RECURSE SOURCES "src/*.cpp")

add_executable(wesql ${SOURCES})