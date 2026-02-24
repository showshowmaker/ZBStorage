cmake_minimum_required(VERSION 3.16)
project(bx_dedup_cpu_parallel LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Threads REQUIRED)

add_executable(bx_dedup
  src/main.cpp
  src/config.cpp
  src/dataset.cpp
  src/chunker.cpp
  src/hasher.cpp
  src/dedup_index.cpp
  src/stats.cpp
  src/engine.cpp
)

target_include_directories(bx_dedup PRIVATE include)

target_link_libraries(bx_dedup PRIVATE Threads::Threads)

if (UNIX AND NOT APPLE)
  target_compile_options(bx_dedup PRIVATE -O3 -Wall -Wextra -Wpedantic)
endif()
