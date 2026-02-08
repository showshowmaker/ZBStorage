cmake_minimum_required(VERSION 3.16)
project(ZBStorage CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/third_party/brpc/cmake")

option(BUILD_BRPC_TOOLS "Build brpc tools" OFF)
option(BUILD_UNIT_TESTS "Build brpc unit tests" OFF)
set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)

add_subdirectory(third_party/brpc)

find_package(Protobuf REQUIRED)
find_package(GFLAGS REQUIRED)
find_package(Threads REQUIRED)

set(PROTO_FILE ${CMAKE_CURRENT_SOURCE_DIR}/src/msg/real_node.proto)
set(PROTOBUF_IMPORT_DIRS ${CMAKE_CURRENT_SOURCE_DIR}/src/msg ${Protobuf_INCLUDE_DIRS})

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILE})

add_library(real_node_proto ${PROTO_SRCS} ${PROTO_HDRS})
target_include_directories(real_node_proto PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(real_node_proto PUBLIC ${Protobuf_LIBRARIES})

set(REAL_NODE_SOURCES
    src/data_node/real_node/config/NodeConfig.cpp
    src/data_node/real_node/io/DiskManager.cpp
    src/data_node/real_node/io/LocalPathResolver.cpp
    src/data_node/real_node/io/IOExecutor.cpp
    src/data_node/real_node/service/StorageServiceImpl.cpp
    src/data_node/real_node/service/BrpcStorageService.cpp
    src/data_node/real_node/server/real_node_server.cpp
)

add_executable(real_node_server ${REAL_NODE_SOURCES})
target_include_directories(real_node_server
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${CMAKE_CURRENT_SOURCE_DIR}/third_party/brpc/src
        ${GFLAGS_INCLUDE_PATH}
        ${CMAKE_CURRENT_BINARY_DIR}
)

target_link_libraries(real_node_server
    PRIVATE
        real_node_proto
        brpc-static
        ${GFLAGS_LIBRARY}
        ${CMAKE_THREAD_LIBS_INIT}
)

get_directory_property(BRPC_DYNAMIC_LIB DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/third_party/brpc DEFINITION DYNAMIC_LIB)
if(BRPC_DYNAMIC_LIB)
    target_link_libraries(real_node_server PRIVATE ${BRPC_DYNAMIC_LIB})
endif()

add_executable(real_node_client
    src/data_node/real_node/client/real_node_client.cpp
)
target_include_directories(real_node_client
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${CMAKE_CURRENT_SOURCE_DIR}/third_party/brpc/src
        ${GFLAGS_INCLUDE_PATH}
        ${CMAKE_CURRENT_BINARY_DIR}
)
target_link_libraries(real_node_client
    PRIVATE
        real_node_proto
        brpc-static
        ${GFLAGS_LIBRARY}
        ${CMAKE_THREAD_LIBS_INIT}
)
if(BRPC_DYNAMIC_LIB)
    target_link_libraries(real_node_client PRIVATE ${BRPC_DYNAMIC_LIB})
endif()

add_executable(real_node_multi_test
    src/client/test/real_node_multi_test.cpp
)
target_include_directories(real_node_multi_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${CMAKE_CURRENT_SOURCE_DIR}/third_party/brpc/src
        ${GFLAGS_INCLUDE_PATH}
        ${CMAKE_CURRENT_BINARY_DIR}
)
target_link_libraries(real_node_multi_test
    PRIVATE
        real_node_proto
        brpc-static
        ${GFLAGS_LIBRARY}
        ${CMAKE_THREAD_LIBS_INIT}
)
if(BRPC_DYNAMIC_LIB)
    target_link_libraries(real_node_multi_test PRIVATE ${BRPC_DYNAMIC_LIB})
endif()
