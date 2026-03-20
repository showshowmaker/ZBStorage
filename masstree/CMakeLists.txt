cmake_minimum_required(VERSION 3.1)
 
project(hello_library)

# Masstree
aux_source_directory(${PROJECT_SOURCE_DIR}/index/masstree MASSTREE_SOURCE_FILES)
add_library(masstree ${MASSTREE_SOURCE_FILES})
target_compile_options(masstree PRIVATE
          $<$<COMPILE_LANGUAGE:CXX>:-include ${PROJECT_SOURCE_DIR}/index/masstree/config.h>)
 
# 添加头文件目录
target_include_directories(masstree PUBLIC ${PROJECT_SOURCE_DIR}/index/masstree)
 
# 定义一个可执行文件
# add_executable(scan_test src/scan_test.cpp)
#add_executable(scan_allpath src/scan_allpath.cpp)
#add_executable(scan_infinifs src/scan_infinifs.cpp)
add_executable(Parallel_insertion_test src/Parallel_insertion_test.cpp)
add_executable(bloom_test src/bloom_test.cpp)
add_executable(Parallel_insertion_test_new src/Parallel_insertion_test_new.cpp)
add_executable(recover src/recover.cpp)
 
# message(STATUS ${scan_test})
#message(STATUS ${scan_allpath})
#message(STATUS ${scan_infinifs})
message(STATUS ${Parallel_insertion_test})
message(STATUS ${bloom_test})
message(STATUS ${Parallel_insertion_test_new})
message(STATUS ${recover})

# 安装静态库到 /usr/local/lib 目录
install(TARGETS masstree DESTINATION /usr/local/lib)

# 安装可执行文件到 /usr/local/bin 目录
# install(TARGETS scan_test DESTINATION /usr/local/bin)
#install(TARGETS scan_allpath DESTINATION /usr/local/bin)
#install(TARGETS scan_infinifs DESTINATION /usr/local/bin)
install(TARGETS Parallel_insertion_test DESTINATION /usr/local/bin)
install(TARGETS bloom_test DESTINATION /usr/local/bin)
install(TARGETS Parallel_insertion_test_new DESTINATION /usr/local/bin)
install(TARGETS recover DESTINATION /usr/local/bin)
 
# 将静态库 MyLib 链接到可执行文件 MyApp 上
#target_link_libraries(scan_test PRIVATE masstree)
#target_link_libraries(scan_allpath PRIVATE masstree)
#target_link_libraries(scan_infinifs PRIVATE masstree)
target_link_libraries(Parallel_insertion_test PRIVATE masstree)
target_link_libraries(bloom_test PRIVATE masstree)
target_link_libraries(Parallel_insertion_test_new PRIVATE masstree)
target_link_libraries(recover PRIVATE masstree)

find_package(OpenSSL REQUIRED)
#target_link_libraries(scan_test PRIVATE OpenSSL::SSL OpenSSL::Crypto)
#target_link_libraries(scan_allpath PRIVATE OpenSSL::SSL OpenSSL::Crypto)
#target_link_libraries(scan_infinifs PRIVATE OpenSSL::SSL OpenSSL::Crypto)
target_link_libraries(Parallel_insertion_test PRIVATE OpenSSL::SSL OpenSSL::Crypto)
target_link_libraries(bloom_test PRIVATE OpenSSL::SSL OpenSSL::Crypto)
target_link_libraries(Parallel_insertion_test_new PRIVATE OpenSSL::SSL OpenSSL::Crypto)
target_link_libraries(recover PRIVATE OpenSSL::SSL OpenSSL::Crypto)

