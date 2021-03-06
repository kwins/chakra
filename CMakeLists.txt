cmake_minimum_required(VERSION 3.14)
include(ExternalProject)
include(cmake/misc.cmake)

project(chakra)
add_compile_options(-g -std=c++17)
add_compile_options(-DNDEBUG)

set_property(DIRECTORY PROPERTY EP_BASE ${CMAKE_CURRENT_BINARY_DIR}/depends)

add_subdirectory(${CMAKE_SOURCE_DIR}/3rdparty EXCLUDE_FROM_ALL)

file(GLOB_RECURSE CLIENT_SRC ${CMAKE_SOURCE_DIR}/src/client/*.cpp)
file(GLOB_RECURSE CLUSTER_SRC ${CMAKE_SOURCE_DIR}/src/cluster/*.cpp)
file(GLOB_RECURSE CMDS_SRC ${CMAKE_SOURCE_DIR}/src/cmds/*.cpp)
file(GLOB_RECURSE DATABASE_SRC ${CMAKE_SOURCE_DIR}/src/database/*.cpp)
file(GLOB_RECURSE NET_SRC ${CMAKE_SOURCE_DIR}/src/net/*.cpp)
file(GLOB_RECURSE UTILS_SRC ${CMAKE_SOURCE_DIR}/src/utils/*.cpp)
file(GLOB_RECURSE SERVICE_SRC ${CMAKE_SOURCE_DIR}/src/service/*.cpp)
file(GLOB_RECURSE REPLICA_SRC ${CMAKE_SOURCE_DIR}/src/replica/*.cpp)
file(GLOB_RECURSE ERROR_SRC ${CMAKE_SOURCE_DIR}/src/error/*.cpp)
file(GLOB_RECURSE CLI_SRC ${CMAKE_SOURCE_DIR}/src/cli/interface/*.cpp)
file(GLOB_RECURSE TEST_SRC ${CMAKE_SOURCE_DIR}/src/unittest/*.cpp)
file(GLOB_RECURSE BM_SRC ${CMAKE_SOURCE_DIR}/src/benchmark/*.cpp)

set(LIBS libev.a libinterface.a libprotobuf.a libgtest.a librocksdb.a libglog.a libgflags.a libz.a liblz4.a)
set(CLI_LIBS libev.a libinterface.a libprotobuf.a libglog.a libgflags.a  libz.a liblz4.a)
set(TEST_LIB  libev.a libinterface.a libprotobuf.a libbenchmark_main.a libbenchmark.a libgtest.a librocksdb.a libglog.a libgflags.a  libz.a liblz4.a)

if (${CMAKE_SYSTEM_NAME} STREQUAL Linux)
    set(LINUX_LINKS pthread)
endif()

# chakra
add_executable(chakra src/main.cpp
        ${UTILS_SRC}
        ${CLIENT_SRC}
        ${CLUSTER_SRC}
        ${CMDS_SRC}
        ${DATABASE_SRC}
        ${NET_SRC}
        ${SERVICE_SRC}
        ${REPLICA_SRC}
        ${ERROR_SRC}
        )
add_dependencies(chakra interface ev protobuf rocksdb glog gflags gtest zlib lz4)
target_include_directories(
        chakra SYSTEM PRIVATE
        ${CMAKE_SOURCE_DIR}/build/depends/Install/include
        ${CMAKE_SOURCE_DIR}/src
        )
target_link_directories(chakra PRIVATE
        ${CMAKE_SOURCE_DIR}/build/depends/Install/lib
        ${CMAKE_SOURCE_DIR}/build/depends/Install/lib64
        )
target_link_libraries(chakra ${LIBS} ${LINUX_LINKS})

# cli
add_executable(chakra-cli src/cli/main.cpp
        ${CLIENT_SRC}
        ${CLI_SRC}
        ${NET_SRC}
        ${ERROR_SRC}
        )
add_dependencies(chakra-cli interface ev protobuf rocksdb glog gflags gtest zlib lz4)
target_include_directories(chakra-cli SYSTEM PRIVATE
        ${CMAKE_SOURCE_DIR}/src
        ${CMAKE_SOURCE_DIR}/src/cli/interface
        ${CMAKE_SOURCE_DIR}/build/depends/Install/include
        )
target_link_directories(chakra-cli PRIVATE
        ${CMAKE_SOURCE_DIR}/build/depends/Install/lib
        ${CMAKE_SOURCE_DIR}/build/depends/Install/lib64
        )
target_link_libraries(chakra-cli ${CLI_LIBS} ${LINUX_LINKS})


# ut
add_executable(ut
        ${TEST_SRC}
        ${UTILS_SRC}
        ${CLIENT_SRC}
        ${NET_SRC}
        ${DATABASE_SRC}
        ${ERROR_SRC}
        )
add_dependencies(ut interface ev protobuf rocksdb glog gflags gbenchmark gtest zlib lz4)
target_include_directories(ut SYSTEM PRIVATE
        ${CMAKE_SOURCE_DIR}/src
        ${CMAKE_SOURCE_DIR}/src/unittest
        ${CMAKE_SOURCE_DIR}/build/depends/Install/include
        )
target_link_directories(ut PRIVATE
        ${CMAKE_SOURCE_DIR}/build/depends/Install/lib
        ${CMAKE_SOURCE_DIR}/build/depends/Install/lib64
        )
target_link_libraries(ut ${TEST_LIB} ${LINUX_LINKS})

# ut
add_executable(bm 
        ${BM_SRC}
        ${UTILS_SRC}
        ${CLIENT_SRC}
        ${NET_SRC}
        ${DATABASE_SRC}
        ${ERROR_SRC}
        )
add_dependencies(bm interface ev protobuf rocksdb glog gflags gbenchmark gtest zlib lz4)
target_include_directories(bm SYSTEM PRIVATE
        ${CMAKE_SOURCE_DIR}/src
        ${CMAKE_SOURCE_DIR}/src/unittest
        ${CMAKE_SOURCE_DIR}/build/depends/Install/include
        )
target_link_directories(bm PRIVATE
        ${CMAKE_SOURCE_DIR}/build/depends/Install/lib
        ${CMAKE_SOURCE_DIR}/build/depends/Install/lib64
        )
target_link_libraries(bm ${TEST_LIB} ${LINUX_LINKS})


