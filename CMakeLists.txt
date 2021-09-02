cmake_minimum_required(VERSION 3.14)
include(ExternalProject)
include(cmake/misc.cmake)

project(chakra)

add_compile_options(-g -std=c++17)

file(GLOB_RECURSE CLIENT_SRC ${CMAKE_SOURCE_DIR}/src/client/*.cpp)
file(GLOB_RECURSE CLUSTER_SRC ${CMAKE_SOURCE_DIR}/src/cluster/*.cpp)
file(GLOB_RECURSE CMDS_SRC ${CMAKE_SOURCE_DIR}/src/cmds/*.cpp)
file(GLOB_RECURSE DATABASE_SRC ${CMAKE_SOURCE_DIR}/src/database/*.cpp)
file(GLOB_RECURSE NET_SRC ${CMAKE_SOURCE_DIR}/src/net/*.cpp)
file(GLOB_RECURSE UTILS_SRC ${CMAKE_SOURCE_DIR}/src/utils/*.cpp)
file(GLOB_RECURSE SERVICE_SRC ${CMAKE_SOURCE_DIR}/src/service/*.cpp)
file(GLOB_RECURSE REPLICA_SRC ${CMAKE_SOURCE_DIR}/src/replica/*.cpp)

add_subdirectory(${CMAKE_SOURCE_DIR}/3rdparty EXCLUDE_FROM_ALL)

set(LIBS libev.a libprotobuf.a libgtest.a librocksdb.a libglog.a libgflags.a libinterface.a libz.a liblz4.a)
set(TEST_LIB librocksdb.a libglog.a libgflags.a libgtest.a  libz.a liblz4.a)

if (${CMAKE_SYSTEM_NAME} STREQUAL Linux)
    set(LINUX_LINKS pthread rt atomic gomp m gfortran profiler dl)
endif()

# chakra
add_executable(chakra src/main.cpp
        ${CLIENT_SRC}
        ${CLUSTER_SRC}
        ${CMDS_SRC}
        ${DATABASE_SRC}
        ${NET_SRC}
        ${UTILS_SRC}
        ${SERVICE_SRC}
        ${REPLICA_SRC}
        )
target_include_directories(
        chakra SYSTEM PRIVATE
        ${CMAKE_SOURCE_DIR}/build/depends/Install/include
        ${CMAKE_SOURCE_DIR}/src
        )
add_dependencies(chakra ev protobuf rocksdb glog gflags gtest interface zlib lz4)
target_link_directories(chakra PRIVATE
        ${CMAKE_SOURCE_DIR}/build/depends/Install/lib)
target_link_libraries(chakra ${LIBS} ${LINUX_LINKS})


# ut
add_executable(ut samples/unittest/ut_main.cpp
        ${UTILS_SRC})
set_target_properties(ut PROPERTIES
        INSTALL_RPATH ${CMAKE_SOURCE_DIR}/build/depends/Install/lib)
target_include_directories(ut SYSTEM PRIVATE
        ${CMAKE_SOURCE_DIR}/src
        ${CMAKE_SOURCE_DIR}/samples/unittest
        ${CMAKE_SOURCE_DIR}/build/depends/Install/include
)
add_dependencies(ut rocksdb glog gflags gtest zlib lz4)
target_link_directories(ut PRIVATE
        ${CMAKE_SOURCE_DIR}/build/depends/Install/lib)
target_link_libraries(ut ${TEST_LIB} ${LINUX_LINKS})

