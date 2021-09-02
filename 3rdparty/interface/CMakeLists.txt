cmake_minimum_required(VERSION 3.14)

project(interface)

set(CMAKE_CXX_STANDARD 17)

file(GLOB_RECURSE PROTOS
        ${PROJECT_SOURCE_DIR}/protos/*.proto
        )
find_package(Protobuf REQUIRED)
set(ProtoBase ${CMAKE_BINARY_DIR}/gen_protos)

message(STATUS gen_protos:${CMAKE_BINARY_DIR}/gen_protos)

file(MAKE_DIRECTORY ${ProtoBase})

execute_process(
    COMMAND ${PROTOBUF_PROTOC_EXECUTABLE}
            --proto_path=${PROJECT_SOURCE_DIR}/protos
            --cpp_out=${ProtoBase}
            ${GRPCOptions}
            ${PROTOS}
)

include_directories(${ProtoBase} ${PROTOBUF_INCLUDE_DIR})

file(GLOB_RECURSE PROTO_SRCS ${ProtoBase}/*.cc)
add_library(interface STATIC ${PROTO_SRCS})

install(TARGETS interface
        ARCHIVE DESTINATION "lib"
        COMPONENT "interface"
        )

install(DIRECTORY ${ProtoBase}/
        DESTINATION "include"
        FILES_MATCHING PATTERN "*.h"
        )