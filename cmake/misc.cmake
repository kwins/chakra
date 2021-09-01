cmake_minimum_required(VERSION 3.15)

# 当已经定义了EPBase变量时, 不再重复定义.
# 在现有编译过程中, 本文件可能会被重复地include多次
# 这有可能导致cpp-base会被编译两次, 下面的判断可以避免
# 这种情况
message(STATUS CMAKE_BINARY_DIR:${CMAKE_BINARY_DIR})
if (NOT EPBase)
    set(EPBase ${CMAKE_BINARY_DIR}/depends)
endif()

file(MAKE_DIRECTORY ${EPBase}/Install/include)
file(MAKE_DIRECTORY ${EPBase}/Install/lib)

message(STATUS ${CMAKE_C_COMPILER} -- ${CMAKE_CXX_COMPILER})
set(CMakeConfig
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DCMAKE_PREFIX_PATH=${EPBase}/Install
        -DCMAKE_CXX_FLAGS=-fPIC
        -DCMAKE_INSTALL_PREFIX=${EPBase}/Install .
        )
set(MakeENV
        export CFLAGS=-fPIC ${CMAKE_C_FLAGS} &&
        export CXXFLAGS=-fPIC ${CMAKE_CXX_FLAGS} &&
        export PATH=${EPBase}/Install/bin:$ENV{PATH} &&
        export PKG_CONFIG_PATH=${EPBase}/Install/lib/pkgconfig:${EPBase}/Install/share/pkgconfig
        )
set(Make
        ${MakeENV} && export CC=${CMAKE_C_COMPILER} && export CXX=${CMAKE_CXX_COMPILER} &&
        $(MAKE)
        )
set(Make_GCC
        ${MakeENV} && export CC=gcc && export CXX=g++ &&
        export PKG_CONFIG_PATH=${EPBase}/Install/lib/pkgconfig:${EPBase}/Install/share/pkgconfig &&
        $(MAKE)
        )

function(prepend var prefix)
    set(ret "")
    foreach(f ${ARGN})
        list(APPEND ret "${prefix}${f}")
    endforeach(f)
    set(${var} "${ret}" PARENT_SCOPE)
endfunction(prepend)
