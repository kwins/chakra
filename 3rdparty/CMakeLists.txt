set (OpenSSLConfigureCommand config)
if (APPLE)
    set (OpenSSLConfigureCommand Configure darwin64-x86_64-cc enable-ec_nistp_64_gcc_128)
endif ()

ExternalProject_Add (
        openssl
        URL ${PROJECT_SOURCE_DIR}/3rdparty/openssl-1.1.1g.tar.gz
        CONFIGURE_COMMAND ./${OpenSSLConfigureCommand} no-zlib no-shared enable-cms --openssldir=${EPBase}/Install --prefix=${EPBase}/Install
        BUILD_COMMAND ${Make}
        BUILD_IN_SOURCE 1
        INSTALL_COMMAND make install_sw
)

ExternalProject_Add (
        zlib
        URL ${PROJECT_SOURCE_DIR}/3rdparty/zlib-1.2.11.tar.gz
        CONFIGURE_COMMAND cmake ${CMakeConfig} -Wno-dev .
        BUILD_IN_SOURCE 1
)

ExternalProject_Add (
        gtest
        URL ${PROJECT_SOURCE_DIR}/3rdparty/googletest-release-1.8.0.tar.gz
        CONFIGURE_COMMAND cmake -DCMAKE_INSTALL_PREFIX=${EPBase}/Install .
        BUILD_COMMAND make
        BUILD_IN_SOURCE 1
        INSTALL_COMMAND make install
)

ExternalProject_Add (
        glog
        URL ${PROJECT_SOURCE_DIR}/3rdparty/glog-0.4.0.tar.gz
        CONFIGURE_COMMAND cmake ${CMakeConfig} .
        BUILD_COMMAND ${Make}
        BUILD_IN_SOURCE 1
        INSTALL_COMMAND make install
)

ExternalProject_Add (
        gflags
        URL ${PROJECT_SOURCE_DIR}/3rdparty/gflags.tar.gz
        CONFIGURE_COMMAND cmake ${CMakeConfig} .
        BUILD_COMMAND ${Make}
        BUILD_IN_SOURCE 1
        INSTALL_COMMAND make install
)

ExternalProject_Add (
        protobuf
        DEPENDS zlib
        URL ${PROJECT_SOURCE_DIR}/3rdparty/protobuf-3.6.0.zip
        PATCH_COMMAND ${MakeENV} && ./autogen.sh
        CONFIGURE_COMMAND ${MakeENV}  && ./configure --disable-shared --enable-static --with-zlib-include=${EPBase}/Install/include --with-zlib-lib=${EPBase}/Install/lib --prefix=${EPBase}/Install
        BUILD_COMMAND ${Make}
        BUILD_IN_SOURCE 1
)

ExternalProject_Add (
        lz4
        URL ${PROJECT_SOURCE_DIR}/3rdparty/lz4-1.8.3.zip
        CONFIGURE_COMMAND ""
        BUILD_COMMAND ${Make} lib
        BUILD_IN_SOURCE 1
        INSTALL_COMMAND ${Make} install PREFIX=${EPBase}/Install
)

ExternalProject_Add (
        rocksdb
        DEPENDS lz4
        URL ${PROJECT_SOURCE_DIR}/3rdparty/rocksdb-6.20.3.tar.gz
        CONFIGURE_COMMAND cmake ${CMakeConfig} . -DWITH_TESTS=OFF -DWITH_TOOLS=OFF -DWITH_GFLAGS=OFF -DWITH_LZ4=ON -DCMAKE_INSTALL_LIBDIR=lib -DCMAKE_BUILD_TYPE=Release -DWITH_BENCHMARK_TOOLS=OFF -DUSE_RTTI=ON
        BUILD_IN_SOURCE 1
        INSTALL_COMMAND ${Make} install PREFIX=${EPBase}/Install
)

ExternalProject_Add (
        ev
        URL ${PROJECT_SOURCE_DIR}/3rdparty/libev-4.33.tar.gz
        PATCH_COMMAND ${MakeENV} && bash autogen.sh
        CONFIGURE_COMMAND  ${MakeENV}  && ./configure -prefix=${EPBase}/Install
        BUILD_COMMAND ${Make}
        BUILD_IN_SOURCE 1
        INSTALL_COMMAND make install
)

ExternalProject_Add (
        interface
        DEPENDS protobuf
        URL ${PROJECT_SOURCE_DIR}/3rdparty/interface
        CONFIGURE_COMMAND cmake ${CMakeConfig} .
        BUILD_COMMAND ${Make}
        BUILD_IN_SOURCE 1
)
