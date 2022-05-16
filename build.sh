#!/bin/bash
show_help () {
    echo "$0 [-abth]"
    echo "-h show help"
    echo "-b build chakra(default)"
    echo "-t build UT and run UT"
    echo "-a build chakra and run UT"
}
init() {
	mkdir -p build
}
build() {
    COMPILE_ENV=8
    cmake -DCMAKE_EXPORT_COMPILE_COMMANDS=YES -DCMAKE_C_COMPILER=/usr/bin/clang -DCMAKE_CXX_COMPILER=/usr/bin/clang++ .. ${CMAKE_EXTRA_OPTION} \
    && make -j${COMPILE_ENV} \
    && cp chakra .. \
    && cp chakra-cli .. \
    && cp ut ..
}
build_test () {
    cmake -DCMAKE_C_COMPILER=/usr/bin/clang -DCMAKE_CXX_COMPILER=/usr/bin/clang++ .. ${CMAKE_EXTRA_OPTION} \
    && make -j${COMPILE_ENV} ut \
    && cp ut .. 
}

run () {
    source /etc/profile && cd "$(dirname $(readlink -f $0))"
    COMPILE_ENV=8
    init
    cd build && build && cd ..
}

fail_exit () {
	if [ $? -ne 0 ]; then
		echo command failed and exit
		exit 1
	fi
}
run_test () {
    source /etc/profile && cd "$(dirname $(readlink -f $0))"
    COMPILE_ENV=8
    init
    cd build && build_test && cd ..
}

run_all () {
    run && run_test
}


while getopts "abht" opt; do
  case "${opt}" in
    h)
      show_help
      ;;
    a)
      run_all
      ;;
    t)
      run_test
      ;;
    b)
      run
      ;;
    *)
      show_help
      ;;
  esac
done

if [ $# -eq 0 ]; then
  run
fi