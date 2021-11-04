#!/bin/bash
cd build
cmake -DCMAKE_C_COMPILER=/usr/bin/clang -DCMAKE_CXX_COMPILER=/usr/bin/clang++ ..
make -j8
cp chakra ..
cp ut ..