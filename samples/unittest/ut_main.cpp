//
// Created by kwins on 2021/5/14.
//

#include <gtest/gtest.h>
#include <gflags/gflags.h>

//#include "ut_client.h"
//#include "ut_utils.h"
//#include "ut_lru.h"
#include "ut_rocksdb.h"

int main(int argc, char* argv[]){
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}