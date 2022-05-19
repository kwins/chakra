#include <gtest/gtest.h>
#include <gflags/gflags.h>

#include "ut_utils.h"
#include "ut_cache.h"

int main(int argc, char* argv[]){
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}