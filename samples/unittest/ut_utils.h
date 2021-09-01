//
// Created by kwins on 2021/6/7.
//

#ifndef CHAKRA_UT_UTILS_H
#define CHAKRA_UT_UTILS_H
#include <gtest/gtest.h>
#include "utils/basic.h"
#include <glog/logging.h>
using namespace chakra::utils;

TEST(Utils, genRandomID){
    LOG(INFO) << Basic::genRandomID();
}
#endif //CHAKRA_UT_UTILS_H
