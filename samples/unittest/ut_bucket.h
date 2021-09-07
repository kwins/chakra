//
// Created by kwins on 2021/9/7.
//

#ifndef CHAKRA_UT_BUCKET_H
#define CHAKRA_UT_BUCKET_H
#include "database/db_bucket.h"

#include <gtest/gtest.h>
#include <glog/logging.h>

TEST(Bucket, test){
    chakra::database::BucketDB::Options options;
    options.dir = ".";
    options.name = "test";
    auto bucket = chakra::database::BucketDB(options);
    LOG(INFO) << "seq:" << bucket.getLastSeqNumber();
}

#endif //CHAKRA_UT_BUCKET_H
