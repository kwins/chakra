//
// Created by kwins on 2021/9/7.
//

#ifndef CHAKRA_UT_BUCKET_H
#define CHAKRA_UT_BUCKET_H
#include <gtest/gtest.h>
#include <glog/logging.h>

#include "database/db_bucket.h"

class BucketTest : public ::testing::Test{
public:
protected:
    void SetUp() override {
        chakra::database::BucketDB::Options options;
        options.dir = "./test/node1";
        options.name = "test_db";
        options.blocktSize = 256;
        options.blockCapaticy = 100000;
        bucket = new chakra::database::BucketDB(options);
    }

    void TearDown() override {
        delete bucket;
    }

    chakra::database::BucketDB* bucket;
};

TEST_F(BucketTest, test){
    auto value = bucket->get("key_1");
    if (value){
        value->getObject()->debugString();
    }
}

#endif //CHAKRA_UT_BUCKET_H
