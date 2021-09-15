//
// Created by kwins on 2021/9/7.
//

#ifndef CHAKRA_UT_FAMILY_H
#define CHAKRA_UT_FAMILY_H
#include "database/db_family.h"

#include <gtest/gtest.h>
#include <glog/logging.h>

TEST(FamilyDB, test){
    chakra::database::FamilyDB::Options options;
    options.dir = "./test/node1";
    LOG(INFO) << "1";
    chakra::database::FamilyDB::initFamilyDB(options);
    LOG(INFO) << "2";
    auto dbptr = chakra::database::FamilyDB::get();
    LOG(INFO) << "3:" << dbptr.get();
    auto value = dbptr->get("test_db", "key_1");
    LOG(INFO) << "4";
    if (value){
        value->getObject()->debugString();
    }
    LOG(INFO) << "5";

//    chakra::database::FamilyDB db(options);
//    auto value = db.get("test_db", "key_1");
//    if (value){
//        value->getObject()->debugString();
//    }
}

#endif //CHAKRA_UT_FAMILY_H
