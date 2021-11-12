//
// Created by kwins on 2021/9/7.
//

#ifndef CHAKRA_UT_FAMILY_H
#define CHAKRA_UT_FAMILY_H
#include "database/db_family.h"

#include <gtest/gtest.h>
#include <glog/logging.h>

TEST(FamilyDB, test){
    auto& dbptr = chakra::database::FamilyDB::get();
    auto value = dbptr->get("test_db", "key_1");
    if (value){
        value->getObject()->debugString();
    }
    LOG(INFO) << "finished";
}

#endif //CHAKRA_UT_FAMILY_H
