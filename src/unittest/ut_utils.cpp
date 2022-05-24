#ifndef CHAKRA_UT_UTILS_H
#define CHAKRA_UT_UTILS_H
#include <gtest/gtest.h>
#include "utils/basic.h"
#include <glog/logging.h>
#include <google/protobuf/util/json_util.h>
#include "peer.pb.h"
#include "error/err.h"
#include "client.pb.h"

TEST(Utils, error) {
    chakra::error::Error noerr("");
    chakra::error::FileError ferr("not exist");
    chakra::error::Error& rferr = ferr;

    ASSERT_EQ((noerr == true), false);
    ASSERT_EQ((noerr == false), true);
    
    ASSERT_EQ((ferr == true), true);
    ASSERT_EQ((ferr == false), false);

    ASSERT_EQ((rferr == true), true);
    ASSERT_EQ((rferr == false), false);
}

TEST(Utils, protobuf) {
    proto::client::MGetMessageResponse response;
    proto::client::MGetMessageResponse subRes1;
    auto subRes1E1 = (*subRes1.mutable_datas())["sub1"].add_value();
    subRes1E1->set_key("e1");
    subRes1E1->mutable_ss()->add_value("v1");
    subRes1E1->mutable_ss()->add_value("v2");

    auto subRes1E2 = (*subRes1.mutable_datas())["sub2"].add_value();
    subRes1E2->set_key("e2");
    subRes1E2->mutable_ss()->add_value("v3");
    subRes1E2->mutable_ss()->add_value("v4");

    proto::client::MGetMessageResponse subRes2;
    subRes2.mutable_error()->set_errcode(2);
    subRes2.mutable_error()->set_errmsg("error_test");
    auto subRes2E1 = (*subRes2.mutable_datas())["sub3"].add_value();
    subRes2E1->set_key("e3");
    subRes2E1->mutable_ss()->add_value("v5");
    subRes2E1->mutable_ss()->add_value("v6");

    auto subRes2E2 = (*subRes2.mutable_datas())["sub4"].add_value();
    subRes2E2->set_key("e4");
    subRes2E2->mutable_ss()->add_value("v7");
    subRes2E2->mutable_ss()->add_value("v8");

    response.MergeFrom(subRes1);
    response.MergeFrom(subRes2);
    ASSERT_EQ(response.datas().size(), 4);
    ASSERT_EQ(response.datas().at("sub1").value(0).key(), "e1");
    ASSERT_EQ(response.datas().at("sub2").value(0).key(), "e2");
    ASSERT_EQ(response.datas().at("sub3").value(0).key(), "e3");
    ASSERT_EQ(response.datas().at("sub4").value(0).key(), "e4");
}
#endif //CHAKRA_UT_UTILS_H