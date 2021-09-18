//
// Created by kwins on 2021/6/7.
//

#ifndef CHAKRA_UT_UTILS_H
#define CHAKRA_UT_UTILS_H
#include <gtest/gtest.h>
#include "utils/basic.h"
#include <glog/logging.h>
#include <google/protobuf/util/json_util.h>
#include "peer.pb.h"
using namespace chakra::utils;

TEST(Utils, genRandomID){
    LOG(INFO) << Basic::genRandomID();
    std::string str;
    proto::peer::ClusterState clusterState;
    clusterState.set_state(2);
    clusterState.set_current_epoch(5);

    auto peer = clusterState.mutable_peers()->Add();
    peer->set_name("xxxxxx");
    peer->set_epoch(4);
    peer->set_ip("127.0.0.1");
    peer->set_port(7290);
    auto db = peer->mutable_dbs()->Add();
    db->set_name("test_db");
    db->set_cached(100000);
    db->set_shard(1);
    db->set_shard_size(3);

    google::protobuf::util::JsonOptions options;
    options.always_print_primitive_fields = true;
    options.preserve_proto_field_names = true;
    options.add_whitespace = true;
    options.always_print_enums_as_ints = true;
    google::protobuf::util::MessageToJsonString(clusterState, &str, options);

    LOG(INFO) << str;
}
#endif //CHAKRA_UT_UTILS_H
