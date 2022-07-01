#include "command_mget.h"
#include <cassert>
#include <element.pb.h>
#include <gflags/gflags_declare.h>
#include <gtest/gtest.h>
#include <vector>

DECLARE_string(mget_keys);
DECLARE_int64(mget_split_n);
DECLARE_bool(hashed);

void chakra::cli::CommandMGet::execute(std::shared_ptr<chakra::client::ChakraCluster> cluster) {
    if (FLAGS_mget_keys.empty()) {
        LOG(ERROR) << "mget command keys empty";
        return;
    }

    proto::client::MGetMessageRequest request;
    auto dbkeys = stringSplit(FLAGS_mget_keys, ',');
    for(auto& dbk : dbkeys) {
        auto v = stringSplit(dbk, ':');
        if (v.size() != 2) {
            LOG(ERROR) << "mget command keys format error: " << FLAGS_mget_keys;
            return;
        }
        auto& dbname = v[0];
        auto& key = v[1];
        (*request.mutable_keys())[dbname].add_value(key);
    }

    LOG(INFO) << "request: " << request.DebugString();
    proto::client::MGetMessageResponse response;
    auto err = cluster->mget(request, response, FLAGS_mget_split_n, FLAGS_hashed);
    if (err) LOG(ERROR) << err.what();
    else LOG(INFO) << "response: " << response.DebugString();
}