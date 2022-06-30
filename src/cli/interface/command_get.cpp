#include "command_get.h"
#include <client.pb.h>
#include <element.pb.h>
#include <gflags/gflags_declare.h>

DECLARE_string(get_key);
DECLARE_bool(hashed);

void chakra::cli::CommandGet::execute(std::shared_ptr<chakra::client::ChakraCluster> cluster) {
    if (FLAGS_get_key.empty()) {
        LOG(ERROR) << "get command key empty";
        return;
    }
    auto dbkeys = stringSplit(FLAGS_get_key, ':');
    if (dbkeys.size() != 2) {
        LOG(ERROR) << "get command keys format error: " << FLAGS_get_key;
        return;
    }

    proto::client::GetMessageRequest request;
    request.set_db_name(dbkeys[0]);
    request.set_key(dbkeys[1]);

    LOG(INFO) << "request: " << request.DebugString();
    proto::client::GetMessageResponse response;
    auto err = cluster->get(request, response, FLAGS_hashed);
    if (err) LOG(ERROR) << err.what();
    else LOG(INFO) << "response: " << response.DebugString();
}