#include "command_get.h"
#include <client.pb.h>
#include <element.pb.h>

DECLARE_string(get_key);

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

    proto::client::GetMessageRequest getMessageRequest;
    getMessageRequest.set_db_name(dbkeys[0]);
    getMessageRequest.set_key(dbkeys[1]);

    proto::client::GetMessageResponse getMessageResponse;
    auto err = cluster->get(getMessageRequest, getMessageResponse);
    if (err) LOG(ERROR) << err.what();
    else LOG(INFO) << getMessageResponse.DebugString();
}