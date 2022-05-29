#include "command_get.h"
#include <client.pb.h>
#include <element.pb.h>

DECLARE_string(get_db);
DECLARE_string(get_key);

void chakra::cli::CommandGet::execute(std::shared_ptr<chakra::client::ChakraCluster> cluster) {
    if (FLAGS_get_db.empty() || FLAGS_get_key.empty()) {
        LOG(ERROR) << "db name empty or key empty";
        return;
    }

    proto::client::GetMessageRequest getMessageRequest;
    getMessageRequest.set_db_name(FLAGS_get_db);
    getMessageRequest.set_key(FLAGS_get_key);

    proto::client::GetMessageResponse getMessageResponse;
    auto err = cluster->get(getMessageRequest, getMessageResponse);
    if (err) LOG(ERROR) << err.what();
    else LOG(INFO) << getMessageResponse.DebugString();
}