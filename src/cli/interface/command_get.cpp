#include "command_get.h"
#include <element.pb.h>

DECLARE_string(get_db);
DECLARE_string(get_key);

void chakra::cli::CommandGet::execute(std::shared_ptr<chakra::client::ChakraCluster> cluster) {
    if (FLAGS_get_db.empty() || FLAGS_get_key.empty()) {
        LOG(ERROR) << "db name empty or key empty";
        return;
    }

    proto::element::Element element;
    auto err = cluster->get(FLAGS_get_db, FLAGS_get_key, element);
    if (err) LOG(ERROR) << err.what();
    else LOG(INFO) << element.DebugString();
}