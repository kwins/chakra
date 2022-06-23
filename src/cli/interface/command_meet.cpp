#include "command_meet.h"
#include <string>

DECLARE_string(meet_ip);
DECLARE_int32(meet_port);

void chakra::cli::CommandMeet::execute(std::shared_ptr<chakra::client::ChakraCluster> cluster) {
    auto err = cluster->meet(FLAGS_meet_ip, FLAGS_meet_port);
    if (err) LOG(ERROR) << err.what();
    else LOG(INFO) << "meet (" << FLAGS_meet_ip << ":" << std::to_string(FLAGS_meet_port) + ") success";
}