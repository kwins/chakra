#include "command_state.h"
#include <gflags/gflags_declare.h>
#include <peer.pb.h>

DECLARE_bool(state_now);

void chakra::cli::CommandState::execute(std::shared_ptr<chakra::client::ChakraCluster> cluster) {
    proto::peer::ClusterState clusterState;
    auto err = cluster->state(clusterState);
    if (err) LOG(ERROR) << err.what();
    LOG(INFO) << "cluster state: " << clusterState.DebugString();
}