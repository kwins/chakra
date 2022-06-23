#include "command_setdb.h"

DECLARE_string(setdb_name); // db name
DECLARE_string(setdb_nodename); // node name
DECLARE_int64(setdb_cached); // db cached

void chakra::cli::CommandSetDB::execute(std::shared_ptr<chakra::client::ChakraCluster> cluster) {
    if (FLAGS_setdb_nodename.empty() || FLAGS_setdb_name.empty()) {
        LOG(ERROR) << "set db bad nodename or dbname";
        return;
    }
    auto err = cluster->setdb(FLAGS_setdb_nodename, FLAGS_setdb_name, FLAGS_setdb_cached);
    if (err) LOG(ERROR) << err.what();
    else LOG(INFO) << "setdb " << FLAGS_setdb_name << " to " << FLAGS_setdb_nodename << " success";
}