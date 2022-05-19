#include "cli.h"
#include <cctype>
#include <command.h>
#include <command_get.h>
#include <command_meet.h>
#include <command_set.h>
#include <command_setdb.h>
#include <cstdint>
#include <cstdlib>
#include <error/err.h>
#include <gflags/gflags_declare.h>
#include <memory>
#include <utility>

DECLARE_string(command);
DECLARE_string(ip);
DECLARE_int32(port);
DECLARE_int32(max_idle_conns);
DECLARE_int32(max_conns);
DECLARE_int32(connect_timeout_ms);
DECLARE_int32(connect_read_timeout_ms);
DECLARE_int32(connect_write_timeout_ms);

chakra::cli::Cli::Cli() {
    try {
        chakra::client::ChakraCluster::Options options;
        options.ip = FLAGS_ip;
        options.port = FLAGS_port;
        options.maxIdleConns = FLAGS_max_idle_conns;
        options.maxConns = FLAGS_max_conns;
        options.connectTimeOut = std::chrono::milliseconds(FLAGS_connect_timeout_ms);
        options.readTimeOut = std::chrono::milliseconds(FLAGS_connect_read_timeout_ms);
        options.writeTimeOut = std::chrono::milliseconds(FLAGS_connect_write_timeout_ms);
        cluster = std::make_shared<chakra::client::ChakraCluster>(options);
    } catch (const std::exception& err) {
        LOG(ERROR) <<"Cli exception:" << err.what();
        exit(-1);
    }
}

void chakra::cli::Cli::execute() {
    auto it = cmds.find(FLAGS_command);
    if (it == cmds.end()) {
        LOG(ERROR) << "unsupport command:" + FLAGS_command;
        return;
    }
    it->second->execute(cluster);
}

std::unordered_map<std::string, std::shared_ptr<chakra::cli::Command>> chakra::cli::Cli::cmds = {
    {"meet" , std::make_shared<chakra::cli::CommandMeet>()},
    {"setdb", std::make_shared<chakra::cli::CommandSetDB>()},
    {"set",   std::make_shared<chakra::cli::CommandSet>()},
    {"get",   std::make_shared<chakra::cli::CommandGet>()},
};
