#include "cli.h"

DECLARE_string(command);
DECLARE_string(ip);
DECLARE_int32(port);
DECLARE_int32(max_idle_conns);
DECLARE_int32(max_conns);
DECLARE_int32(connect_timeout_ms);
DECLARE_int32(connect_read_timeout_ms);
DECLARE_int32(connect_write_timeout_ms);

DECLARE_string(meet_ip);
DECLARE_int32(meet_port);

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
        LOG(ERROR) << err.what();
        exit(-1);
    }
}

void chakra::cli::Cli::execute() {
    error::Error err;
    if (FLAGS_command == "meet") {
        err = cluster->meet(FLAGS_meet_ip, FLAGS_meet_port);
    } else if (FLAGS_command == "setdb") {
        err = error::Error("not implement");
    } else {
        err = error::Error("unsupport command:" + FLAGS_command);
    }
    if (err) LOG(ERROR) << err.what();
}