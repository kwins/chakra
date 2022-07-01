#ifndef CHAKRA_CLI_FLAGS_CPP
#define CHAKRA_CLI_FLAGS_CPP

#include <glog/logging.h>
#include <gflags/gflags.h>

DEFINE_string(command, "", "command");
static bool validCliCommand(const char* flagname, const std::string& value){
    if (value.empty()) {
        return false;
    }
    return true;
}
DEFINE_validator(command, validCliCommand);   

DEFINE_string(ip, "", "ip");
static bool validCliIP(const char* flagname, const std::string& value){
    if (value.empty()) {
        return false;
    }
    return true;
}
DEFINE_validator(ip, validCliIP); 

DEFINE_int32(port, 7290, "port");
static bool validCliPort(const char* flagname, int32_t value){
    if (value == 0) {
        return false;
    }
    return true;
}
DEFINE_validator(port, validCliPort); 

DEFINE_int32(max_idle_conns, 3, "max idle connect num");
DEFINE_int32(max_conns, 10, "max connect num");
DEFINE_int32(connect_timeout_ms, 50, "connect timeout ms");
DEFINE_int32(connect_read_timeout_ms, 100, "connect read timeout ms");
DEFINE_int32(connect_write_timeout_ms, 100, "connect write timeout ms");


// meet command
DEFINE_string(meet_ip, "", "meet ip");
DEFINE_int32(meet_port, 7290, "meet port");

// set command
DEFINE_string(set_keys, "", "set keys");
DEFINE_string(set_values, "", "set values");
DEFINE_int64(set_ttl, 0, "set ttl ms"); // ms

// setdb command
DEFINE_string(setdb_name, "", "setdb command db name"); // db name
DEFINE_string(setdb_nodename, "", "setdb command node name"); // node name
DEFINE_int64(setdb_cached, 0, "setdb command db cache size"); // db cached

// get command
DEFINE_bool(hashed, false, "get and mget key use hash");
DEFINE_string(get_key, "", "get command key name"); // key name

// mget command
DEFINE_string(mget_keys, "", "mget command keys");
DEFINE_int64(mget_split_n, 10, "mget command split num");

// push command
DEFINE_string(push_key, "", "push command key");
DEFINE_string(push_values, "", "push command values");
DEFINE_int64(push_ttl, 0, "push command ttl"); // ms

// state command
DEFINE_bool(state_now, false, "state command that get cluster now state");
#endif // CHAKRA_CLI_FLAGS_CPP