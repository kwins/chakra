#ifndef CHAKRA_CLI_FLAGS_CPP
#define CHAKRA_CLI_FLAGS_CPP

#include <glog/logging.h>
#include <gflags/gflags.h>

DEFINE_string(command, "", "command");
static bool validCliCommand(const char* flagname, const std::string& value){
    if (value.empty()) {
        return false;
    }
    if (value != "meet" && value != "setdb") {
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
DEFINE_int32(max_conns, 3, "max connect num");
DEFINE_int32(connect_timeout_ms, 50, "connect timeout ms");
DEFINE_int32(connect_read_timeout_ms, 100, "connect read timeout ms");
DEFINE_int32(connect_write_timeout_ms, 100, "connect write timeout ms");


// meet command
DEFINE_string(meet_ip, "", "meet ip");
DEFINE_int32(meet_port, 7290, "meet port");

#endif // CHAKRA_CLI_FLAGS_CPP