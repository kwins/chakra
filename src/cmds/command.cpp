#include "command.h"


void chakra::cmds::Command::fillError(proto::types::Error& error, int32_t code, const std::string& errmsg) {
    error.set_errcode(code);
    error.set_errmsg(errmsg);
}

void chakra::cmds::Command::fillError(proto::types::Error* error, int32_t code, const std::string& errmsg) {
    error->set_errcode(code);
    error->set_errmsg(errmsg);
}