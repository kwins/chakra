#include "command_client_incr.h"
#include "client.pb.h"
#include "database/db_family.h"
#include "net/packet.h"
#include <chrono>
#include <cluster/cluster.h>
#include <cstddef>
#include <element.pb.h>
#include <exception>
#include <rocksdb/options.h>
#include <service/chakra.h>
#include <types.pb.h>

void chakra::cmds::CommandClientIncr::execute(char *req, size_t len, void *data, std::function<error::Error (char *, size_t)> cbf) {
    proto::client::IncrMessageRequest incrMessageRequest;
    proto::client::IncrMessageResponse incrMessageResponse;
    auto dbptr = chakra::database::FamilyDB::get();
    auto err = chakra::net::Packet::deSerialize(req, len, incrMessageRequest, proto::types::C_INCR);
    if (err) {
        chakra::net::Packet::fillError(incrMessageResponse.mutable_error(), 1, err.what());
    } else if (!dbptr->servedDB(incrMessageRequest.db_name())) {
        chakra::net::Packet::fillError(incrMessageResponse.mutable_error(), 1, "db " + incrMessageRequest.db_name() + " not exist.");
    } else if (incrMessageRequest.key().empty()) {
        chakra::net::Packet::fillError(incrMessageResponse.mutable_error(), 1, "key empty");
    } else if (incrMessageRequest.type() != proto::element::ElementType::FLOAT) {
        chakra::net::Packet::fillError(incrMessageResponse.mutable_error(), 1, "type must be float");
    } else {
        auto err = dbptr->incr(incrMessageRequest.db_name(), incrMessageRequest.key(), incrMessageRequest.f());
        if (err) {
            chakra::net::Packet::fillError(incrMessageResponse.mutable_error(), 1, err.what());
        }
    }
    chakra::net::Packet::serialize(incrMessageResponse, proto::types::C_INCR, cbf);
}