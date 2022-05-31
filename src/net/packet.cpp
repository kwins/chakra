//
// Created by kwins on 2021/5/21.
//

#include "packet.h"
#include <glog/logging.h>
chakra::error::Error chakra::net::Packet::serialize(const google::protobuf::Message &msg, proto::types::Type type, const std::function<error::Error(char*, size_t)>& cbf) {
    size_t bodySize = msg.ByteSizeLong();
    uint64_t packSize = HEAD_LEN + FLAG_LEN + TYPE_LEN + bodySize;
    char reply[packSize];
    append<uint64_t>(reply, HEAD_IDX, packSize);
    append<uint32_t>(reply, FLAG_IDX, (uint32_t)0);
    append<uint32_t>(reply, TYPE_IDX, (uint32_t)type);
    msg.SerializeToArray(&reply[BODY_IDX], bodySize);
    return cbf(reply, packSize);
}

chakra::error::Error chakra::net::Packet::deSerialize(char *src, size_t srcLen, google::protobuf::Message &msg, proto::types::Type type) {
    proto::types::Type reqtype;
    if ((reqtype = chakra::net::Packet::getType(src, srcLen)) != type) {
        return  error::Error("pack type not match(data type: " + std::to_string(reqtype) + ", need type: " + std::to_string(type) + ")");
    }

    auto packSize = read<uint64_t>(src, srcLen, 0);
    if (packSize < (HEAD_LEN + FLAG_LEN + TYPE_LEN)){
        return error::Error("pack head size too small");
    }

    uint64_t bodyLen = packSize - HEAD_LEN - FLAG_LEN - TYPE_LEN;
    auto ok = msg.ParseFromArray(&src[BODY_IDX], bodyLen);
    if (!ok){
        return error::Error("message parse error");
    }
    return error::Error();
}


void chakra::net::Packet::fillError(proto::types::Error &error, int32_t code, const std::string &errmsg) {
    error.set_errcode(code);
    error.set_errmsg(errmsg);
}

void chakra::net::Packet::fillError(proto::types::Error *error, int32_t code, const std::string &errmsg) {
    error->set_errcode(code);
    error->set_errmsg(errmsg);
}

uint64_t chakra::net::Packet::getSize(char *src, size_t len) { return read<uint64_t>(src, len, HEAD_IDX); }

uint32_t chakra::net::Packet::getFlag(char *src, size_t len) { return read<uint32_t>(src, len, FLAG_IDX); }

proto::types::Type chakra::net::Packet::getType(char *src, size_t len) { return (proto::types::Type) read<uint32_t>(src, len, TYPE_IDX); }
