//
// Created by kwins on 2021/5/21.
//

#include "packet.h"

void chakra::net::Packet::serialize(const ::google::protobuf::Message& msg, proto::types::Type type, Buffer* buffer) {
    size_t bodysize = msg.ByteSizeLong();
    uint64_t packsize = headLength() + bodysize;
    buffer->maybeRealloc(packsize);

    auto apl = append<uint64_t>(buffer->data, buffer->len, packsize);
    buffer->len += apl;
    buffer->free -= apl;

    apl = append<uint32_t>(buffer->data, buffer->len, (uint32_t)0);
    buffer->len += apl;
    buffer->free -= apl;

    apl = append<uint32_t>(buffer->data, buffer->len, (uint32_t)type);
    buffer->len += apl;
    buffer->free -= apl;

    msg.SerializeToArray(&buffer->data[buffer->len], bodysize);
    buffer->len += bodysize;
    buffer->free -= bodysize;
}

chakra::error::Error chakra::net::Packet::deSerialize(char *src, size_t sl, google::protobuf::Message &msg, proto::types::Type msgtype) {
    auto packsize = read<uint64_t>(src, sl, 0);
    if (packsize < headLength()){
        return error::Error("packet head size too small");
    }

    proto::types::Type reqtype = chakra::net::Packet::type(src, sl);
    if (reqtype != msgtype) {
        return  error::Error("packet type not match (data type: " + std::to_string(reqtype) + ", need type: " + std::to_string(msgtype) + ")");
    }

    uint64_t bodyLen = packsize - headLength();
    auto ok = msg.ParseFromArray(&src[BODY_IDX], bodyLen);
    if (!ok){
        return error::Error("message parse error");
    }
    return error::Error();
}
size_t chakra::net::Packet::headLength() { return HEAD_LEN + FLAG_LEN + TYPE_LEN; }
uint64_t chakra::net::Packet::size(char *src, size_t sl) { return read<uint64_t>(src, sl, HEAD_IDX); }
uint32_t chakra::net::Packet::flag(char *src, size_t sl) { return read<uint32_t>(src, sl, FLAG_IDX); }
proto::types::Type chakra::net::Packet::type(char *src, size_t sl) { return (proto::types::Type) read<uint32_t>(src, sl, TYPE_IDX); }
