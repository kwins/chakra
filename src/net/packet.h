//
// Created by kwins on 2021/5/21.
//

#ifndef CHAKRA_PACKET_H
#define CHAKRA_PACKET_H
#include <sys/types.h>
#include <google/protobuf/message.h>
#include "types.pb.h"
#include "error/err.h"
#include "buffer.h"

namespace chakra::net {

class Packet {
public:
    static void serialize(const ::google::protobuf::Message& msg, proto::types::Type msgtype, Buffer* buffer);
    static error::Error deSerialize(char* src, size_t sl, ::google::protobuf::Message& msg, proto::types::Type msgtype);
    static uint64_t size(char* src, size_t l);
    static uint32_t flag(char* src, size_t l);
    static proto::types::Type type(char* src, size_t l);
    static size_t headLength();

private:
    template<typename T>
    static T read(char* src, size_t len, int index){
        if (index + sizeof(T) <= len){
            return *((T*)&src[index]);
        }
        return 0;
    }

    // 针对数值 append
    template<typename T>
    static uint32_t append(char* dest, size_t pos, T data){
        uint32_t dl = sizeof(data);
        memcpy(&dest[pos], (char*)&data, dl);
        return dl;
    }

    static const size_t HEAD_LEN = 8; // 字节
    static const size_t FLAG_LEN = 4;
    static const size_t TYPE_LEN = 4;

    static const size_t HEAD_IDX = 0;
    static const size_t FLAG_IDX = 8;
    static const size_t TYPE_IDX = 12;
    static const size_t BODY_IDX = 16;
};

}



#endif //CHAKRA_PACKET_H
