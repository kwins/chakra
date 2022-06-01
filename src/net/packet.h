//
// Created by kwins on 2021/5/21.
//

#ifndef CHAKRA_PACKET_H
#define CHAKRA_PACKET_H
#include <sys/types.h>
#include <google/protobuf/message.h>
#include "types.pb.h"
#include <functional>
#include "error/err.h"
#include "buffer.h"

namespace chakra::net{

class Packet {
public:
    static error::Error serialize(const ::google::protobuf::Message& msg, proto::types::Type type,
                          const std::function<error::Error(char* reply, size_t len)>& cbf);
    static void serialize(const ::google::protobuf::Message& msg, proto::types::Type type, Buffer* buffer);

    static error::Error deSerialize(char* src, size_t srcLen, ::google::protobuf::Message& msg, proto::types::Type type);

    static uint64_t getSize(char* src, size_t len);
    static uint32_t getFlag(char* src, size_t len);
    static proto::types::Type getType(char* src, size_t len);

    static void fillError(proto::types::Error& error, int32_t code, const std::string& errmsg);
    static void fillError(proto::types::Error* error, int32_t code, const std::string& errmsg);

    template<typename T>
    static T read(char* src, size_t len, int index){
        if (index + sizeof(T) <= len){
            return *((T*)&src[index]);
        }
        return 0;
    }

    // 针对数值 append
    template<typename T>
    static void append(char* dest, size_t pos, T data){
        uint32_t dataLen = sizeof(data);
        memcpy(&dest[pos], (char*)&data, dataLen);
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
