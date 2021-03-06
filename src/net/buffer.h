#ifndef CHAKRA_NET_BUFFER_H
#define CHAKRA_NET_BUFFER_H

#include <cstddef>
#include <google/protobuf/message.h>
#include <types.pb.h>

namespace chakra::net {

struct Buffer {
    char* data;
    size_t size;
    size_t bfree;
    size_t len;

    Buffer(size_t n);
    void writeMsg(const ::google::protobuf::Message& message, proto::types::Type type);
    void move(int start, int end);
    void maybeRealloc(size_t added = 0);

    template<typename T>
    T read(int index){
        if (index + sizeof(T) <= len){
            return *((T*)&data[index]);
        }
        return 0;
    }

    ~Buffer();
};

}

#endif // CHAKRA_NET_BUFFER_H