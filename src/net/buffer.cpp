#include "buffer.h"
#include <cstring>
#include <cstdlib>
#include <glog/logging.h>
#include "net/packet.h"

chakra::net::Buffer::Buffer(size_t n) : size(n), len(0), bfree(n) {
    data = (char*) ::malloc(sizeof(char) * n); 
}

chakra::net::Buffer::~Buffer() { if (data != nullptr) { ::free(data); data = nullptr; }}
void chakra::net::Buffer::maybeRealloc(size_t added) {
    if (bfree < added || (bfree == 0 && len == size)) { /* buffer not enough or full */
        LOG(INFO) << "[buffer] realloc because add size" << added 
                  << " current len " << len << " bfree " << bfree << " size " << size;
        size_t rsize = 0;
        if (added > size) {
            rsize = size * 2 + added;
        } else {
            rsize = size * 2; 
        }
        data = (char*)::realloc((void*)data, rsize);
        bfree = rsize - size;
        size = rsize;
    }
}

void chakra::net::Buffer::writeMsg(const ::google::protobuf::Message& message, proto::types::Type type) {
    chakra::net::Packet::serialize(message, type, this);
}

void chakra::net::Buffer::move(int start, int end) {
    int newLen = 0;
    if (len == 0) return;
    if (start < 0) {
        start = len + start;
        if (start < 0) start = 0;
    }
    if (end < 0) {
        end = len + end;
        if (end < 0) end = 0;
    }
    newLen = (start > end) ? 0 : (end - start)+1;
    if (newLen != 0) {
        if (start >= (signed)len) {
            newLen = 0;
        } else if (end >= (signed)len) {
            end = len - 1;
            newLen = (start > end) ? 0 : (end - start)+1;
        }
    } else {
        start = 0;
    }

    // 如果有需要，对字符串进行移动
    // T = O(N)
    if (start && newLen) ::memmove(data, data+start, newLen);
    data[newLen] = '\0';
    bfree = bfree + (len - newLen);
    len = newLen;
}