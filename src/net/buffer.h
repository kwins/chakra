#ifndef CHAKRA_NET_BUFFER_H
#define CHAKRA_NET_BUFFER_H

#include <cstddef>

namespace chakra::net {

struct Buffer {
    char* data;
    size_t size;
    size_t free;
    size_t len;

    Buffer(size_t n);
    void move(int start, int end);
    void maybeRealloc(size_t added = 0);
    ~Buffer();
};

}

#endif // CHAKRA_NET_BUFFER_H