//
// Created by kwins on 2021/6/9.
//

#ifndef CHAKRA_LRU_H
#define CHAKRA_LRU_H
#include <sys/types.h>
#include <list>
#include <unordered_map>

namespace chakra::utils{

template<typename K, typename V>
class LruCache {
public:
    explicit LruCache(size_t size);
    bool get(K key, V& v);
    void put(K key, V val);

private:
    using PairIter = typename std::list<std::pair<K, V>>::iterator;
    size_t capacity = 0;
    std::list<std::pair<K, V>> pairs;
    std::unordered_map<K, PairIter> k2iter;
};

}



#endif //CHAKRA_LRU_H
