//
// Created by kwins on 2021/6/9.
//

#include "lru.h"

template<typename K, typename V>
chakra::utils::LruCache<K, V>::LruCache(size_t capacity) { this->capacity = capacity; }

template<typename K, typename V>
bool chakra::utils::LruCache<K, V>::get(K key, V &v) {
    auto it = k2iter.find(key);
    if (it == k2iter.end()){
        return false;
    }

    std::pair<K, V> node = it->second;
    pairs.erase(it);
    pairs.push_front(node);
    k2iter[key] = pairs.begin();
    v = node.second;
    return true;
}

template<typename K, typename V>
void chakra::utils::LruCache<K, V>::put(K key, V val) {
    auto node = std::make_pair(key, val);
    if (k2iter.count(key)){
        pairs.erase(k2iter[key]); // 若该节点已存在，则删除旧的节点
    } else {
        if (capacity == pairs.size()){ // 已满

        }
    }
}
