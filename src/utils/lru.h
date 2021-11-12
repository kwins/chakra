//
// Created by kwins on 2021/11/12.
//

#ifndef CHAKRA_LRU_H
#define CHAKRA_LRU_H
#include <unordered_map>
#include <list>
#include <cstddef>
#include <stdexcept>
#include <shared_mutex>
#include <mutex>
#include <thread>

namespace chakra::utils{
template<typename key_t, typename value_t>
class LruCache {
public:
    typedef typename std::pair<key_t, value_t> key_value_pair_t;
    typedef typename std::list<key_value_pair_t>::iterator list_iterator_t;

    explicit LruCache(size_t max_size) : _max_size(max_size) {}

    void put(const key_t& key, const value_t& value) {
        std::unique_lock<std::shared_mutex> lck(mutex_);
        auto it = _cache_items_map.find(key);
        _cache_items_list.push_front(key_value_pair_t(key, value));
        if (it != _cache_items_map.end()) {
            _cache_items_list.erase(it->second);
            _cache_items_map.erase(it);
        }
        _cache_items_map[key] = _cache_items_list.begin();

        if (_cache_items_map.size() > _max_size) {
            auto last = _cache_items_list.end();
            last--;
            _cache_items_map.erase(last->first);
            _cache_items_list.pop_back();
        }
    }

    bool get(const key_t& key, value_t& val) {
        std::shared_lock<std::shared_mutex> lck(mutex_);
        auto it = _cache_items_map.find(key);
        if (it == _cache_items_map.end()) {
            return false;
        } else {
            _cache_items_list.splice(_cache_items_list.begin(), _cache_items_list, it->second);
            val = it->second->second;
            return true;
        }
    }

    void del(const key_t& key){
        std::unique_lock<std::shared_mutex> lck(mutex_);
        auto it = _cache_items_map.find(key);
        if (it == _cache_items_map.end()) return;
        _cache_items_map.erase(it);
        _cache_items_list.erase(it->second);
    }

    bool exists(const key_t& key) const {
        std::unique_lock<std::shared_mutex> lck(mutex_);
        return _cache_items_map.find(key) != _cache_items_map.end();
    }

    size_t size() const {
        std::unique_lock<std::shared_mutex> lck(mutex_);
        return _cache_items_map.size();
    }

    size_t capacity(){ return _max_size; }

    void refresh(){
        std::unique_lock<std::shared_mutex> lck(mutex_);
        _cache_items_list.clear();
        _cache_items_map.clear();
    }
private:
    mutable std::shared_mutex mutex_;
    std::list<key_value_pair_t> _cache_items_list;
    std::unordered_map<key_t, list_iterator_t> _cache_items_map;
    size_t _max_size;
};

}
#endif //CHAKRA_LRU_H
