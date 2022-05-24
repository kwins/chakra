#ifndef CHAKRA_DB_COLUMN_CACHE_H
#define CHAKRA_DB_COLUMN_CACHE_H

#include <cstdint>
#include <functional>
#include <glog/logging.h>
#include <memory>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <list>
#include <cstddef>
#include <stdexcept>
#include <shared_mutex>
#include <mutex>
#include <thread>
#include <vector>
#include "element.pb.h"
#include "error/err.h"

namespace chakra::database {

class ColumnDBLRUCache {
public:
    using KEY_VALUE_PAIR = typename std::shared_ptr<proto::element::Element>;
    using KEY_VALUE_PAIR_ITERATOR =  typename std::list<KEY_VALUE_PAIR>::iterator;
    using Callback =  typename std::function<error::Error(std::shared_ptr<proto::element::Element>)>;
    explicit ColumnDBLRUCache(size_t capacity);
    
    std::shared_ptr<proto::element::Element> get(const std::string& key);
    std::vector<std::shared_ptr<proto::element::Element>> mget(const std::vector<std::string>& keys);

    error::Error set(std::shared_ptr<proto::element::Element> element, const std::string& key, const std::string& value, int64_t ttl = 0, Callback f = defaultCB);
    error::Error set(std::shared_ptr<proto::element::Element> element, const std::string& key, float value, int64_t ttl = 0, Callback f = defaultCB);

    error::Error push(std::shared_ptr<proto::element::Element> element, const std::string& key, const std::vector<std::string>& values, int64_t ttl = 0, Callback f = defaultCB);
    error::Error push(std::shared_ptr<proto::element::Element> element, const std::string& key, const std::vector<float>& values, int64_t ttl = 0, Callback f = defaultCB);

    error::Error incr(std::shared_ptr<proto::element::Element> element, const std::string& key, float value, int64_t ttl = 0, Callback f = defaultCB);
    
    void set(const std::string& key, std::shared_ptr<proto::element::Element> element);

    void erase(const std::string& key);
    bool exist(const std::string& key);
    void refresh();
    float useage();
    int64_t size() const;

private:    
    size_t membytes(const std::string& key, const std::shared_ptr<proto::element::Element>& element);
    mutable std::shared_mutex mutex;
    std::list<KEY_VALUE_PAIR> list;
    std::unordered_map<std::string, KEY_VALUE_PAIR_ITERATOR> table;
    size_t capacity_bytes;
    size_t used_bytes;
    static Callback defaultCB;
};

}
#endif // CHAKRA_DB_COLUMN_CACHE_H