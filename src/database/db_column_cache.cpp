#include "db_column_cache.h"
#include <element.pb.h>
#include <error/err.h>
#include <memory>
#include <chrono>
#include <type_traits>

chakra::database::ColumnDBLRUCache::ColumnDBLRUCache(size_t capacity):capacity_bytes(capacity), used_bytes(0) {}

std::shared_ptr<proto::element::Element> chakra::database::ColumnDBLRUCache::get(const std::string &key) {
    std::shared_lock<std::shared_mutex> lck(mutex);
    auto it = table.find(key);
    if (it == table.end()) {
        return nullptr;
    } else {
        // 头插入
        list.splice(list.begin(), list, it->second);
        return (*it->second);
    }
}

void chakra::database::ColumnDBLRUCache::set(const std::string& key, const std::string& value, int64_t ttl, Callback cb) {
    std::unique_lock<std::shared_mutex> lck(mutex);

    std::shared_ptr<proto::element::Element> element = step1(key, ttl);

    element->set_type(::proto::element::ElementType::STRING);
    element->set_s(value);
    
    set(key, element);

    cb(element);
} 

void chakra::database::ColumnDBLRUCache::set(const std::string& key, int64_t value, int64_t ttl, Callback cb) {
    std::unique_lock<std::shared_mutex> lck(mutex);

    std::shared_ptr<proto::element::Element> element = step1(key, ttl);

    element->set_type(::proto::element::ElementType::INTEGER);
    element->set_i(value);
    
    set(key, element);

    cb(element);
} 

void chakra::database::ColumnDBLRUCache::set(const std::string& key, float value, int64_t ttl, Callback cb) {
    std::unique_lock<std::shared_mutex> lck(mutex);

    std::shared_ptr<proto::element::Element> element = step1(key, ttl);

    element->set_type(::proto::element::ElementType::FLOAT);
    element->set_f(value);
    
    set(key, element);

    cb(element);
}

void chakra::database::ColumnDBLRUCache::push(const std::string& key, const std::vector<std::string>& values, int64_t ttl, Callback cb) {
    std::unique_lock<std::shared_mutex> lck(mutex);

    std::shared_ptr<proto::element::Element> element = step1(key, ttl);

    element->set_type(::proto::element::ElementType::STRING_ARRAY);
    for (auto& v : values) {
        element->mutable_ss()->add_ss(v);
    }
    
    set(key, element);

    cb(element);
}

void chakra::database::ColumnDBLRUCache::push(const std::string& key, const std::vector<int64_t>& values, int64_t ttl, Callback cb) {
    std::unique_lock<std::shared_mutex> lck(mutex);

    std::shared_ptr<proto::element::Element> element = step1(key, ttl);

    element->set_type(::proto::element::ElementType::INTEGER_ARRAY);
    for (auto& v : values) {
        element->mutable_ii()->add_ii(v);
    }
    
    set(key, element);

    cb(element);
}

void chakra::database::ColumnDBLRUCache::push(const std::string& key, const std::vector<float>& values, int64_t ttl, Callback cb) {
    std::unique_lock<std::shared_mutex> lck(mutex);

    std::shared_ptr<proto::element::Element> element = step1(key, ttl);
    element->set_type(::proto::element::ElementType::FLOAT_ARRAY);
    for (auto& v : values) {
        element->mutable_ff()->add_ff(v);
    }
    
    set(key, element);

    cb(element);
}

chakra::error::Error chakra::database::ColumnDBLRUCache::incr(const std::string& key, int64_t value, int64_t ttl, Callback cb) {
    std::unique_lock<std::shared_mutex> lck(mutex);

    std::shared_ptr<proto::element::Element> element = step1(key, ttl);
    if (element->type() != ::proto::element::ElementType::INTEGER) {
        return error::Error("Incr value type must be int64");
    }
    element->set_type(::proto::element::ElementType::INTEGER);
    element->set_i(element->i() + value);
    
    set(key, element);

    cb(element);
    return error::Error();
}

chakra::error::Error chakra::database::ColumnDBLRUCache::incr(const std::string& key, float value, int64_t ttl, Callback cb) {
    std::unique_lock<std::shared_mutex> lck(mutex);

    std::shared_ptr<proto::element::Element> element = step1(key, ttl);
    if (element->type() != ::proto::element::ElementType::FLOAT) {
        return error::Error("Incr value type must be float");
    }
    element->set_type(::proto::element::ElementType::FLOAT);
    element->set_f(element->i() + value);
    
    set(key, element);

    cb(element);
    return error::Error();
}

std::shared_ptr<proto::element::Element> chakra::database::ColumnDBLRUCache::step1(const std::string& key, int64_t ttl) {
    auto nowms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    std::shared_ptr<proto::element::Element> element;
    auto it = table.find(key);
    if (it != table.end()) {
        element = (*it->second);
        list.erase(it->second);
        table.erase(it);
        used_bytes -= membytes(key, element);
    } else {
        element = std::make_shared<proto::element::Element>();
        element->set_key(key);
        element->set_type(::proto::element::ElementType::NF);
        element->set_create_time(nowms);
        element->set_last_visit_time(0);
    }
    if (ttl > 0) {
        element->set_expire_time(nowms + ttl);
    } else {
        element->set_expire_time(0);
    }
    return element;
}

void chakra::database::ColumnDBLRUCache::set(const std::string& key, std::shared_ptr<proto::element::Element> element) {
    // 头插入
    list.push_front(KEY_VALUE_PAIR(element));
    // 记录新的位置
    table[key] = list.begin();
    used_bytes += membytes(key, element);
    // 超出容量
    if (used_bytes > capacity_bytes) {
        auto last = list.end();
        last--; //  last one
        table.erase((*last)->key());
        list.pop_back();
        used_bytes -= membytes((*last)->key(), (*last));
    }
}

void chakra::database::ColumnDBLRUCache::erase(const std::string& key) {
    std::unique_lock<std::shared_mutex> lck(mutex);
    auto it = table.find(key);
    if (it == table.end()) return;
    used_bytes -= membytes(key, (*it->second));
    table.erase(it);
    list.erase(it->second);
}

bool chakra::database::ColumnDBLRUCache::exist(const std::string& key) {
    std::unique_lock<std::shared_mutex> lck(mutex);
    auto it = table.find(key);
    return it != table.end();
}

void chakra::database::ColumnDBLRUCache::refresh() {
    std::unique_lock<std::shared_mutex> lck(mutex);
    list.clear();
    table.clear();
}

int64_t chakra::database::ColumnDBLRUCache::size() const { return list.size(); }
float chakra::database::ColumnDBLRUCache::useage() { return (float)used_bytes / (float)capacity_bytes; }

size_t chakra::database::ColumnDBLRUCache::membytes(const std::string& key, const std::shared_ptr<proto::element::Element>& element) {
    return (sizeof(element) + element->ByteSizeLong() + sizeof(key) + key.size());
}

chakra::database::ColumnDBLRUCache::Callback chakra::database::ColumnDBLRUCache::defaultCB = [](std::shared_ptr<proto::element::Element>)->chakra::error::Error{ return error::Error(); };