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
        list.splice(list.begin(), list, it->second); // 头插入
        return (*it->second);
    }
}

std::vector<std::shared_ptr<proto::element::Element>> chakra::database::ColumnDBLRUCache::mget(const std::vector<std::string>& keys) {
    std::vector<std::shared_ptr<proto::element::Element>> result;
    result.resize(keys.size());
    std::shared_lock<std::shared_mutex> lck(mutex);
    for(int i = 0; i < keys.size(); i++) {
        auto it = table.find(keys.at(i));
        if (it == table.end()) {
            result[i] = nullptr;
        } else {
            result[i] = (*it->second);
            list.splice(list.begin(), list, it->second); // 头插入
        }
    }
    return result;
}

void chakra::database::ColumnDBLRUCache::set(std::shared_ptr<proto::element::Element> element, const std::string& key, const std::string& value, int64_t ttl, Callback cb) {
    std::unique_lock<std::shared_mutex> lck(mutex);
    auto nowms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    if (!element) {
        element = std::make_shared<proto::element::Element>();
        element->set_key(key);
        element->set_type(::proto::element::ElementType::NF);
        element->set_create_time(nowms);
        element->set_last_visit_time(0);
    }
    if (ttl > 0) {
        element->set_expire_time(element->create_time() + ttl);
    } else {
        element->set_expire_time(0);
    }
    element->set_type(::proto::element::ElementType::STRING);
    element->set_s(value);
    
    set(key, element);

    cb(element);
}

void chakra::database::ColumnDBLRUCache::set(std::shared_ptr<proto::element::Element> element, const std::string& key, float value, int64_t ttl, Callback cb) {
    std::unique_lock<std::shared_mutex> lck(mutex);
    auto nowms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    if (!element) {
        element = std::make_shared<proto::element::Element>();
        element->set_key(key);
        element->set_type(::proto::element::ElementType::NF);
        element->set_create_time(nowms);
        element->set_last_visit_time(0);
    }
    if (ttl > 0) {
        element->set_expire_time(element->create_time() + ttl);
    } else {
        element->set_expire_time(0);
    }
    element->set_type(::proto::element::ElementType::FLOAT);
    element->set_f(value);
    
    set(key, element);

    cb(element);
}

void chakra::database::ColumnDBLRUCache::push(std::shared_ptr<proto::element::Element> element, const std::string& key, const std::vector<std::string>& values, int64_t ttl, Callback cb) {
    std::unique_lock<std::shared_mutex> lck(mutex);
    auto nowms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    if (!element) {
        element = std::make_shared<proto::element::Element>();
        element->set_key(key);
        element->set_type(::proto::element::ElementType::NF);
        element->set_create_time(nowms);
        element->set_last_visit_time(0);
    }
    if (ttl > 0) {
        element->set_expire_time(element->create_time() + ttl);
    } else {
        element->set_expire_time(0);
    }
    element->set_type(::proto::element::ElementType::STRING_ARRAY);
    for (auto& v : values) {
        element->mutable_ss()->add_value(v);
    }
    
    set(key, element);

    cb(element);
}

void chakra::database::ColumnDBLRUCache::push(std::shared_ptr<proto::element::Element> element, const std::string& key, const std::vector<float>& values, int64_t ttl, Callback cb) {
    std::unique_lock<std::shared_mutex> lck(mutex);
    auto nowms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    if (!element) {
        element = std::make_shared<proto::element::Element>();
        element->set_key(key);
        element->set_type(::proto::element::ElementType::NF);
        element->set_create_time(nowms);
        element->set_last_visit_time(0);
    }
    if (ttl > 0) {
        element->set_expire_time(element->create_time() + ttl);
    } else {
        element->set_expire_time(0);
    }
    element->set_type(::proto::element::ElementType::FLOAT_ARRAY);
    for (auto& v : values) {
        element->mutable_ff()->add_value(v);
    }
    
    set(key, element);

    cb(element);
}

chakra::error::Error chakra::database::ColumnDBLRUCache::incr(std::shared_ptr<proto::element::Element> element, const std::string& key, float value, int64_t ttl, Callback cb) {
    if (element && element->type() != ::proto::element::ElementType::FLOAT) {
        return error::Error("incr value type must be float");
    }

    std::unique_lock<std::shared_mutex> lck(mutex);
    auto nowms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    if (!element) {
        element = std::make_shared<proto::element::Element>();
        element->set_key(key);
        element->set_type(::proto::element::ElementType::NF);
        element->set_create_time(nowms);
        element->set_last_visit_time(0);
    }
    if (ttl > 0) {
        element->set_expire_time(element->create_time() + ttl);
    } else {
        element->set_expire_time(0);
    }
    element->set_type(::proto::element::ElementType::FLOAT);
    element->set_f(element->f() + value);
    
    set(key, element);

    cb(element);
    return error::Error();
}

void chakra::database::ColumnDBLRUCache::set(const std::string& key, std::shared_ptr<proto::element::Element> element) {
    // 头插入
    list.push_front(element);
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