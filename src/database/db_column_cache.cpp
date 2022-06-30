#include "db_column_cache.h"
#include <element.pb.h>
#include <error/err.h>
#include <memory>
#include <chrono>
#include <mutex>
#include <string_view>
#include <type_traits>

chakra::database::ColumnDBLRUCache::ColumnDBLRUCache(size_t capacity):capacity_bytes(capacity), used_bytes(0) {}

std::shared_ptr<proto::element::Element> chakra::database::ColumnDBLRUCache::get(const std::string &key) {
    std::lock_guard<std::mutex> lck(mutex);
    auto it = table.find(key);
    if (it == table.end()) {
        return nullptr;
    }
    list.splice(list.begin(), list, it->second); // 头插入
    return (*it->second);
}

std::vector<std::shared_ptr<proto::element::Element>> chakra::database::ColumnDBLRUCache::mget(const std::vector<std::string>& keys) {
    std::lock_guard<std::mutex> lck(mutex);
    std::vector<std::shared_ptr<proto::element::Element>> result;
    result.resize(keys.size());
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

chakra::error::Error chakra::database::ColumnDBLRUCache::set(std::shared_ptr<proto::element::Element> element, const std::string& key, const std::string& value, int64_t ttl, Callback cb) {
    std::lock_guard<std::mutex> lck(mutex);
    auto nowms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    if (!element) {
        element = std::make_shared<proto::element::Element>();
        element->set_key(key);
        element->set_type(::proto::element::ElementType::NF);
        element->set_create_time(nowms);
        element->set_last_visit_time(0);
    } else if (element->type() != proto::element::ElementType::STRING) {
        return error::Error("data type must be " + proto::element::ElementType_Name(element->type()));
    }

    if (ttl > 0) {
        element->set_expire_time(element->create_time() + ttl);
    } else {
        element->set_expire_time(0);
    }
    element->set_type(::proto::element::ElementType::STRING);
    element->set_s(value);
    
    setNL(key, element);

    return cb(element);
}

chakra::error::Error chakra::database::ColumnDBLRUCache::set(std::shared_ptr<proto::element::Element> element, const std::string& key, float value, int64_t ttl, Callback cb) {
    std::lock_guard<std::mutex> lck(mutex);
    auto nowms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    if (!element) {
        element = std::make_shared<proto::element::Element>();
        element->set_key(key);
        element->set_type(::proto::element::ElementType::NF);
        element->set_create_time(nowms);
        element->set_last_visit_time(0);
    } else if (element->type() != proto::element::ElementType::FLOAT) {
        return error::Error("data type must be " + proto::element::ElementType_Name(element->type()));
    }

    if (ttl > 0) {
        element->set_expire_time(element->create_time() + ttl);
    } else {
        element->set_expire_time(0);
    }
    element->set_type(::proto::element::ElementType::FLOAT);
    element->set_f(value);
    
    setNL(key, element);

    return cb(element);
}

chakra::error::Error chakra::database::ColumnDBLRUCache::push(std::shared_ptr<proto::element::Element> element, const std::string& key, const std::vector<std::string>& values, int64_t ttl, Callback cb) {
    std::lock_guard<std::mutex> lck(mutex);
    auto nowms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    if (!element) {
        element = std::make_shared<proto::element::Element>();
        element->set_key(key);
        element->set_type(::proto::element::ElementType::STRING_ARRAY);
        element->set_create_time(nowms);
        element->set_last_visit_time(0);
    } else if (element->type() != proto::element::ElementType::STRING_ARRAY) {
        return error::Error("data type must be " + proto::element::ElementType_Name(element->type()));
    }
    
    if (ttl > 0) {
        element->set_expire_time(element->create_time() + ttl);
    } else {
        element->set_expire_time(0);
    }
    
    for (auto& v : values) {
        element->mutable_ss()->add_value(v);
    }
    
    setNL(key, element);

    return cb(element);
}

chakra::error::Error chakra::database::ColumnDBLRUCache::push(std::shared_ptr<proto::element::Element> element, const std::string& key, const std::vector<float>& values, int64_t ttl, Callback cb) {
    std::lock_guard<std::mutex> lck(mutex);
    auto nowms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    if (!element) {
        element = std::make_shared<proto::element::Element>();
        element->set_key(key);
        element->set_type(::proto::element::ElementType::FLOAT_ARRAY);
        element->set_create_time(nowms);
        element->set_last_visit_time(0);
    } else if (element->type() != proto::element::ElementType::FLOAT_ARRAY) {
        return error::Error("data type must be " + proto::element::ElementType_Name(element->type()));
    }

    if (ttl > 0) {
        element->set_expire_time(element->create_time() + ttl);
    } else {
        element->set_expire_time(0);
    }
    
    for (auto& v : values) {
        element->mutable_ff()->add_value(v);
    }
    
    setNL(key, element);

    return cb(element);
}

chakra::error::Error chakra::database::ColumnDBLRUCache::incr(std::shared_ptr<proto::element::Element> element, const std::string& key, float value, int64_t ttl, Callback cb) {
    std::lock_guard<std::mutex> lck(mutex);
    auto nowms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    if (!element) {
        element = std::make_shared<proto::element::Element>();
        element->set_key(key);
        element->set_type(::proto::element::ElementType::FLOAT);
        element->set_create_time(nowms);
        element->set_last_visit_time(0);
    } else if (element->type() != proto::element::ElementType::FLOAT) {
        return error::Error("data type must be " + proto::element::ElementType_Name(element->type()));
    }

    if (ttl > 0) {
        element->set_expire_time(element->create_time() + ttl);
    } else {
        element->set_expire_time(0);
    }
    
    element->set_f(element->f() + value);

    setNL(key, element);

    cb(element);
    return error::Error();
}

void chakra::database::ColumnDBLRUCache::set(const std::string& key, std::shared_ptr<proto::element::Element> element) {
    std::lock_guard<std::mutex> lck(mutex);
    setNL(key, element);
}

void chakra::database::ColumnDBLRUCache::setNL(const std::string& key, std::shared_ptr<proto::element::Element> element) {
    if (table.count(key) > 0) /* 删除老元素 */
        list.erase(table[key]);

    list.push_front(element);
    table[key] = list.begin(); /*  直接覆盖 */
    used_bytes += membytes(element);
    // 超出容量
    if (used_bytes > capacity_bytes) {
        auto& back = list.back();
        used_bytes -= membytes(back);
        table.erase(list.back()->key());
        list.pop_back();
    }
}

void chakra::database::ColumnDBLRUCache::erase(const std::string& key) {
    std::lock_guard<std::mutex> lck(mutex);
    eraseNL(key);
}

void chakra::database::ColumnDBLRUCache::erase(const std::set<std::string>& keys) {
    std::lock_guard<std::mutex> lck(mutex);
    for (auto& k : keys) {
        eraseNL(k);
    }
}

void chakra::database::ColumnDBLRUCache::eraseNL(const std::string& key) {
    auto it = table.find(key);
    if (it == table.end()) 
        return;
    used_bytes -= membytes((*it->second));
    list.erase(it->second);
    table.erase(it);
}

bool chakra::database::ColumnDBLRUCache::exist(const std::string& key) {
    std::lock_guard<std::mutex> lck(mutex);
    auto it = table.find(key);
    return it != table.end();
}

void chakra::database::ColumnDBLRUCache::clear() {
    std::lock_guard<std::mutex> lck(mutex);
    list.clear();
    table.clear();
}

int64_t chakra::database::ColumnDBLRUCache::size() const { return list.size(); }
float chakra::database::ColumnDBLRUCache::useage() { return (float)used_bytes / (float)capacity_bytes; }

size_t chakra::database::ColumnDBLRUCache::membytes(const std::shared_ptr<proto::element::Element>& element) {
    return (sizeof(element) + element->ByteSizeLong() + sizeof(element->key()) + element->key().size());
}

chakra::database::ColumnDBLRUCache::Callback chakra::database::ColumnDBLRUCache::defaultCB = [](std::shared_ptr<proto::element::Element>)->chakra::error::Error{ return error::Error(); };