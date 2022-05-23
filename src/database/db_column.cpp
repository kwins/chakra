//
// Created by kwins on 2021/6/9.
//

#include "db_column.h"
#include <database/db_column_cache.h>
#include <element.pb.h>
#include <error/err.h>
#include <memory>
#include <rocksdb/cache.h>
#include <string>
#include <vector>
#include <zlib.h>
#include <rocksdb/db.h>
#include <glog/logging.h>
#include <rocksdb/utilities/backupable_db.h>
#include <gflags/gflags.h>
#include <iomanip>

DECLARE_int64(db_wal_ttl_seconds);
DECLARE_string(db_dir);
DECLARE_int32(db_cache_shard_size);
DECLARE_int64(db_default_cache_bytes);

chakra::database::ColumnDB::ColumnDB(const proto::peer::MetaDB& meta) {
    metaDB = meta;
    LOG(INFO) << "Create column db meta " << metaDB.DebugString();
    rocksdb::Options rocksOpts;
    rocksOpts.keep_log_file_num = 5;
    rocksOpts.create_if_missing = true;
    rocksOpts.WAL_ttl_seconds = FLAGS_db_wal_ttl_seconds;
    rocksdb::DB* dbself;
    auto s = rocksdb::DB::Open(rocksOpts, FLAGS_db_dir + "/" + metaDB.name() + ".self", &dbself);
    if (!s.ok()) {
        throw std::logic_error(s.ToString());
    }
    self = std::shared_ptr<rocksdb::DB>(dbself);
    rocksdb::DB* db;
    s = rocksdb::DB::Open(rocksOpts, FLAGS_db_dir + "/" + metaDB.name(), &db);
    if (!s.ok()){
        throw std::logic_error(s.ToString());
    }
    full = std::shared_ptr<rocksdb::DB>(db);

    cache.resize(FLAGS_db_cache_shard_size);
    int capacity = meta.cached() / FLAGS_db_cache_shard_size;
    if (capacity <= FLAGS_db_default_cache_bytes) {
        capacity = FLAGS_db_default_cache_bytes; // 缓存最小分片
    }
    for (int i = 0; i < FLAGS_db_cache_shard_size; i++) {
        cache[i] = std::make_shared<ColumnDBLRUCache>(capacity);
    }

    // 加载一些热数据
    auto iter = self->NewIterator(rocksdb::ReadOptions());
    for(iter->SeekToLast(); iter->Valid(); iter->Prev()){
        auto element = std::make_shared<proto::element::Element>();
        if (!element->ParseFromArray(iter->value().data(), iter->value().size())) {
            LOG(ERROR) << "parse element error";
            continue;
        }
        unsigned long hashed = ::crc32(0L, (unsigned char*)element->key().data(), element->key().size());
        cache[hashed % cache.size()]->set(element->key(), element);
        if (cache[hashed % cache.size()]->useage() > 0.5) {
            break;
        }
    }

    LOG(INFO) << "Column db " << meta.name() 
              << " preheat cache usage " << std::to_string(cacheUsage())
              << " cache size " << size();
    delete iter;
    LOG(INFO) << "Column db end";
}

std::shared_ptr<proto::element::Element> chakra::database::ColumnDB::get(const std::string &key) {
    unsigned long hashed = ::crc32(0L, (unsigned char*)key.data(), key.size());
    int i = hashed % FLAGS_db_cache_shard_size;
    auto element = cache[i]->get(key);
    if (!element) {
        std::string oriVal;
        auto s = full->Get(rocksdb::ReadOptions(), key, &oriVal);
        if (s.ok()) {
            element = std::make_shared<proto::element::Element>();
            element->ParseFromArray(oriVal.data(), oriVal.size());
            cache[i]->set(element->key(), element);
        } else {
            LOG(ERROR) << s.ToString();
        }
    }
    return element;
}

std::vector<std::shared_ptr<proto::element::Element>> chakra::database::ColumnDB::mget(const std::vector<std::string>& keys) {
    std::vector<std::shared_ptr<proto::element::Element>> result;
    result.resize(keys.size());
    for (int i = 0; i < keys.size(); i++) {
        result[i] = get(keys.at(i));
    }
    return result;
}

void chakra::database::ColumnDB::set(const std::string& key, const std::string& value, int64_t ttl) {
    unsigned long hashed = ::crc32(0L, (unsigned char*)key.data(), key.size());
    cache[hashed % FLAGS_db_cache_shard_size]->set(get(key), key, value, ttl, [this](std::shared_ptr<proto::element::Element> element){
        auto s = self->Put(rocksdb::WriteOptions(), element->key() , element->SerializeAsString());
        if (!s.ok()) return error::Error(s.ToString());
        return error::Error();
    });
}

void chakra::database::ColumnDB::set(const std::string& key, float value, int64_t ttl) {
    unsigned long hashed = ::crc32(0L, (unsigned char*)key.data(), key.size());
    cache[hashed % FLAGS_db_cache_shard_size]->set(get(key), key, value, ttl, [this](std::shared_ptr<proto::element::Element> element){
        auto s = self->Put(rocksdb::WriteOptions(), element->key() , element->SerializeAsString());
        if (!s.ok()) return error::Error(s.ToString());
        return error::Error();
    });
}

void chakra::database::ColumnDB::push(const std::string& key, const std::vector<std::string>& values, int64_t ttl) {
    unsigned long hashed = ::crc32(0L, (unsigned char*)key.data(), key.size());
    cache[hashed % FLAGS_db_cache_shard_size]->push(get(key), key, values, ttl, [this](std::shared_ptr<proto::element::Element> element){
        auto s = self->Put(rocksdb::WriteOptions(), element->key() , element->SerializeAsString());
        if (!s.ok()) return error::Error(s.ToString());
        return error::Error();
    });
}

void chakra::database::ColumnDB::push(const std::string& key, const std::vector<float>& values, int64_t ttl) {
    unsigned long hashed = ::crc32(0L, (unsigned char*)key.data(), key.size());
    cache[hashed % FLAGS_db_cache_shard_size]->push(get(key), key, values, ttl, [this](std::shared_ptr<proto::element::Element> element){
        auto s = self->Put(rocksdb::WriteOptions(), element->key() , element->SerializeAsString());
        if (!s.ok()) return error::Error(s.ToString());
        return error::Error();
    });
}

chakra::error::Error chakra::database::ColumnDB::incr(const std::string& key, float value, int64_t ttl) {
    unsigned long hashed = ::crc32(0L, (unsigned char*)key.data(), key.size());
    return cache[hashed % FLAGS_db_cache_shard_size]->incr(get(key), key, value, ttl, [this](std::shared_ptr<proto::element::Element> element){
        auto s = self->Put(rocksdb::WriteOptions(), element->key() , element->SerializeAsString());
        if (!s.ok()) return error::Error(s.ToString());
        return error::Error();
    });
}

void chakra::database::ColumnDB::erase(const std::string& key) {
    unsigned long hashed = ::crc32(0L, (unsigned char*)key.data(), key.size());
    cache[hashed % FLAGS_db_cache_shard_size]->erase(key);
    auto s = self->Delete(rocksdb::WriteOptions(), key);
    if (!s.ok()) LOG(ERROR) << "db del error " << s.ToString();
}

float chakra::database::ColumnDB::cacheUsage() {
    float used = 0.0;
    for(int i = 0; i < cache.size(); i++) {
        used += cache[i]->useage();
    }
    return used / (float)cache.size();
}

chakra::error::Error chakra::database::ColumnDB::rocksWriteBulk(rocksdb::WriteBatch& batch) {
    auto s = batch.Iterate(this);
    if (!s.ok()){
        return error::Error(s.ToString());
    }
    return error::Error();
}

size_t chakra::database::ColumnDB::size() {
    size_t num = 0;
    for(auto & shard : cache){
        num += shard->size();
    }
    return num;
}

proto::peer::MetaDB chakra::database::ColumnDB::getMetaDB(const std::string &dbname) { return metaDB; }

chakra::error::Error chakra::database::ColumnDB::restoreDB() {
    // backup
    rocksdb::BackupEngine* backupEngine;
    std::string backupdir = FLAGS_db_dir + "/" + metaDB.name();
    auto s = rocksdb::BackupEngine::Open(rocksdb::Env::Default(), rocksdb::BackupableDBOptions(backupdir), &backupEngine);
    if (!s.ok()){
        return error::Error(s.ToString());
    }
    s = backupEngine->CreateNewBackup(self.get());
    if (!s.ok()){
        return error::Error(s.ToString());
    }
    delete backupEngine;

    // restore
    rocksdb::BackupEngineReadOnly* restore;
    std::string restoredir = FLAGS_db_dir + "/" + metaDB.name() + ".backup";
    s = rocksdb::BackupEngineReadOnly::Open(rocksdb::Env::Default(),rocksdb::BackupableDBOptions(restoredir), &restore);
    if (!s.ok()){
        return error::Error(s.ToString());
    }
    s = restore->RestoreDBFromLatestBackup(restoredir, restoredir);
    if (!s.ok()){
        return error::Error(s.ToString());
    }
    delete restore;
    lastRestore.seq = self->GetLatestSequenceNumber();
    return error::Error();
}

chakra::database::ColumnDB::RestoreDB chakra::database::ColumnDB::getLastRestoreDB() {
    return chakra::database::ColumnDB::RestoreDB();
}

chakra::error::Error chakra::database::ColumnDB::getUpdateSince(rocksdb::SequenceNumber seq,
                                                                std::unique_ptr<rocksdb::TransactionLogIterator> *iter) {
    /* 1、请求seq小于lastseq 且 size > 0 有效
     * 2、请求seq小于lastseq 且 size = 0 无效，增量日志已经被删除了
     * 3、请求seq等于lastseq 且 size > 0 有效，但是size=1 这个数据已经被获取过了
     * 4、请求seq等于lastseq 且 size = 0 有效，增量日志已经被删了，但是lastseq并没有变化，说并在此期间并没有新的数据写入
     * */
    auto s = self->GetUpdatesSince(seq, iter);
    if (!s.ok()){
        return error::Error(s.ToString());
    }
    return error::Error();
}

chakra::error::Error
chakra::database::ColumnDB::snapshot(rocksdb::Iterator **iter, rocksdb::SequenceNumber &seq) {
    auto snapshot = self->GetSnapshot();
    rocksdb::ReadOptions readOptions;
    readOptions.snapshot = snapshot;
    seq = snapshot->GetSequenceNumber();
    (*iter) = self->NewIterator(readOptions);
    return error::Error();
}

rocksdb::SequenceNumber chakra::database::ColumnDB::getLastSeqNumber() {
    return self->GetLatestSequenceNumber();
}

chakra::database::ColumnDB::~ColumnDB() {
    if (self) {
        self->Close();
    }
    if (full) {
        full->Close();
    }
}

void chakra::database::ColumnDB::Put(const rocksdb::Slice &key, const rocksdb::Slice &value) {
    auto s = full->Put(rocksdb::WriteOptions(), key, value);
    if (!s.ok()) LOG(ERROR) << "db delta put error " << s.ToString();
    // 淘汰缓存，下次read重新加载
    unsigned long hashed = ::crc32(0L, (unsigned char*)key.data(), key.size());
    cache[hashed % FLAGS_db_cache_shard_size]->erase(key.ToString());
}

void chakra::database::ColumnDB::Delete(const rocksdb::Slice &key) {
    auto s = full->Delete(rocksdb::WriteOptions(), key);
    if (!s.ok()) LOG(ERROR) << "db delta del error " << s.ToString();    
    // 淘汰缓存，下次read重新加载
    unsigned long hashed = ::crc32(0L, (unsigned char*)key.data(), key.size());
    cache[hashed % FLAGS_db_cache_shard_size]->erase(key.ToString());
}
