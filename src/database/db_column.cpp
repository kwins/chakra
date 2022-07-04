//
// Created by kwins on 2021/6/9.
//

#include "db_column.h"
#include <database/db_column_cache.h>
#include <element.pb.h>
#include <error/err.h>
#include <memory>
#include <rocksdb/cache.h>
#include <rocksdb/iterator.h>
#include <rocksdb/options.h>
#include <rocksdb/snapshot.h>
#include <string>
#include <utils/basic.h>
#include <vector>
#include <zlib.h>
#include <rocksdb/db.h>
#include <glog/logging.h>
#include <rocksdb/utilities/backupable_db.h>
#include <gflags/gflags.h>
#include <iomanip>

DECLARE_int64(db_wal_ttl_seconds);
DECLARE_int64(db_keep_log_file_num);
DECLARE_string(db_dir);
DECLARE_int32(db_cache_shard_size);
DECLARE_int64(db_default_cache_shard_bytes);

chakra::database::ColumnDB::ColumnDB(const proto::peer::MetaDB& meta) {
    metaDB = meta;
    DLOG(INFO) << "[columndb] create meta is " << metaDB.DebugString();
    rocksdb::Options rocksOpts;
    rocksOpts.keep_log_file_num = FLAGS_db_keep_log_file_num;
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
    if (!s.ok()) {
        throw std::logic_error(s.ToString());
    }
    full = std::shared_ptr<rocksdb::DB>(db);

    caches.resize(FLAGS_db_cache_shard_size);
    int capacity = meta.cached();
    if (capacity <= FLAGS_db_default_cache_shard_bytes) {
        capacity = FLAGS_db_default_cache_shard_bytes; // 缓存最小分片
    }
    for (int i = 0; i < FLAGS_db_cache_shard_size; i++) {
        caches[i] = std::make_shared<ColumnDBLRUCache>(capacity);
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
        caches[hashed % caches.size()]->set(element->key(), element);
        if (caches[hashed % caches.size()]->useage() > 0.5) {
            break;
        }
    }

    DLOG(INFO) << "[columndb] " << meta.name() 
              << " preheat cache usage " << std::to_string(cacheUsage())
              << " cache size " << size();
    delete iter;
    DLOG(INFO) << "[columndb] end";
}

std::shared_ptr<proto::element::Element> chakra::database::ColumnDB::get(const std::string &key) {
    size_t hashed = ::crc32(0L, (unsigned char*)key.data(), key.size());
    int i = hashed % FLAGS_db_cache_shard_size;
    auto element = caches[i]->get(key);
    if (!element) {
        std::string oriVal;
        auto s = full->Get(rocksdb::ReadOptions(), key, &oriVal);
        if (s.ok()) {
            element = std::make_shared<proto::element::Element>();
            element->ParseFromArray(oriVal.data(), oriVal.size());
            caches[i]->set(element->key(), element);
        } else if (!s.IsNotFound()) {
            LOG(ERROR) << s.ToString();
        }
    }
    return element;
}

std::vector<std::shared_ptr<proto::element::Element>> chakra::database::ColumnDB::mget(const std::vector<std::string>& keys) {
    std::vector<std::shared_ptr<proto::element::Element>> result;
    result.resize(keys.size());
    for (int i = 0; i < keys.size(); i++) {
        result[i] = get(keys.at(i)); // result[i] may be nullptr
    }
    return result;
}

chakra::error::Error chakra::database::ColumnDB::set(const std::string& key, const std::string& value, int64_t ttl) {
    size_t hashed = ::crc32(0L, (unsigned char*)key.data(), key.size());
    return caches[hashed % FLAGS_db_cache_shard_size]->set(get(key), key, value, ttl, [this](std::shared_ptr<proto::element::Element> element){
        auto s = self->Put(rocksdb::WriteOptions(), element->key() , element->SerializeAsString());
        if (!s.ok()) return error::Error(s.ToString());
        return error::Error();
    });
}

chakra::error::Error chakra::database::ColumnDB::set(const std::string& key, float value, int64_t ttl) {
    size_t hashed = ::crc32(0L, (unsigned char*)key.data(), key.size());
    return caches[hashed % FLAGS_db_cache_shard_size]->set(get(key), key, value, ttl, [this](std::shared_ptr<proto::element::Element> element){
        auto s = self->Put(rocksdb::WriteOptions(), element->key() , element->SerializeAsString());
        if (!s.ok()) return error::Error(s.ToString());
        return error::Error();
    });
}

chakra::error::Error chakra::database::ColumnDB::push(const std::string& key, const std::vector<std::string>& values, int64_t ttl) {
    size_t hashed = ::crc32(0L, (unsigned char*)key.data(), key.size());
    return caches[hashed % FLAGS_db_cache_shard_size]->push(get(key), key, values, ttl, [this](std::shared_ptr<proto::element::Element> element){
        auto s = self->Put(rocksdb::WriteOptions(), element->key() , element->SerializeAsString());
        if (!s.ok()) return error::Error(s.ToString());
        return error::Error();
    });
}

chakra::error::Error chakra::database::ColumnDB::push(const std::string& key, const std::vector<float>& values, int64_t ttl) {
    size_t hashed = ::crc32(0L, (unsigned char*)key.data(), key.size());
    return caches[hashed % FLAGS_db_cache_shard_size]->push(get(key), key, values, ttl, [this](std::shared_ptr<proto::element::Element> element){
        auto s = self->Put(rocksdb::WriteOptions(), element->key() , element->SerializeAsString());
        if (!s.ok()) return error::Error(s.ToString());
        return error::Error();
    });
}

chakra::error::Error chakra::database::ColumnDB::incr(const std::string& key, float value, int64_t ttl) {
    size_t hashed = ::crc32(0L, (unsigned char*)key.data(), key.size());
    return caches[hashed % FLAGS_db_cache_shard_size]->incr(get(key), key, value, ttl, [this](std::shared_ptr<proto::element::Element> element){
        auto s = self->Put(rocksdb::WriteOptions(), element->key() , element->SerializeAsString());
        if (!s.ok()) return error::Error(s.ToString());
        return error::Error();
    });
}

void chakra::database::ColumnDB::erase(const std::string& key) {
    size_t hashed = ::crc32(0L, (unsigned char*)key.data(), key.size());
    caches[hashed % FLAGS_db_cache_shard_size]->erase(key);
    auto s = self->Delete(rocksdb::WriteOptions(), key);
    if (!s.ok() && !s.IsNotFound()) LOG(ERROR) << "[columndb] db del error " << s.ToString();
}

float chakra::database::ColumnDB::cacheUsage() {
    float used = 0.0;
    for(int i = 0; i < caches.size(); i++) {
        used += caches[i]->useage();
    }
    return used / (float)caches.size();
}

size_t chakra::database::ColumnDB::size() {
    size_t num = 0;
    for(auto & shard : caches){
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
    if (!s.ok()) {
        return error::Error(s.ToString());
    }
    s = backupEngine->CreateNewBackup(self.get());
    if (!s.ok()) {
        return error::Error(s.ToString());
    }
    delete backupEngine;

    // restore
    rocksdb::BackupEngineReadOnly* restore;
    std::string restoredir = FLAGS_db_dir + "/" + metaDB.name() + ".backup";
    s = rocksdb::BackupEngineReadOnly::Open(rocksdb::Env::Default(),rocksdb::BackupableDBOptions(restoredir), &restore);
    if (!s.ok()) {
        return error::Error(s.ToString());
    }
    s = restore->RestoreDBFromLatestBackup(restoredir, restoredir);
    if (!s.ok()) {
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

const rocksdb::Snapshot* chakra::database::ColumnDB::snapshot() { return self->GetSnapshot(); }
void chakra::database::ColumnDB::releaseSnapshot(const rocksdb::Snapshot *snapshot) { self->ReleaseSnapshot(snapshot); }
rocksdb::Iterator* chakra::database::ColumnDB::iterator(const rocksdb::ReadOptions& readOptions) {
    return self->NewIterator(readOptions);
}

rocksdb::SequenceNumber chakra::database::ColumnDB::getLastSeqNumber() {
    return self->GetLatestSequenceNumber();
}

chakra::error::Error chakra::database::ColumnDB::writeBatch(rocksdb::WriteBatch &batch, const std::set<std::string>& keys) {
    auto s1 = utils::Basic::getNowMillSec();
    rocksdb::WriteOptions writeOpts;
    writeOpts.disableWAL = true;
    auto s = full->Write(writeOpts, &batch);
    if (!s.ok()) {
        return error::Error(s.ToString());
    }
    auto s2 = utils::Basic::getNowMillSec();
    for (auto& key : keys) {
        size_t hashed = ::crc32(0L, (unsigned char*)key.data(), key.size());
        if (caches[hashed % FLAGS_db_cache_shard_size]->exist(key)) caches[hashed % FLAGS_db_cache_shard_size]->erase(key);
    }
    auto s3 = utils::Basic::getNowMillSec();
    LOG_IF(INFO, (s2 - s1 > 2000) || (s3 - s2 > 2000)) << "[familydb] write batch spend step1:" << (s2 - s1) << " step2:" << (s3 - s2);
    return error::Error();
}

void chakra::database::ColumnDB::cacheClear() { 
    for (auto& c : caches) {
        c->clear();
    }
}

chakra::database::ColumnDB::~ColumnDB() {
    if (self) {
        self->Close();
    }
    if (full) {
        full->Close();
    }
}
