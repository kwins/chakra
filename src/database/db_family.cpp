//
// Created by kwins on 2021/6/9.
//

#include "db_family.h"
#include "utils/basic.h"
#include <glog/logging.h>
#include <nlohmann/json.hpp>
#include <fstream>
#include <zlib.h>
#include "utils/file_helper.h"
#include <gflags/gflags.h>
#include "peer.pb.h"
#include "error/err_file_not_exist.h"

DECLARE_string(cluster_dir);
DECLARE_string(db_dir);
DECLARE_string(db_restore_dir);
DECLARE_string(db_backup_dir);
DECLARE_int32(db_block_size);
DECLARE_int64(db_wal_ttl_seconds);

chakra::database::FamilyDB::FamilyDB() {
    proto::peer::MetaDBs metaDBs;
    std::string filename = FLAGS_cluster_dir + "/" + DB_FILE;
    try {
        utils::FileHelper::loadFile(filename, metaDBs);
        for(auto& info : metaDBs.dbs()){
            auto bucket = std::make_shared<BucketDB>(info);
            columnBuckets[index.load()].emplace(std::make_pair(info.name(), bucket));
        }
    } catch (error::FileNotExistError& err) {
        return;
    } catch (std::exception& err) {
        LOG(INFO) << "load db from" << filename << " error " << err.what();
        exit(-1);
    }
}

chakra::error::Error chakra::database::FamilyDB::getMetaDB(const std::string &dbname, proto::peer::MetaDB &meta) {
    int pos = index.load();
    auto it = columnBuckets[pos].find(dbname);
    if (it == columnBuckets[pos].end()){
        return error::Error("db " + dbname + " not found");
    }
    meta.CopyFrom(it->second->getMetaDB(dbname));
    return chakra::error::Error();
}

std::shared_ptr<chakra::database::FamilyDB>& chakra::database::FamilyDB::get() {
    static auto familyptr = std::make_shared<chakra::database::FamilyDB>();
    return familyptr;
}

void chakra::database::FamilyDB::addDB(const std::string &name, size_t cached) { addDB(name, FLAGS_db_block_size, cached); }

void chakra::database::FamilyDB::addDB(const std::string &name, size_t blockSize, size_t cached) {
    if (servedDB(name)) return;

    int next = (index == 0 ? 1 : 0);
    columnBuckets[next].clear();
    for(auto & bucket : columnBuckets[index.load()]){
        columnBuckets[next].emplace(std::make_pair(bucket.first, bucket.second));
    }

    // 添加新的 bucket db
    proto::peer::MetaDB meta;
    meta.set_name(name);
    meta.set_cached(cached);
    meta.set_memory(false);
    auto bucket = std::make_shared<BucketDB>(meta);
    columnBuckets[next].emplace(std::make_pair(name, bucket));
    index = next;
    LOG(INFO) << "add db " << name << " success.";
}

bool chakra::database::FamilyDB::servedDB(const std::string &name) {
    return columnBuckets[index.load()].count(name) > 0;
}

void chakra::database::FamilyDB::dropDB(const std::string &name) {
    int next = index == 0 ? 1 : 0;
    columnBuckets[next].clear();
    for(auto & bucket : columnBuckets[index]){
        if (bucket.first != name){
            columnBuckets[next].emplace(std::make_pair(bucket.first, bucket.second));
        }
    }
    index = next;
    LOG(INFO) << "drop column name " << name << " success.";
}

std::shared_ptr<chakra::database::Element> chakra::database::FamilyDB::get(const std::string& name, const std::string &key) {
    int pos = index.load();
    auto it = columnBuckets[pos].find(name);
    if (it == columnBuckets[pos].end()){
        return nullptr;
    }
    return it->second->get(key);
}

void
chakra::database::FamilyDB::put(const std::string& name, const std::string &key, std::shared_ptr<Element> val) {
    int pos = index.load();
    auto it = columnBuckets[pos].find(name);
    if (it == columnBuckets[pos].end()){
        return;
    }
    it->second->put(key, val);
}

chakra::error::Error chakra::database::FamilyDB::putAll(const std::string& name, rocksdb::WriteBatch &batch) {
    int pos = index.load();
    auto it = columnBuckets[pos].find(name);
    if (it == columnBuckets[pos].end()){
        return error::Error("db " + name + " not found");
    }
    return it->second->putAll(batch);
}

chakra::error::Error
chakra::database::FamilyDB::putAll(const std::string &name, const std::string &key, const std::string &value) {
    int pos = index.load();
    auto it = columnBuckets[pos].find(name);
    if (it == columnBuckets[pos].end()){
        return error::Error("db " + name + " not found");
    }
    return it->second->putAll(key, value);
}

chakra::error::Error
chakra::database::FamilyDB::putAll(const std::string &name, const rocksdb::Slice &key, const rocksdb::Slice &value) {
    int pos = index.load();
    auto it = columnBuckets[pos].find(name);
    if (it == columnBuckets[pos].end()){
        return error::Error("db " + name + " not found");
    }
    return it->second->putAll(key, value);
}

void chakra::database::FamilyDB::del(const std::string &name, const std::string &key) {
    int pos = index.load();
    auto it = columnBuckets[pos].find(name);
    if (it == columnBuckets[pos].end()){
        return;
    }
    it->second->del(key);
}

size_t chakra::database::FamilyDB::dbSize(const std::string &name) {
    int pos = index;
    auto it = columnBuckets[pos].find(name);
    if (it == columnBuckets[pos].end()){
        return 0;
    }
    return it->second->size();
}

chakra::error::Error chakra::database::FamilyDB::restoreDB(const std::string &name) {
    int pos = index;
    auto it = columnBuckets[pos].find(name);
    if (it == columnBuckets[pos].end()){
        return error::Error("db " + name + " not found");
    }

    return it->second->restoreDB();
}

chakra::database::FamilyDB::RestoreDB chakra::database::FamilyDB::getLastRestoreDB() { return lastRestore; }

chakra::error::Error
chakra::database::FamilyDB::getLastSeqNumber(const std::string &name, rocksdb::SequenceNumber &seq) {
    int pos = index.load();
    auto it = columnBuckets[pos].find(name);
    if (it == columnBuckets[pos].end()){
        return error::Error("db " + name + " not found");
    }
    seq = it->second->getLastSeqNumber();
    return error::Error();
}

chakra::error::Error
chakra::database::FamilyDB::getUpdateSince(const std::string &name, rocksdb::SequenceNumber seq,
                                           std::unique_ptr<rocksdb::TransactionLogIterator> *iter) {
    int pos = index.load();
    auto it = columnBuckets[pos].find(name);
    if (it == columnBuckets[pos].end()){
        return error::Error("db " + name + " not found");
    }

    return it->second->getUpdateSince(seq, iter);
}

chakra::error::Error
chakra::database::FamilyDB::snapshot(const std::string &name, rocksdb::Iterator **iter, rocksdb::SequenceNumber &seq) {
    int pos = index.load();
    auto it = columnBuckets[pos].find(name);
    if (it == columnBuckets[pos].end()){
        return error::Error("db " + name + " not found");
    }
    return it->second->snapshot(iter, seq);
}

void chakra::database::FamilyDB::stop() {
    columnBuckets[0].clear();
    columnBuckets[1].clear();
}

const std::string chakra::database::FamilyDB::DB_FILE = "dbs.json";