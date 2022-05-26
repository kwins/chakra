//
// Created by kwins on 2021/6/9.
//

#include "db_family.h"
#include "utils/basic.h"
#include <database/db_column.h>
#include <element.pb.h>
#include <glog/logging.h>
#include <fstream>
#include <zlib.h>
#include "utils/file_helper.h"
#include <gflags/gflags.h>
#include "peer.pb.h"
#include "error/err.h"

DECLARE_string(cluster_dir);
DECLARE_string(db_dir);
DECLARE_string(db_restore_dir);
DECLARE_string(db_backup_dir);
DECLARE_int32(db_cache_shard_size);
DECLARE_int64(db_wal_ttl_seconds);

chakra::database::FamilyDB::FamilyDB() : index(0) {
    LOG(INFO) << "FamilyDB init";
    proto::peer::MetaDBs metaDBs;
    std::string filename = FLAGS_cluster_dir + "/" + DB_FILE;
    try {
        utils::FileHelper::loadFile(filename, metaDBs);
        for(auto& info : metaDBs.dbs()) {
            auto bucket = std::make_shared<ColumnDB>(info);
            columnDBs[index.load()].emplace(std::make_pair(info.name(), bucket));
        }
    } catch (const error::FileError& err) {
        /* 文件不存在或者首次运行 */
    } catch (const std::exception& err) {
        LOG(INFO) << "FamilyDB load db from" << filename << " error " << err.what();
        exit(-1);
    }
    LOG(INFO) << "FamilyDB success";
}

chakra::error::Error chakra::database::FamilyDB::getMetaDB(const std::string &dbname, proto::peer::MetaDB &meta) {
    int pos = index.load();
    auto it = columnDBs[pos].find(dbname);
    if (it == columnDBs[pos].end()){
        return error::Error("db " + dbname + " not found");
    }
    meta.CopyFrom(it->second->getMetaDB(dbname));
    return chakra::error::Error();
}

std::shared_ptr<chakra::database::FamilyDB>& chakra::database::FamilyDB::get() {
    static auto familyptr = std::make_shared<chakra::database::FamilyDB>();
    return familyptr;
}

void chakra::database::FamilyDB::addDB(const std::string &name, size_t cached) { addDB(name, FLAGS_db_cache_shard_size, cached); }

void chakra::database::FamilyDB::addDB(const std::string &name, size_t blockSize, size_t cached) {
    if (servedDB(name)) return;

    int next = (index == 0 ? 1 : 0);
    columnDBs[next].clear();
    for(auto & bucket : columnDBs[index.load()]){
        columnDBs[next].emplace(std::make_pair(bucket.first, bucket.second));
    }

    // 添加新的 bucket db
    proto::peer::MetaDB meta;
    meta.set_name(name);
    meta.set_cached(cached);
    meta.set_memory(false);
    auto columnDB = std::make_shared<ColumnDB>(meta);
    columnDBs[next].emplace(std::make_pair(name, columnDB));
    index = next;
    LOG(INFO) << "Add db " << name << " success.";
}

bool chakra::database::FamilyDB::servedDB(const std::string &name) {
    return columnDBs[index.load()].count(name) > 0;
}

void chakra::database::FamilyDB::dropDB(const std::string &name) {
    int next = index == 0 ? 1 : 0;
    columnDBs[next].clear();
    for(auto & bucket : columnDBs[index]){
        if (bucket.first != name){
            columnDBs[next].emplace(std::make_pair(bucket.first, bucket.second));
        }
    }
    index = next;
    LOG(INFO) << "Drop db " << name << " success.";
}

std::shared_ptr<proto::element::Element> chakra::database::FamilyDB::get(const std::string& name, const std::string &key) {
    int pos = index.load();
    auto it = columnDBs[pos].find(name);
    if (it == columnDBs[pos].end()) {
        LOG(INFO) << "db:" << name << " not found";
        return nullptr;
    }
    return it->second->get(key);
}

std::unordered_map<std::string, std::vector<std::shared_ptr<proto::element::Element>>> chakra::database::FamilyDB::mget(std::unordered_map<std::string,std::vector<std::string>> dbkeys) {
    std::unordered_map<std::string,std::vector<std::shared_ptr<proto::element::Element>>> result;
    int pos = index.load();
    for(auto& keys : dbkeys) {
        auto it = columnDBs[pos].find(keys.first);
        if (it == columnDBs[pos].end()) {
            continue;
        }
        result[keys.first] = it->second->mget(keys.second);
    }
    return result;
}

chakra::error::Error chakra::database::FamilyDB::set(const std::string& name, const std::string& key, const std::string& value, int64_t ttl) {
    int pos = index.load();
    auto it = columnDBs[pos].find(name);
    if (it == columnDBs[pos].end()) {
        return dbnf(name);
    }
    return it->second->set(key, value, ttl);
}

chakra::error::Error chakra::database::FamilyDB::set(const std::string& name, const std::string& key, float value, int64_t ttl) {
    int pos = index.load();
    auto it = columnDBs[pos].find(name);
    if (it == columnDBs[pos].end()) {
        return dbnf(name);
    }
    return it->second->set(key, value, ttl);
}

chakra::error::Error chakra::database::FamilyDB::push(const std::string& name, const std::string& key, const std::vector<std::string>& values, int64_t ttl) {
    int pos = index.load();
    auto it = columnDBs[pos].find(name);
    if (it == columnDBs[pos].end()) {
        return dbnf(name);;
    }
    return it->second->push(key, values, ttl);    
}

chakra::error::Error chakra::database::FamilyDB::push(const std::string& name, const std::string& key, const std::vector<float>& values, int64_t ttl) {
    int pos = index.load();
    auto it = columnDBs[pos].find(name);
    if (it == columnDBs[pos].end()) {
        return dbnf(name);;
    }
    return it->second->push(key, values, ttl);    
}

chakra::error::Error chakra::database::FamilyDB::incr(const std::string& name, const std::string& key, float value, int64_t ttl) {
    int pos = index.load();
    auto it = columnDBs[pos].find(name);
    if (it == columnDBs[pos].end()) {
        return dbnf(name);
    }
    return it->second->incr(key, value, ttl); 
}

void chakra::database::FamilyDB::erase(const std::string& name, const std::string& key) {
    int pos = index.load();
    auto it = columnDBs[pos].find(name);
    if (it == columnDBs[pos].end()) {
        return;
    }
    it->second->erase(key);
}

chakra::error::Error chakra::database::FamilyDB::rocksWriteBulk(const std::string& name, rocksdb::WriteBatch &batch) {
    int pos = index.load();
    auto it = columnDBs[pos].find(name);
    if (it == columnDBs[pos].end()){
        return dbnf(name);
    }
    return it->second->rocksWriteBulk(batch);
}

size_t chakra::database::FamilyDB::dbSize(const std::string &name) {
    int pos = index;
    auto it = columnDBs[pos].find(name);
    if (it == columnDBs[pos].end()){
        return 0;
    }
    return it->second->size();
}

chakra::error::Error chakra::database::FamilyDB::restoreDB(const std::string &name) {
    int pos = index;
    auto it = columnDBs[pos].find(name);
    if (it == columnDBs[pos].end()){
        return dbnf(name);
    }

    return it->second->restoreDB();
}

chakra::database::FamilyDB::RestoreDB chakra::database::FamilyDB::getLastRestoreDB() { return lastRestore; }

chakra::error::Error
chakra::database::FamilyDB::getLastSeqNumber(const std::string &name, rocksdb::SequenceNumber &seq) {
    int pos = index.load();
    auto it = columnDBs[pos].find(name);
    if (it == columnDBs[pos].end()){
        return dbnf(name);
    }
    seq = it->second->getLastSeqNumber();
    return error::Error();
}

chakra::error::Error
chakra::database::FamilyDB::getUpdateSince(const std::string &name, rocksdb::SequenceNumber seq,
                                           std::unique_ptr<rocksdb::TransactionLogIterator> *iter) {
    int pos = index.load();
    auto it = columnDBs[pos].find(name);
    if (it == columnDBs[pos].end()){
        return dbnf(name);
    }

    return it->second->getUpdateSince(seq, iter);
}

chakra::error::Error
chakra::database::FamilyDB::snapshot(const std::string &name, rocksdb::Iterator **iter, rocksdb::SequenceNumber &seq) {
    int pos = index.load();
    auto it = columnDBs[pos].find(name);
    if (it == columnDBs[pos].end()){
        return dbnf(name);
    }
    return it->second->snapshot(iter, seq);
}

void chakra::database::FamilyDB::stop() {
    columnDBs[0].clear();
    columnDBs[1].clear();
}

chakra::error::Error chakra::database::FamilyDB::dbnf(const std::string& str) { return error::Error("db:" + str + " not found"); }
const std::string chakra::database::FamilyDB::DB_FILE = "dbs.json";