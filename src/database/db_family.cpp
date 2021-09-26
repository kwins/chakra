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

DEFINE_string(db_dir, "data", "rocksdb save dir");                                      /* NOLINT */
DEFINE_string(db_restore_dir, "data", "rocksdb restore dir");                           /* NOLINT */
DEFINE_string(db_backup_dir, "data", "rocksdb backup dir");                             /* NOLINT */
DEFINE_int32(db_block_size, 256, "rocksdb cached block size");                          /* NOLINT */

chakra::database::FamilyDB::FamilyDB() {
    auto err = utils::FileHelper::mkDir(FLAGS_db_dir);
    if (!err.success()){
        LOG(ERROR) << "FamilyDB mkdir dir error " << err.toString();
        exit(-1);
    }

    proto::peer::DBState dbState;
    std::string filename = FLAGS_db_dir + "/" + DB_FILE;
    err = utils::FileHelper::loadFile(filename, dbState);
    if (!err.success()){
        if (!err.is(utils::Error::ERR_FILE_NOT_EXIST)){
            LOG(INFO) << "FamilyDB load " << filename << " error " << err.toString();
            exit(-1);
        }
        return;
    }

    for(auto& info : dbState.dbs()){
        BucketDB::Options bucketOpts;
        bucketOpts.dir = FLAGS_db_dir;
        bucketOpts.name = info.name();
        bucketOpts.flag = info.flag();
        bucketOpts.cached = info.cached();
        bucketOpts.blockSize = FLAGS_db_block_size;
        auto bucket = std::make_shared<BucketDB>(bucketOpts);
        columnBuckets[index.load()].emplace(std::make_pair(info.name(), bucket));
    }
}

chakra::database::FamilyDB &chakra::database::FamilyDB::get() {
    static chakra::database::FamilyDB familyDb;
    return familyDb;
}

void chakra::database::FamilyDB::addDB(const std::string &name, size_t cached) { addDB(name, FLAGS_db_block_size, cached); }

void chakra::database::FamilyDB::addDB(const std::string &name, size_t blockSize, size_t cached) {
    if (servedDB(name)) return;

    int next = (index == 0 ? 1 : 0);
    columnBuckets[next].clear();
    for(auto & bucket : columnBuckets[index.load()]){
        columnBuckets[next].emplace(std::make_pair(bucket.first, bucket.second));
    }

    // 添加新的 bucket
    BucketDB::Options bucketOpts;
    bucketOpts.dir = FLAGS_db_dir;
    bucketOpts.name = name;
    bucketOpts.blockSize = blockSize;
    bucketOpts.cached = cached;
    auto bucket = std::make_shared<BucketDB>(bucketOpts);
    columnBuckets[next].emplace(std::make_pair(name, bucket));
    index = next;
    dumpDBsFile();
    LOG(INFO) << "RocksDB add db " << name << " success.";
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
    dumpDBsFile();
    LOG(INFO) << "RocksDB drop column name " << name << " success.";
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

void chakra::database::FamilyDB::put(const std::string& name, rocksdb::WriteBatch &batch) {
    int pos = index.load();
    auto it = columnBuckets[pos].find(name);
    if (it == columnBuckets[pos].end()){
        return;
    }
    it->second->put(batch);
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

void chakra::database::FamilyDB::dumpDBsFile() const {
    std::string filename = FLAGS_db_dir + "/" + DB_FILE;
    proto::peer::DBState dbState;
    for (auto& it : columnBuckets[index.load()]){
        auto info = dbState.mutable_dbs()->Add();
        it.second->dumpDB(*info);
    }

    auto err = utils::FileHelper::saveFile(dbState, filename);
    if (!err.success()){
        LOG(ERROR) << "DB dump dbs error " << strerror(errno);
    } else {
        LOG(INFO) << "DB dump dbs to filename " << filename << " success.";
    }
}

chakra::utils::Error chakra::database::FamilyDB::restoreDB(const std::string &name) {
    int pos = index;
    auto it = columnBuckets[pos].find(name);
    if (it == columnBuckets[pos].end()){
        return utils::Error(1, "DB " + name + " not found");
    }

    return it->second->restoreDB();
}

chakra::database::FamilyDB::RestoreDB chakra::database::FamilyDB::getLastRestoreDB() { return lastRestore; }

chakra::utils::Error
chakra::database::FamilyDB::getLastSeqNumber(const std::string &name, rocksdb::SequenceNumber &seq) {
    int pos = index.load();
    auto it = columnBuckets[pos].find(name);
    if (it == columnBuckets[pos].end()){
        return utils::Error(1, "DB " + name + " not found");
    }
    seq = it->second->getLastSeqNumber();
    return utils::Error();
}

chakra::utils::Error
chakra::database::FamilyDB::getUpdateSince(const std::string &name, rocksdb::SequenceNumber seq,
                                           std::unique_ptr<rocksdb::TransactionLogIterator> *iter) {
    int pos = index.load();
    auto it = columnBuckets[pos].find(name);
    if (it == columnBuckets[pos].end()){
        return utils::Error(1, "DB " + name + " not found");
    }

    return it->second->getUpdateSince(seq, iter);
}

chakra::utils::Error
chakra::database::FamilyDB::snapshot(const std::string &name, rocksdb::Iterator **iter, rocksdb::SequenceNumber &seq) {
    int pos = index.load();
    auto it = columnBuckets[pos].find(name);
    if (it == columnBuckets[pos].end()){
        return utils::Error(1, "DB " + name + " not found");
    }
    return it->second->snapshot(iter, seq);
}

chakra::database::FamilyDB::~FamilyDB() {
    LOG(INFO) << "~FamilyDB";
}

const std::string chakra::database::FamilyDB::DB_FILE = "dbs.json";