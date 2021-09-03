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

std::shared_ptr<chakra::database::FamilyDB> chakra::database::FamilyDB::get() {
    static auto dbptr = std::make_shared<FamilyDB>();
    return dbptr;
}

void chakra::database::FamilyDB::initFamilyDB(const chakra::database::FamilyDB::Options &familyOpts) {
    opts = familyOpts;
    nlohmann::json j;
    std::string filename = opts.dir + "/" + DB_FILE;
    auto err = utils::FileHelper::loadFile(filename, j);
    if (!err.success()){
        if (!err.is(utils::Error::ERR_FILE_NOT_EXIST)){
            LOG(ERROR) << "DB load dbs config from " << filename << " :" << strerror(errno);
            exit(-1);
        }
        return;
    }
    index = 0;
    auto dbs = j.at("dbs");
    for (int i = 0; i < dbs.size(); ++i) {
        auto name = dbs[i].at("name").get<std::string>();
        auto cached = dbs[i].at("cached").get<int64_t>();
        auto blockSize = dbs[i].at("block_size").get<int64_t>();
        BucketDB::Options bucketOpts;
        bucketOpts.dir = opts.dir;
        bucketOpts.name = name;
        bucketOpts.blockCapaticy = cached;
        bucketOpts.blocktSize = blockSize;
        auto bucket = std::make_shared<BucketDB>(bucketOpts);
        columnBuckets[index.load()].emplace(std::make_pair(name, bucket));
    }
    LOG(INFO) << "Load DB config from " << filename << " success.";
}

void chakra::database::FamilyDB::addDB(const std::string &name) { addDB(name, opts.blockSize, opts.blockCapacity); }

void chakra::database::FamilyDB::addDB(const std::string &name, size_t blockSize, size_t blocktCapacity) {
    if (servedDB(name)) return;

    int next = index == 0 ? 1 : 0;
    columnBuckets[next].clear();
    for(auto & bucket : columnBuckets[index]){
        columnBuckets[next].emplace(std::make_pair(bucket.first, bucket.second));
    }

    // 添加新的 bucket
    BucketDB::Options bucketOpts;
    bucketOpts.dir = opts.dir;
    bucketOpts.name = name;
    bucketOpts.blocktSize = blockSize;
    bucketOpts.blockCapaticy = blocktCapacity;
    auto bucket = std::make_shared<BucketDB>(bucketOpts);
    columnBuckets[next].emplace(std::make_pair(name, bucket));
    index = next;
    dumpDBsFile();
    LOG(INFO) << "RocksDB add db " << name << " success.";
}

bool chakra::database::FamilyDB::servedDB(const std::string &name) {
    return columnBuckets[index].count(name) > 0;
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
    std::string filename = opts.dir + "/" + DB_FILE;
    nlohmann::json j;
    for (auto& it : columnBuckets[index.load()]){
        j["dbs"].push_back(it.second->dumpDB());
    }

    auto err = utils::FileHelper::saveFile(j, filename);
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

    // backup
    rocksdb::BackupEngine* backupEngine;
    std::string backupdir = opts.backUpDir + "/" + name;
    auto s = rocksdb::BackupEngine::Open(rocksdb::Env::Default(), rocksdb::BackupableDBOptions(backupdir), &backupEngine);
    if (!s.ok()){
        return utils::Error(1, s.ToString());
    }
    s = backupEngine->CreateNewBackup(it->second->getRocksDB().get());
    if (!s.ok()){
        return utils::Error(1, s.ToString());
    }
    delete backupEngine;

    // restore
    rocksdb::BackupEngineReadOnly* restore;
    std::string restoredir = opts.restoreDir + "/" + name;
    s = rocksdb::BackupEngineReadOnly::Open(rocksdb::Env::Default(),rocksdb::BackupableDBOptions(restoredir), &restore);
    if (!s.ok()){
        return utils::Error(1, s.ToString());
    }
    s = restore->RestoreDBFromLatestBackup(restoredir, restoredir);
    if (!s.ok()){
        return utils::Error(1, s.ToString());
    }
    delete restore;
    lastRestore.seq = it->second->getRocksDB()->GetLatestSequenceNumber();

    // 压缩
    return utils::Error();
}

chakra::database::FamilyDB::RestoreDB chakra::database::FamilyDB::getLastRestoreDB() { return lastRestore; }

chakra::utils::Error
chakra::database::FamilyDB::getLastSeqNumber(const std::string &name, rocksdb::SequenceNumber &seq) {
    int pos = index;
    auto it = columnBuckets[pos].find(name);
    if (it == columnBuckets[pos].end()){
        return utils::Error(1, "DB " + name + " not found");
    }
    seq = it->second->getRocksDB()->GetLatestSequenceNumber();
    return utils::Error();
}

chakra::utils::Error
chakra::database::FamilyDB::fetch(const std::string &name, rocksdb::SequenceNumber seq,
                                                       std::unique_ptr<rocksdb::TransactionLogIterator> *iter) {
    int pos = index.load();
    auto it = columnBuckets[pos].find(name);
    if (it == columnBuckets[pos].end()){
        return utils::Error(1, "DB " + name + " not found");
    }
    auto s = it->second->getRocksDB()->GetUpdatesSince(seq, iter);
    if (!s.ok()){
        return utils::Error(1,s.ToString());
    }
    return utils::Error();
}

chakra::utils::Error
chakra::database::FamilyDB::snapshot(const std::string &name, rocksdb::Iterator **iter, rocksdb::SequenceNumber &seq) {
    int pos = index.load();
    auto it = columnBuckets[pos].find(name);
    if (it == columnBuckets[pos].end()){
        return utils::Error(1, "DB " + name + " not found");
    }

    auto snapshot = it->second->getRocksDB()->GetSnapshot();
    rocksdb::ReadOptions readOptions;
    readOptions.snapshot = snapshot;
    seq = snapshot->GetSequenceNumber();
    (*iter) = it->second->getRocksDB()->NewIterator(readOptions);
    return utils::Error();
}

chakra::database::FamilyDB::~FamilyDB() {
    LOG(INFO) << "~FamilyDB";
}

const std::string  chakra::database::FamilyDB::DB_FILE = "dbs.json";