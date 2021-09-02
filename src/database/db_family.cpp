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

void chakra::database::FamilyDB::initFamilyDB(const chakra::database::FamilyDB::Options &familyOpts) {
    opts = familyOpts;
    nlohmann::json j;
    std::string filename = opts.dir + "/" + DB_FILE;

    auto err = utils::FileHelper::loadFile(filename, j);
    if (err){
        LOG(ERROR) << "DB load dbs config from " << filename << " :" << strerror(errno);
        return;
    }

    LOG(INFO) << "DB load dbs from config file " << filename;
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
        columnBuckets[index].emplace(std::make_pair(name, bucket));
    }
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

    LOG(INFO) << "RocksDB drop column name " << name << " success.";
}

std::shared_ptr<chakra::database::Element> chakra::database::FamilyDB::get(const std::string& name, const std::string &key) {
    int pos = index;
    auto it = columnBuckets[pos].find(name);
    if (it == columnBuckets[pos].end()){
        return nullptr;
    }
    return it->second->get(key);
}

void
chakra::database::FamilyDB::put(const std::string& name, const std::string &key, std::shared_ptr<Element> val) {
    int pos = index;
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

std::shared_ptr<chakra::database::FamilyDB> chakra::database::FamilyDB::get() {
    static auto familyDBPtr = std::make_shared<FamilyDB>();
    return familyDBPtr;
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
    std::string tmpfile = filename + ".tmp";
    std::ofstream out(tmpfile, std::ios::out|std::ios::trunc);
    if (!out.is_open()){
        LOG(ERROR) << "DB dump dbs conf open file " << strerror(errno);
        return;
    }

    nlohmann::json j;
    for (auto& it : columnBuckets[index]){
        j["dbs"].push_back(it.second->dumpDB());
    }
    out << j.dump(4);
    if (::rename(tmpfile.c_str(), filename.c_str()) == -1){
        LOG(ERROR) << "DB dump dbs error " << strerror(errno);
        return;
    }
    out.close();
    LOG(INFO) << "DB dump dbs to filename " << filename << " success.";
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

const std::string  chakra::database::FamilyDB::DB_FILE = "dbs.json";