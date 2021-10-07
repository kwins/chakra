//
// Created by kwins on 2021/6/9.
//

#include "db_bucket.h"
#include <zlib.h>
#include <rocksdb/db.h>
#include <glog/logging.h>
#include <rocksdb/utilities/backupable_db.h>
#include <gflags/gflags.h>

DECLARE_int64(db_wal_ttl_seconds);
DECLARE_string(db_dir);
DECLARE_int32(db_block_size);

chakra::database::BucketDB::BucketDB(const proto::peer::MetaDB& meta) {
    metaDB = meta;
    LOG(INFO) << "bucket db meta " << metaDB.DebugString();
    rocksdb::Options rocksOpts;
    rocksOpts.keep_log_file_num = 5;
    rocksOpts.create_if_missing = true;
    rocksOpts.WAL_ttl_seconds = FLAGS_db_wal_ttl_seconds;
    rocksdb::DB* dbself;
    auto s = rocksdb::DB::Open(rocksOpts, FLAGS_db_dir + "/" + metaDB.name() + ".self", &dbself);
    if (!s.ok()){
        throw std::logic_error(s.ToString());
    }
    self = std::shared_ptr<rocksdb::DB>(dbself);
    rocksdb::DB* db;
    s = rocksdb::DB::Open(rocksOpts, FLAGS_db_dir + "/" + metaDB.name(), &db);
    if (!s.ok()){
        throw std::logic_error(s.ToString());
    }
    dbptr = std::shared_ptr<rocksdb::DB>(db);
    blocks.resize(FLAGS_db_block_size);
    for (int i = 0; i < FLAGS_db_block_size; ++i) {
        blocks[i] = std::make_shared<BlockDB>(metaDB.cached());
    }

    // 加载一些热数据
    uint64_t num = 0;
    auto iter = self->NewIterator(rocksdb::ReadOptions());
    for(iter->SeekToLast(); iter->Valid(); iter->Prev()){
        auto element = std::make_shared<Element>();
        element->deSeralize(iter->value().data(), iter->value().size());
        element->setUpdated(false);
        put(iter->key().ToString(), element, false);
        if (++num >= metaDB.cached() * 0.5){
            break;
        }
    }
    delete iter;
}

std::shared_ptr<chakra::database::Element>
chakra::database::BucketDB::get(const std::string &key) {
    unsigned long hashed = ::crc32(0L, (unsigned char*)key.data(), key.size());
    return blocks.at(hashed % FLAGS_db_block_size)->get(key, [this](const std::string& key, std::string& value)->bool{
        auto s = dbptr->Get(rocksdb::ReadOptions(), key, &value);
        if (!s.ok()){
            if (!s.IsNotFound())
                LOG(ERROR) << "DB get error " << s.ToString();
            return false;
        }
        return true;
    });
}

void chakra::database::BucketDB::put(const std::string &key, std::shared_ptr<Element> val, bool dbput) {
    // TODO: 找一个支持多语言的hash函数
    unsigned long hashed = ::crc32(0L, (unsigned char*)key.data(), key.size());
    blocks[hashed % FLAGS_db_block_size]->put(key, val);
    if (dbput){
        val->serialize([this, &key](char* data, size_t len){
            auto s = self->Put(rocksdb::WriteOptions(), key, rocksdb::Slice(data, len));
            if (!s.ok()) LOG(ERROR) << "DB put error " << s.ToString();
            s = dbptr->Put(rocksdb::WriteOptions(), key, rocksdb::Slice(data, len));
            if (!s.ok()) LOG(ERROR) << "DB put error " << s.ToString();
        });
    }
}

chakra::utils::Error chakra::database::BucketDB::put(rocksdb::WriteBatch& batch) {
    auto s = batch.Iterate(this);
    if (!s.ok()){
        return utils::Error(utils::Error::ERR_DB_ITERATOR, s.ToString());
    }
    return utils::Error();
}

void chakra::database::BucketDB::del(const std::string &key, bool dbdel) {
    unsigned long hashed = ::crc32(0L, (unsigned char*)key.data(), key.size());
    blocks[hashed % FLAGS_db_block_size]->del(key);
    if (dbdel){
        auto s = self->Delete(rocksdb::WriteOptions(), key);
        if (!s.ok()) LOG(ERROR) << "DB del error " << s.ToString();
    }
}

size_t chakra::database::BucketDB::size() {
    size_t num = 0;
    for(auto & block : blocks){
        num += block->size();
    }
    return num;
}

proto::peer::MetaDB chakra::database::BucketDB::getMetaDB(const std::string &dbname) { return metaDB; }

chakra::utils::Error chakra::database::BucketDB::restoreDB() {
    // backup
    rocksdb::BackupEngine* backupEngine;
    std::string backupdir = FLAGS_db_dir + "/" + metaDB.name();
    auto s = rocksdb::BackupEngine::Open(rocksdb::Env::Default(), rocksdb::BackupableDBOptions(backupdir), &backupEngine);
    if (!s.ok()){
        return utils::Error(1, s.ToString());
    }
    s = backupEngine->CreateNewBackup(self.get());
    if (!s.ok()){
        return utils::Error(1, s.ToString());
    }
    delete backupEngine;

    // restore
    rocksdb::BackupEngineReadOnly* restore;
    std::string restoredir = FLAGS_db_dir + "/" + metaDB.name() + ".backup";
    s = rocksdb::BackupEngineReadOnly::Open(rocksdb::Env::Default(),rocksdb::BackupableDBOptions(restoredir), &restore);
    if (!s.ok()){
        return utils::Error(1, s.ToString());
    }
    s = restore->RestoreDBFromLatestBackup(restoredir, restoredir);
    if (!s.ok()){
        return utils::Error(1, s.ToString());
    }
    delete restore;
    lastRestore.seq = self->GetLatestSequenceNumber();
    return utils::Error();
}

chakra::database::BucketDB::RestoreDB chakra::database::BucketDB::getLastRestoreDB() {
    return chakra::database::BucketDB::RestoreDB();
}

chakra::utils::Error chakra::database::BucketDB::getUpdateSince(rocksdb::SequenceNumber seq,
                                                                std::unique_ptr<rocksdb::TransactionLogIterator> *iter) {
    auto s = self->GetUpdatesSince(seq, iter);
    if (!s.ok()){
        return utils::Error(s.code(), s.ToString());
    }
    return utils::Error();
}

chakra::utils::Error
chakra::database::BucketDB::snapshot(rocksdb::Iterator **iter, rocksdb::SequenceNumber &seq) {
    auto snapshot = self->GetSnapshot();
    rocksdb::ReadOptions readOptions;
    readOptions.snapshot = snapshot;
    seq = snapshot->GetSequenceNumber();
    (*iter) = self->NewIterator(readOptions);
    return utils::Error();
}

rocksdb::SequenceNumber chakra::database::BucketDB::getLastSeqNumber() {
    return self->GetLatestSequenceNumber();
}

chakra::database::BucketDB::~BucketDB() {
    LOG(INFO) << "~BucketDB 1";
    if (self){
        // TODO: it will generate error like pthread lock: Invalid argument error
        //      when use static pointer hold this bucket object
        //      why?
        self->Close();
    }

    if (dbptr){
        dbptr->Close();
    }
}

void chakra::database::BucketDB::Put(const rocksdb::Slice &key, const rocksdb::Slice &value) {
    auto s = dbptr->Put(rocksdb::WriteOptions(), key, value);
    if (!s.ok()) LOG(ERROR) << "DB delta put error " << s.ToString();
    // 淘汰缓存，下次read重新加载
    unsigned long hashed = ::crc32(0L, (unsigned char*)key.data(), key.size());
    blocks[hashed % FLAGS_db_block_size]->del(key.ToString());
}

void chakra::database::BucketDB::Delete(const rocksdb::Slice &key) {
    auto s = dbptr->Delete(rocksdb::WriteOptions(), key);
    if (!s.ok()) LOG(ERROR) << "DB delta del error " << s.ToString();
    // 淘汰缓存，下次read重新加载
    unsigned long hashed = ::crc32(0L, (unsigned char*)key.data(), key.size());
    blocks[hashed % FLAGS_db_block_size]->del(key.ToString());
}
