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
    caches.resize(FLAGS_db_block_size);
    for (int i = 0; i < FLAGS_db_block_size; ++i) {
        caches[i] = std::make_shared<Cache>(metaDB.cached());
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
    LOG(INFO) << "db " << meta.name() << " preheat cache size " << num;
    delete iter;
    LOG(INFO) << "bucket end";
}

std::shared_ptr<chakra::database::Element>
chakra::database::BucketDB::get(const std::string &key) {
    unsigned long hashed = ::crc32(0L, (unsigned char*)key.data(), key.size());
    int i = hashed % FLAGS_db_block_size;
    std::shared_ptr<chakra::database::Element> val = nullptr;
    if (!caches[i]->get(key, val)){
        std::string oriVal;
        auto s = dbptr->Get(rocksdb::ReadOptions(), key, &oriVal);
        if (s.ok()){
            val = std::make_shared<Element>();
            val->deSeralize(oriVal.data(), oriVal.size());
            caches[i]->put(key, val);
        }
    }
    return val;
}

void chakra::database::BucketDB::put(const std::string &key, std::shared_ptr<Element> val, bool dbput) {
    unsigned long hashed = ::crc32(0L, (unsigned char*)key.data(), key.size());
    caches[hashed % FLAGS_db_block_size]->put(key, val);
    if (dbput){
        val->serialize([this, &key](char* data, size_t len){
            auto s = self->Put(rocksdb::WriteOptions(), key, rocksdb::Slice(data, len));
            if (!s.ok()) LOG(ERROR) << "db put error " << s.ToString();
        });
    }
}

// 增量同步过来的数据，需要写入到全量的rocksdb中，并清除缓存
chakra::error::Error chakra::database::BucketDB::putAll(rocksdb::WriteBatch& batch) {
    auto s = batch.Iterate(this);
    if (!s.ok()){
        return error::Error(s.ToString());
    }
    return error::Error();
}


chakra::error::Error
chakra::database::BucketDB::putAll(const std::string &key, const std::string &value) {
    auto s = dbptr->Put(rocksdb::WriteOptions(), key, value);
    if (!s.ok()){
        return chakra::error::Error(s.ToString());
    }
    return chakra::error::Error();
}

chakra::error::Error chakra::database::BucketDB::putAll(const rocksdb::Slice &key, const rocksdb::Slice &value) {
    auto s = dbptr->Put(rocksdb::WriteOptions(), key, value);
    if (!s.ok()){
        return chakra::error::Error(s.ToString());
    }
    return chakra::error::Error();
}

void chakra::database::BucketDB::del(const std::string &key, bool dbdel) {
    unsigned long hashed = ::crc32(0L, (unsigned char*)key.data(), key.size());
    caches[hashed % FLAGS_db_block_size]->del(key);
    if (dbdel){
        auto s = self->Delete(rocksdb::WriteOptions(), key);
        if (!s.ok()) LOG(ERROR) << "db del error " << s.ToString();
        // TODO: replica self
//        s = dbptr->Delete(rocksdb::WriteOptions(), key);
//        if (!s.ok()) LOG(ERROR) << "db del error " << s.ToString();
    }
}

size_t chakra::database::BucketDB::size() {
    size_t num = 0;
    for(auto & block : caches){
        num += block->size();
    }
    return num;
}

proto::peer::MetaDB chakra::database::BucketDB::getMetaDB(const std::string &dbname) { return metaDB; }

chakra::error::Error chakra::database::BucketDB::restoreDB() {
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

chakra::database::BucketDB::RestoreDB chakra::database::BucketDB::getLastRestoreDB() {
    return chakra::database::BucketDB::RestoreDB();
}

chakra::error::Error chakra::database::BucketDB::getUpdateSince(rocksdb::SequenceNumber seq,
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
chakra::database::BucketDB::snapshot(rocksdb::Iterator **iter, rocksdb::SequenceNumber &seq) {
    auto snapshot = self->GetSnapshot();
    rocksdb::ReadOptions readOptions;
    readOptions.snapshot = snapshot;
    seq = snapshot->GetSequenceNumber();
    (*iter) = self->NewIterator(readOptions);
    return error::Error();
}

rocksdb::SequenceNumber chakra::database::BucketDB::getLastSeqNumber() {
    return self->GetLatestSequenceNumber();
}

chakra::database::BucketDB::~BucketDB() {
    if (self){
        self->Close();
    }
    if (dbptr){
        dbptr->Close();
    }
}

void chakra::database::BucketDB::Put(const rocksdb::Slice &key, const rocksdb::Slice &value) {
    auto s = dbptr->Put(rocksdb::WriteOptions(), key, value);
    if (!s.ok()) LOG(ERROR) << "db delta put error " << s.ToString();
    // 淘汰缓存，下次read重新加载
    unsigned long hashed = ::crc32(0L, (unsigned char*)key.data(), key.size());
    caches[hashed % FLAGS_db_block_size]->del(key.ToString());
}

void chakra::database::BucketDB::Delete(const rocksdb::Slice &key) {
    auto s = dbptr->Delete(rocksdb::WriteOptions(), key);
    if (!s.ok()) LOG(ERROR) << "db delta del error " << s.ToString();
    // 淘汰缓存，下次read重新加载
    unsigned long hashed = ::crc32(0L, (unsigned char*)key.data(), key.size());
    caches[hashed % FLAGS_db_block_size]->del(key.ToString());
}
