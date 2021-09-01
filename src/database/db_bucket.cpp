//
// Created by kwins on 2021/6/9.
//

#include "db_bucket.h"
#include <zlib.h>
#include <rocksdb/db.h>
#include <rocksdb/utilities/write_batch_with_index.h>

chakra::database::BucketDB::BucketDB(Options options) {
    this->opts = std::move(options);
    blocks.reserve(opts.blocktSize);
    rocksdb::Options rocksOpts;
    rocksOpts.keep_log_file_num = 5;
    rocksOpts.create_if_missing = true;
    rocksdb::DB* dbSelf;
    auto s = rocksdb::DB::Open(rocksOpts, opts.dir + opts.name + ".self", &dbSelf);
    if (!s.ok()){
        throw std::logic_error(s.ToString());
    }
    rocksdb::DB* db;
    s = rocksdb::DB::Open(rocksOpts, opts.dir + opts.name, &db);
    if (!s.ok()){
        throw std::logic_error(s.ToString());
    }

    self = std::shared_ptr<rocksdb::DB>(dbSelf);
    dbptr = std::shared_ptr<rocksdb::DB>(db);
    for (int i = 0; i < opts.blocktSize; ++i) {
        blocks[i] = std::make_shared<BlockDB>(self, dbptr, opts.blockCapaticy);
    }

    // 加载一些热数据
    uint64_t num = 0;
    auto iter = self->NewIterator(rocksdb::ReadOptions());
    for(iter->SeekToLast(); iter->Valid(); iter->Prev()){
        auto element = std::make_shared<Element>();
        element->deSeralize(iter->value().data(), iter->value().size());
        element->setUpdated(false);
        put(iter->key().ToString(), element, false);
        if (++num >= opts.blockCapaticy * 0.5){
            break;
        }
    }
}

std::shared_ptr<chakra::database::Element>
chakra::database::BucketDB::get(const std::string &key) {
    unsigned long hashed = ::crc32(0L, (unsigned char*)key.data(), key.size());
    return blocks[hashed % opts.blocktSize]->get(key);
}

void chakra::database::BucketDB::put(const std::string &key, std::shared_ptr<Element> val, bool dbput) {
    // TODO: 找一个支持多语言的hash函数
    unsigned long hashed = ::crc32(0L, (unsigned char*)key.data(), key.size());
    blocks[hashed % opts.blocktSize]->put(key, val, dbput);
}

void chakra::database::BucketDB::put(rocksdb::WriteBatch& batch) { batch.Iterate(this); }

void chakra::database::BucketDB::del(const std::string &key, bool dbdel) {
    unsigned long hashed = ::crc32(0L, (unsigned char*)key.data(), key.size());
    blocks[hashed % opts.blocktSize]->del(key, dbdel);
}

std::shared_ptr<rocksdb::DB> chakra::database::BucketDB::getRocksDB() { return self; }

size_t chakra::database::BucketDB::size() {
    size_t num = 0;
    for(auto & block : blocks){
        num += block->size();
    }
    return num;
}

nlohmann::json chakra::database::BucketDB::dumpDB() {
    nlohmann::json j;
    j["name"] = opts.name;
    j["block_size"] = opts.blocktSize;
    j["cached"] = opts.blockCapaticy;
    return j;
}

chakra::database::BucketDB::~BucketDB() {
    if (self)
        self->Close();
    if (dbptr)
        dbptr->Close();
    self = nullptr;
    dbptr = nullptr;
}

void chakra::database::BucketDB::Put(const rocksdb::Slice &key, const rocksdb::Slice &value) {
    dbptr->Put(rocksdb::WriteOptions(), key, value);
    // 淘汰缓存，下次read重新加载
    unsigned long hashed = ::crc32(0L, (unsigned char*)key.data(), key.size());
    blocks[hashed % opts.blocktSize]->del(key.ToString(), false);
}

void chakra::database::BucketDB::Delete(const rocksdb::Slice &key) {
    dbptr->Delete(rocksdb::WriteOptions(), key);
    // 淘汰缓存，下次read重新加载
    unsigned long hashed = ::crc32(0L, (unsigned char*)key.data(), key.size());
    blocks[hashed % opts.blocktSize]->del(key.ToString(), false);
}
