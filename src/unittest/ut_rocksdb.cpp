#ifndef CHAKRA_UT_ROCKSDB_H
#define CHAKRA_UT_ROCKSDB_H

#include <gtest/gtest.h>
#include <memory>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/write_batch_base.h>
#include <string>
#include <glog/logging.h>
#include <vector>
#include "database/db_replica_batch.h"

namespace chakra::unitest {

class RocksDBTest : public ::testing::Test {
protected:
    void SetUp() override {
        db = openRocksDB("./data/test/unittest.db");
    }

    void TearDown() override { if (db != nullptr) db->Close(); }
    rocksdb::DB* openRocksDB(std::string name) {
        rocksdb::DB* dbptr;
        rocksdb::Options rocksOpts;
        rocksOpts.keep_log_file_num = 5;
        rocksOpts.create_if_missing = true;
        rocksOpts.WAL_ttl_seconds = 3600;
        auto s = rocksdb::DB::Open(rocksOpts,  name, &dbptr);
        if (!s.ok()) {
            LOG(ERROR) << "open db error:" << s.ToString();
            exit(-1);
        }
        return dbptr;
    }

    void makeFakeRocksData(int n) {
        for (int i = 0; i < n; i++) {
            auto s = db->Put(rocksdb::WriteOptions(), "key_" + std::to_string(i), "value_" + std::to_string(i));
            if (!s.ok()) {
                LOG(ERROR) << s.ToString();
            }
        }
    }
    void testRocksDBCase0() {
        // makeFakeRocksData(1000000);
        auto iter = db->NewIterator(rocksdb::ReadOptions());
        iter->Seek("vey_999995");
        while (iter->Valid()) {
            LOG(INFO) << "key:" << iter->key().ToString() << " value:" << iter->value().ToString();
            iter->Next();
        }
    }
    void testRocksDBCase1(){
        auto db1 = openRocksDB("./data/test/unittest2.db");
        // makeFakeRocksData(10000);
        std::unique_ptr<rocksdb::TransactionLogIterator> iter;
        auto s = db->GetUpdatesSince(500000, &iter);
        if (!s.ok()) {
            LOG(ERROR) << s.ToString();
            return;
        }

        while (iter->Valid()) {
            database::ReplicaBatchHandler batchHandler;
            auto err = batchHandler.Put(iter->GetBatch().writeBatchPtr->Data());
            if (err) {
                LOG(ERROR) << err.what();
                continue;
            }
            s = db1->Write(rocksdb::WriteOptions(), &batchHandler.GetBatch());
            if (!s.ok()) {
                LOG(ERROR) << s.ToString();
                continue;
            }
            iter->Next();
        }

        for (int i = 0; i < 20; i++) {
            std::string value;
            s = db1->Get(rocksdb::ReadOptions(), "key_" + std::to_string(i), &value);
            if (!s.ok()) {
                LOG(ERROR) << s.ToString();
            }
        }
        db1->Close();
    }
private:
    rocksdb::DB* db;
};

TEST_F(RocksDBTest, case0) {
    testRocksDBCase0();
}

TEST_F(RocksDBTest, case1) {
    testRocksDBCase1();
}

}
#endif // CHAKRA_UT_ROCKSDB_H