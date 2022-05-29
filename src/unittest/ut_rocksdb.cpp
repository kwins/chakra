#ifndef CHAKRA_UT_ROCKSDB_H
#define CHAKRA_UT_ROCKSDB_H

#include <gtest/gtest.h>
#include <memory>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <string>
#include <glog/logging.h>
#include <vector>

namespace chakra::unitest {

class RocksDBTest : public ::testing::Test {
protected:
    void SetUp() override {
        rocksdb::Options rocksOpts;
        rocksOpts.keep_log_file_num = 5;
        rocksOpts.create_if_missing = true;
        rocksOpts.WAL_ttl_seconds = 3600;
        auto s = rocksdb::DB::Open(rocksOpts,  + "./data/test/unittest.db", &db);
        if (!s.ok()) {
            LOG(ERROR) << "open db error:" << s.ToString();
            return;
        }
    }
    void TearDown() override { if (db != nullptr) db->Close(); }

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
private:
    rocksdb::DB* db;
};

TEST_F(RocksDBTest, case0) {
    testRocksDBCase0();
}

}
#endif // CHAKRA_UT_ROCKSDB_H