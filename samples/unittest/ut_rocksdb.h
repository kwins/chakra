//
// Created by kwins on 2021/6/10.
//

#ifndef CHAKRA_UT_ROCKSDB_H
#define CHAKRA_UT_ROCKSDB_H
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include <vector>
#include <zlib.h>
#include <algorithm>
#include <rocksdb/utilities/backupable_db.h>
#include <unistd.h>
#include "utils/file_helper.h"
#include <rocksdb/utilities/db_ttl.h>
#include <thread>
#include <rocksdb/utilities/write_batch_with_index.h>
#include <rocksdb/comparator.h>

DEFINE_string(rocks_dir, "./test/data", "测试目录");

struct Entry {
    std::string key;
    std::string value;
    rocksdb::WriteType type;
};

struct TestHandler : public rocksdb::WriteBatch::Handler {

    void Put(const rocksdb::Slice &slice, const rocksdb::Slice &slice1) override {
        LOG(INFO) << "Put key:" << slice.ToString() << " value:" << slice1.ToString();
        db->Put(rocksdb::WriteOptions(), slice, slice1);
    }

    void Delete(const rocksdb::Slice &slice) override {
        LOG(INFO) << "Delete key:" << slice.ToString();
    }
    rocksdb::DB* db;
};

class RocksDBTest : public ::testing::Test{
public:
    void testSeq(){
        rocksdb::DB* db;
        rocksdb::Options options;
        options.keep_log_file_num = 2;
        options.create_if_missing = true;
        rocksdb::Status status =  rocksdb::DB::Open(options, FLAGS_rocks_dir, &db);
        if (!status.ok()){
            LOG(ERROR) << "RocksDB open " << status.ToString();
            return;
        }
        rocksdb::Slice key = "haha_1";
        rocksdb::Slice key2 = "haha_2";

        db->Put(rocksdb::WriteOptions(), key, rocksdb::Slice("haha_1_value"));
        db->Put(rocksdb::WriteOptions(), key2, rocksdb::Slice("haha_2_value"));
        auto seq = db->GetLatestSequenceNumber();
        db->Close();
        delete db;
        LOG(INFO) << "after put seq: " << seq;
    }

    void testRestore(){
        std::string restoreDir = "../test/data_1";
        rocksdb::BackupEngineReadOnly* backupEngineReadOnly;
        auto status = rocksdb::BackupEngineReadOnly::Open(rocksdb::Env::Default(),rocksdb::BackupableDBOptions("./test/data_back"), &backupEngineReadOnly);
        assert(status.ok());
        backupEngineReadOnly->RestoreDBFromBackup(1, restoreDir, restoreDir);
        delete backupEngineReadOnly;

        rocksdb::DBOptions options;
        options.keep_log_file_num = 2;
        options.create_if_missing = true;
        std::vector<std::string> columnFamilys;
        rocksdb::DB::ListColumnFamilies(options, restoreDir, &columnFamilys);
        std::for_each(columnFamilys.begin(), columnFamilys.end(), [](const std::string& v){
            LOG(INFO) << "RocksDB column family " << v;
        });

        std::vector<rocksdb::ColumnFamilyDescriptor> columnFamilysDescriptors;
        columnFamilysDescriptors.reserve(columnFamilys.size());
        for (auto& name : columnFamilys){
            columnFamilysDescriptors.emplace_back(name, rocksdb::ColumnFamilyOptions());
        }
        std::vector<rocksdb::ColumnFamilyHandle*> cfsRestore;
        rocksdb::DB* dbRestore;
        status =  rocksdb::DB::Open(options, restoreDir, columnFamilysDescriptors, &cfsRestore, &dbRestore);
        if (!status.ok()){
            LOG(ERROR) << "RocksDB open " << status.ToString();
            return;
        }

        rocksdb::Slice key = "haha_1";
        std::string str;
        status = dbRestore->Get(rocksdb::ReadOptions(), cfsRestore[0], key, &str);
        assert(status.ok());
        LOG(INFO) << "RocksDB restore get haha_1 " << str;
        for (auto& ptr : cfsRestore){
            dbRestore->DestroyColumnFamilyHandle(ptr);
        }
        dbRestore->Close();
        delete dbRestore;
    }

    void testReplica(){
        rocksdb::DB* data2;
        rocksdb::Options options;
        options.create_if_missing = true;
        options.WAL_ttl_seconds = 100000;
        auto s = rocksdb::DB::Open(options, "../test/data_4", &data2);
        for (int i = 0; i < 30; ++i) {
            s = data2->Put(rocksdb::WriteOptions(), rocksdb::Slice("key_"+ std::to_string(i)), rocksdb::Slice("value_"+ std::to_string(i)));
            assert(s.ok());
        }

        rocksdb::DB* data3;
        s = rocksdb::DB::Open(options, "../test/data_3", &data3);

        std::unique_ptr<rocksdb::TransactionLogIterator> iter;
        s = data2->GetUpdatesSince(0, &iter);

        LOG(INFO) << "state:" << s.ToString();
        LOG(INFO) << "latest seq:" << data2->GetLatestSequenceNumber();
        assert(s.ok());
        int size = 0;
        TestHandler handler;
        handler.db = data3;
        while (iter->Valid()){
//
            rocksdb::WriteBatch batch(iter->GetBatch().writeBatchPtr->Data());
//            data3->Write(rocksdb::WriteOptions(), iter->GetBatch().writeBatchPtr.get());
            batch.Iterate(&handler);
            LOG(INFO) << "data:" << iter->GetBatch().writeBatchPtr->Data() << " seq:" << iter->GetBatch().sequence;
            iter->Next();
            size++;
        }
        std::string str;
        data3->Get(rocksdb::ReadOptions(), "key_1", &str);

        LOG(INFO) << "sync size:" << size << " data3 key_1 value:" << str;
        data2->Close();
        data3->Close();
        delete data2;
        delete data3;
    }

    void testBackUpReplica(){
        rocksdb::DB* data5;
        rocksdb::Options options;
        options.keep_log_file_num = 2;
        options.create_if_missing = true;
        options.WAL_ttl_seconds = 1000;
        options.WAL_size_limit_MB = 10;
        auto s = rocksdb::DB::Open(options, "../test/data_5", &data5);
        for (int i = 0; i < 10000; ++i) {
            s = data5->Put(rocksdb::WriteOptions(), rocksdb::Slice("key_1"), rocksdb::Slice("value_" + std::to_string(i)));

            assert(s.ok());
        }
        auto oriSeq = data5->GetLatestSequenceNumber();


        std::string backupDir = "./test/data5_backup";
        rocksdb::BackupEngine* backupEngine;
        s = rocksdb::BackupEngine::Open(rocksdb::Env::Default(), rocksdb::BackupableDBOptions(backupDir), &backupEngine);
        assert(s.ok());

        s = backupEngine->CreateNewBackup(data5);
        assert(s.ok());


        std::string restoreDir = "../test/data5_restore";
        rocksdb::BackupEngineReadOnly* backupEngineReadOnly;
        s = rocksdb::BackupEngineReadOnly::Open(rocksdb::Env::Default(),rocksdb::BackupableDBOptions(backupDir), &backupEngineReadOnly);
        assert(s.ok());
        backupEngineReadOnly->RestoreDBFromBackup(1, restoreDir, restoreDir);


        rocksdb::DB* data5_backup;
        s = rocksdb::DB::Open(options, restoreDir, &data5_backup);
        assert(s.ok());
        auto restoreSeq = data5_backup->GetLatestSequenceNumber();
        data5_backup->Close();
        data5->Close();
        delete data5;
        delete backupEngine;
        delete backupEngineReadOnly;
        delete data5_backup;

        std::string name = "tar";
        std::string opts = "cvf";
        std::string dest = "./restore.tar";
        std::string dir = "./test/data5_restore";
        char* argv[4] = {
                const_cast<char*>(name.c_str()),
                const_cast<char*>(opts.c_str()),
                const_cast<char*>(dest.c_str()),
                const_cast<char*>(dir.c_str()),
        };
        int ret = execv("/usr/bin/tar", argv);
        LOG(INFO) << "ori seq:" << oriSeq << " restore seq:" << restoreSeq << " ret:" << ret;
    }

    void testTTLRocksDB(){
        rocksdb::DBWithTTL* data6;
        rocksdb::Options options;
        options.create_if_missing = true;
        options.WAL_ttl_seconds = 1000;
        options.WAL_size_limit_MB = 10;
        options.level0_file_num_compaction_trigger = 1;
        options.max_bytes_for_level_base = 1024;
        options.max_bytes_for_level_multiplier = 1;
        auto s = rocksdb::DBWithTTL::Open(options,"../test/data_6", &data6, 3);
        assert(s.ok());
        std::string str;
        for (int j = 0; j < 100; ++j) {
            str += "abcdefghijklmnopqistuvwxyz";
        }
        int i = 4000;
        for (; i < 6000; ++i) {
            std::string v = std::to_string(i) + "_" + str;
            s = data6->Put(rocksdb::WriteOptions(), rocksdb::Slice("key_ttl_" + std::to_string(i)), rocksdb::Slice(v));
            assert(s.ok());
            std::this_thread::sleep_for(std::chrono::milliseconds (10));
        }

        std::unique_ptr<rocksdb::TransactionLogIterator> iter;
        s = data6->GetUpdatesSince(0, &iter, rocksdb::TransactionLogIterator::ReadOptions());

        LOG(INFO) << "state:" << s.ToString();
        assert(s.ok());
        int size = 0;
        while (iter->Valid()){
            auto batch = (*iter->GetBatch().writeBatchPtr);
            LOG(INFO) << "data:" << iter->GetBatch().writeBatchPtr->Data() << " sequence:" << iter->GetBatch().sequence;
            iter->Next();
            size++;
        }

//        rocksdb::WriteBatch batch;
//        LOG(INFO) << "latest seq:" << data6->GetLatestSequenceNumber() << " size:" << size;
        std::string value_1;
        rocksdb::Slice key("key_ttl_2");
        s = data6->Get(rocksdb::ReadOptions(), key, &value_1);
        if (!s.ok()){
            LOG(INFO) << "!ok  " << s.ToString();
        }
        LOG(INFO) << key.ToString() << ":" << value_1;

        rocksdb::Slice key1("key_ttl_4000");
        s = data6->Get(rocksdb::ReadOptions(), key1, &value_1);
        if (!s.ok()){
            LOG(INFO) << "!ok  " << s.ToString();
        }
        LOG(INFO) << key1.ToString() << ":" << value_1;
        data6->Close();
        delete data6;
    }

    void testIterator(){
        rocksdb::DB* data7;
        rocksdb::Options options;
        options.create_if_missing = true;
        options.WAL_ttl_seconds = 86400;
        auto s = rocksdb::DB::Open(options, "./data/node2/db1.self", &data7);
        if (!s.ok()){
            LOG(ERROR) << s.ToString();
        } else{
            rocksdb::Iterator* it = data7->NewIterator(rocksdb::ReadOptions());
            for(it->SeekToFirst(); it->Valid(); it->Next()){
                LOG(INFO) << "key:" << it->key().ToString() << " value:" << it->value().ToString();
            }
            LOG(INFO) << "it->Valid():" << it->Valid();
            data7->Close();
            delete data7;
        }
    }

    void testSnapshot(){
        rocksdb::DB* data8;
        rocksdb::Options options;
        options.create_if_missing = true;
        options.WAL_ttl_seconds = 86400;
        auto s = rocksdb::DB::Open(options, "./data/node2/db1.self", &data8);
        if (!s.ok()){
            LOG(ERROR) << s.ToString();
        } else {
            rocksdb::ReadOptions readOptions;
            auto snapshot = data8->GetSnapshot();
            readOptions.snapshot = snapshot;
            auto it = data8->NewIterator(readOptions);
            for(it->SeekToFirst(); it->Valid(); it->Next()){
                LOG(INFO) << "key:" << it->key().ToString() << " value:" << it->value().ToString();
            }

            auto snapshotSeq = readOptions.snapshot->GetSequenceNumber();
            auto afterSnapshotSeq = data8->GetLatestSequenceNumber();

            delete data8;
            LOG(INFO) << " afterSnapshotSeq:" << afterSnapshotSeq << " valid" << it->Valid();
        }
    }

    void testWriteBatchIndex(){
        rocksdb::WriteBatch batch;
        rocksdb::WriteBatchWithIndex batchWithIndex;
        for (int i = 0; i < 10; ++i) {
            LOG(INFO) << "write key:" << "batch_key_" + std::to_string(i);
            batch.Put("batch_key_" + std::to_string(i), "batch_value_" + std::to_string(i));
        }
        TestHandler handler;
        std::string  str = "\n"
                           "\n"
                           "batch_value_0\n"
                           "batch_value_1batch_key_1\n"
                           "batch_value_2batch_key_2\n"
                           "batch_value_3batch_key_3\n"
                           "batch_value_4batch_key_4\n"
                           "batch_value_5batch_key_5\n"
                           "batch_value_6batch_key_6\n"
                           "batch_value_7batch_key_7\n"
                           "batch_value_8batch_key_8\n"
                           "batch_value_9batch_key_9";
        LOG(INFO) << "str:" <<str;
        rocksdb::WriteBatch batch1(str);
        batch1.Iterate(&handler);

        LOG(INFO) << "1";
        auto it = batchWithIndex.NewIterator();
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            LOG(INFO)  << "key:" << it->Entry().key.ToString() << " value:" << it->Entry().value.ToString();
        }
        LOG(INFO) << "2";
    }

    void testReplicaV2(){
        rocksdb::Options options;
        options.create_if_missing = true;
        options.WAL_ttl_seconds = 100000;

        rocksdb::DB* data3;
        auto s = rocksdb::DB::Open(options, "./data/node4/db4.self", &data3);

        s = data3->Put(rocksdb::WriteOptions(), "key_1", "value_1");

        LOG(INFO) << "write state:" << s.ToString() << "latest seq:" << data3->GetLatestSequenceNumber();

        std::unique_ptr<rocksdb::TransactionLogIterator> iter;
        s = data3->GetUpdatesSince(data3->GetLatestSequenceNumber(), &iter);

        LOG(INFO) << "state:" << s.ToString() << " valid:" << iter->Valid();

//        assert(s.ok());
        int size = 0;
        while (iter->Valid()){
            LOG(INFO) << "data:" << iter->GetBatch().writeBatchPtr->Data() << " seq:" << iter->GetBatch().sequence;
            iter->Next();
            size++;
        }
        LOG(INFO) << "size:" << size;
        delete data3;
    }
};

TEST_F(RocksDBTest, basicOp){
//    testSeq();
//    testRestore();
//testWriteBatch();
//    testReplica();
//testBackUpReplica();
//    testTTLRocksDB();
//    testIterator();
//    testSnapshot();
    testReplicaV2();
//    testWriteBatchIndex();
//testReplicaV2();
}
#endif //CHAKRA_UT_ROCKSDB_H
