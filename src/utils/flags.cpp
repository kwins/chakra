//
// Created by kwins on 2021/9/30.
//

#ifndef CHAKRA_FLAGS_CPP
#define CHAKRA_FLAGS_CPP

#include <gflags/gflags.h>
#include <glog/logging.h>
#include "utils/file_helper.h"

DEFINE_string(server_ip, "127.0.0.1", "chakra server ip");                                          /* NOLINT */
DEFINE_int32(server_port, 7290, "chakra server listen port");                                       /* NOLINT */
DEFINE_int32(server_tcp_backlog, 512, "chakra server tcp back log");                                /* NOLINT */
DEFINE_double(server_cron_interval_sec, 1.0, "chakra server tcp back log");                         /* NOLINT */
DEFINE_int64(connect_buff_size, 16384, "connect recv max buff size of message, default is 16384");  /* NOLINT */

DEFINE_string(replica_dir, "data", "replica dir");                                                  /* NOLINT */
static bool validReplicaDir(const char* flagname, const std::string& value){
    if (value.empty()) return false;
    auto err = chakra::utils::FileHelper::mkDir(value);
    if (!err.success()){
        LOG(ERROR) << "valid replica dir error " << err.what();
        return false;
    }
    return true;
}
DEFINE_validator(replica_dir, validReplicaDir);                                                         /* NOLINT */

DEFINE_int32(replica_tcp_back_log, 512, "replica tcp back log");                                /* NOLINT */
DEFINE_int32(replica_timeout_ms, 10000, "replicas timeout ms");                                 /* NOLINT */
DEFINE_double(replica_cron_interval_sec, 1.0, "replica cron interval sec, use double");         /* NOLINT */
DEFINE_int32(replica_timeout_retry, 10, "replica timeout retry");                               /* NOLINT */
DEFINE_double(replica_delta_pull_interval_sec, 1.0, "replica pull db dalta interval sec");      /* NOLINT */
DEFINE_int64(replica_delta_batch_bytes, 4096, "replica delta batch size");                      /* NOLINT */
DEFINE_double(replica_bulk_send_interval_sec, 1.0, "replica send db bulk interval sec");        /* NOLINT */
DEFINE_int64(replica_bulk_batch_bytes, 4096, "replica bulk batch size");                        /* NOLINT */

DEFINE_string(db_dir, "data", "rocksdb save dir");                                              /* NOLINT */
static bool validDbDir(const char* flagname, const std::string& value){
    if (value.empty()) return false;
    auto err = chakra::utils::FileHelper::mkDir(value);
    if (!err.success()){
        LOG(ERROR) << "valid db dir error " << err.what();
        return false;
    }
    return true;
}
DEFINE_validator(db_dir, validDbDir);                                                                   /* NOLINT */

DEFINE_string(db_restore_dir, "data", "rocksdb restore dir");                                   /* NOLINT */
DEFINE_string(db_backup_dir, "data", "rocksdb backup dir");                                     /* NOLINT */
DEFINE_int32(db_block_size, 256, "rocksdb cached block size");                                  /* NOLINT */
DEFINE_int64(db_wal_ttl_seconds, 86400, "rocksdb wal log ttl seconds");                         /* NOLINT */


DEFINE_string(cluster_dir, "data", "cluster dir");                                              /* NOLINT */
static bool validClusterDir(const char* flagname, const std::string& value){
    if (value.empty()) return false;
    auto err = chakra::utils::FileHelper::mkDir(value);
    if (!err.success()){
        LOG(ERROR) << "valid cluster dir error " << err.what();
        return false;
    }
    return true;
}
DEFINE_validator(cluster_dir, validClusterDir);                                                         /* NOLINT */

DEFINE_int32(cluster_port, 7291, "cluster port");                                               /* NOLINT */
DEFINE_int32(cluster_handshake_timeout_ms, 10000, "cluster handshake timeout ms");              /* NOLINT */
DEFINE_int32(cluster_peer_timeout_ms, 10000, "cluster peer timeout ms");                        /* NOLINT */
DEFINE_int32(cluster_peer_link_retry_timeout_ms, 10000, "cluster peer link retry timeout ms");  /* NOLINT */
DEFINE_double(cluster_cron_interval_sec, 1.0, "cluster cron interval sec");                     /* NOLINT */
DEFINE_int32(cluster_tcp_back_log, 512, "cluster tcp back log");                                /* NOLINT */

#endif //CHAKRA_FLAGS_CPP
