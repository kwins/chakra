//
// Created by kwins on 2021/9/30.
//

#ifndef CHAKRA_FLAGS_CPP
#define CHAKRA_FLAGS_CPP

#include <gflags/gflags.h>
#include <glog/logging.h>
#include "utils/file_helper.h"

DEFINE_int32(server_port, 7290, "chakra server listen port");                                       /* NOLINT */
DEFINE_int32(server_tcp_backlog, 512, "chakra server tcp back log");                                /* NOLINT */
DEFINE_double(server_cron_interval_sec, 1.0, "chakra server cron interval sec");                    /* NOLINT */

DEFINE_string(replica_dir, "data", "replica dir");                                                  /* NOLINT */
static bool validReplicaDir(const char* flagname, const std::string& value){
    if (value.empty()) return false;
    auto err = chakra::utils::FileHelper::mkdir(value);
    if (err){
        LOG(ERROR) << "valid replica dir error " << err.what();
        return false;
    }
    return true;
}
DEFINE_validator(replica_dir, validReplicaDir);                                                 /* NOLINT */
DEFINE_int32(replica_timeout_ms, 10000, "replicas timeout ms");                                 /* NOLINT */
DEFINE_double(replica_cron_interval_sec, 1.0, "replica cron interval sec, use double");         /* NOLINT */
DEFINE_int32(replica_timeout_retry, 10, "replica timeout retry");                               /* NOLINT */
DEFINE_int64(replica_last_try_resync_timeout_ms, 1000, "last try resync timeout");              /* NOLINT */
DEFINE_double(replica_delta_pull_interval_sec, 0.2, "replica pull db dalta interval sec");      /* NOLINT */
DEFINE_int64(replica_delta_batch_bytes, 1024 * 8, "replica delta batch size");                  /* NOLINT */
DEFINE_double(replica_bulk_send_interval_sec, 0.2, "replica send db bulk interval sec");        /* NOLINT */
DEFINE_int64(replica_bulk_batch_bytes, 1024 * 16, "replica bulk batch size");                   /* NOLINT */

DEFINE_string(db_dir, "data", "rocksdb save dir");                                              /* NOLINT */
static bool validDbDir(const char* flagname, const std::string& value){
    if (value.empty()) return false;
    auto err = chakra::utils::FileHelper::mkdir(value);
    if (err) return false;
    return true;
}
DEFINE_validator(db_dir, validDbDir);                                                                   /* NOLINT */
DEFINE_string(db_restore_dir, "data", "rocksdb restore dir");                                   /* NOLINT */
DEFINE_string(db_backup_dir, "data", "rocksdb backup dir");                                     /* NOLINT */
DEFINE_int32(db_cache_shard_size, 16, "rocksdb cached block size");                             /* NOLINT */
DEFINE_int64(db_default_cache_bytes, 1024 * 1024 * 100, "rocksdb cached default cache size");   /* NOLINT */
DEFINE_int64(db_wal_ttl_seconds, 86400, "rocksdb wal log ttl seconds");                         /* NOLINT */


DEFINE_string(cluster_dir, "data", "cluster dir");                                              /* NOLINT */
static bool validClusterDir(const char* flagname, const std::string& value){
    if (value.empty()) return false;
    auto err = chakra::utils::FileHelper::mkdir(value);
    if (err) return false;
    return true;
}
DEFINE_validator(cluster_dir, validClusterDir);                                                 /* NOLINT */
DEFINE_int32(cluster_handshake_timeout_ms, 10000, "cluster handshake timeout ms");              /* NOLINT */
DEFINE_int32(cluster_peer_timeout_ms, 10000, "cluster peer timeout ms");                        /* NOLINT */
DEFINE_int32(cluster_peer_link_retry_timeout_ms, 10000, "cluster peer link retry timeout ms");  /* NOLINT */
DEFINE_double(cluster_cron_interval_sec, 0.5, "cluster cron interval sec");                     /* NOLINT */

#endif //CHAKRA_FLAGS_CPP
