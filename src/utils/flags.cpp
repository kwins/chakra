//
// Created by kwins on 2021/9/30.
//

#ifndef CHAKRA_FLAGS_CPP
#define CHAKRA_FLAGS_CPP

#include <gflags/gflags.h>

DEFINE_string(server_ip, "127.0.0.1", "chakra server ip");                                      /* NOLINT */
DEFINE_int32(server_port, 7290, "chakra server listen port");                                   /* NOLINT */
DEFINE_int32(server_tcp_backlog, 512, "chakra server tcp back log");                            /* NOLINT */
DEFINE_double(server_cron_interval_sec, 1.0, "chakra server tcp back log");                     /* NOLINT */


DEFINE_string(replica_dir, "data", "replica dir");                                              /* NOLINT */
DEFINE_string(replica_ip, "127.0.0.1", "replica ip");                                           /* NOLINT */
DEFINE_int32(replica_tcp_back_log, 512, "replica tcp back log");                                /* NOLINT */
DEFINE_int32(replica_timeout_ms, 10000, "replicas timeout ms");                                 /* NOLINT */
DEFINE_double(replica_cron_interval_sec, 1.0, "replica cron interval sec, use double");         /* NOLINT */
DEFINE_int32(replica_timeout_retry, 10, "replica timeout retry");                               /* NOLINT */
DEFINE_double(replica_delta_pull_interval_sec, 1.0, "replica pull db dalta interval sec");      /* NOLINT */
DEFINE_double(replica_bulk_send_interval_sec, 1.0, "replica send db bulk interval sec");        /* NOLINT */

DEFINE_string(db_dir, "data", "rocksdb save dir");                                              /* NOLINT */
DEFINE_string(db_restore_dir, "data", "rocksdb restore dir");                                   /* NOLINT */
DEFINE_string(db_backup_dir, "data", "rocksdb backup dir");                                     /* NOLINT */
DEFINE_int32(db_block_size, 256, "rocksdb cached block size");                                  /* NOLINT */
DEFINE_int64(db_wal_ttl_seconds, 86400, "rocksdb wal log ttl seconds");                         /* NOLINT */


DEFINE_string(cluster_dir, "data", "cluster dir");                                              /* NOLINT */
DEFINE_string(cluster_ip, "127.0.0.1", "cluster ip");                                           /* NOLINT */
DEFINE_int32(cluster_port, 7291, "cluster port");                                               /* NOLINT */
DEFINE_int32(cluster_handshake_timeout_ms, 10000, "cluster handshake timeout ms");              /* NOLINT */
DEFINE_int32(cluster_peer_timeout_ms, 10000, "cluster peer timeout ms");                        /* NOLINT */
DEFINE_int32(cluster_peer_link_retry_timeout_ms, 10000, "cluster peer link retry timeout ms");  /* NOLINT */
DEFINE_double(cluster_cron_interval_sec, 1.0, "cluster cron interval sec");                     /* NOLINT */
DEFINE_int32(cluster_tcp_back_log, 512, "cluster tcp back log");                                /* NOLINT */

#endif //CHAKRA_FLAGS_CPP
