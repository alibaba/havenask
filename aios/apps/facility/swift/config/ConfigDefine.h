/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once
#include <limits>
#include <stdint.h>

namespace swift {
namespace config {

constexpr char HIPPO_FILE_NAME[] = "swift_hippo.json";
constexpr char SWIFT_CONF_FILE_NAME[] = "swift.conf";

constexpr char SECTION_COMMON[] = "common";
constexpr char SECTION_ADMIN[] = "admin";
constexpr char SECTION_BROKER[] = "broker";
constexpr char SECTION_WHITE_LIST[] = "white_list";

constexpr char SYS_USER_PREFIX[] = "sys_user_";
constexpr char NORMAL_USER_PREFIX[] = "normal_user_";
constexpr char INNER_SYS_USER[] = "inner_sys_user";

// COMMON
constexpr char USER_NAME[] = "user_name";
constexpr char SERVICE_NAME[] = "service_name";
constexpr char HIPPO_ROOT_PATH[] = "hippo_root";
constexpr char GATEWAY_ROOT_PATH[] = "gateway_root";
constexpr char ZOOKEEPER_ROOT_PATH[] = "zookeeper_root";
constexpr char ADMIN_COUNT[] = "admin_count";
constexpr char BROKER_COUNT[] = "broker_count";
constexpr char GROUP_BROKER_COUNT[] = "group_broker_count";
constexpr char GROUP_NET_PRIORITY[] = "group_net_priority";
constexpr char BROKER_EXCLUSIVE_LEVEL[] = "broker_exclusive_level";

constexpr char AMONITOR_PORT[] = "amon_agent_port";
constexpr char LEADER_LEASE_TIME[] = "leader_lease_time";
constexpr char LEADER_LOOP_INTERVAL[] = "leader_loop_interval";
constexpr char USE_RECOMMEND_PORT[] = "use_recommend_port";
constexpr char ENABLE_BACKUP[] = "enable_backup";
constexpr char INITIAL_MASTER_VERSION[] = "initial_master_version";
constexpr char MIRROR_ZKROOT[] = "backup_peer_zkroot";
constexpr char ENABLE_AUTHENTICATION[] = "enable_authentication";
constexpr char INTERNAL_AUTHENTICATION_USERNAME[] = "internal_authentication_username";
constexpr char INTERNAL_AUTHENTICATION_PASSWD[] = "internal_authentication_passwd";
constexpr char READ_NOT_COMMITED_MSG[] = "read_not_committed_msg";
constexpr char MAX_TOPIC_ACL_SYNC_INTERVAL_US[] = "max_topic_acl_sync_interval_us";

// ADMIN
constexpr char DISPATCH_INTERVAL[] = "dispatch_interval_ms";
constexpr char CANDIDATE_WAIT_TIME_MS[] = "candidate_wait_time_ms";
constexpr char BROKER_NETLIMIT_MB[] = "broker_netlimit_mb";
constexpr char CLEAN_DATA_INTERVAL_HOUR[] = "clean_data_interval_hour";
constexpr char DELETE_EXPIRED_TOPIC_INTERVAL_SECOND[] = "delete_expired_topic_interval_second";
constexpr char RESERVE_DATA_HOUR[] = "reserve_data_hour";
constexpr char DECSION_THRESHOLD[] = "decsion_threshold";
constexpr char SYNC_ADMIN_INFO_PATH[] = "sync_admin_info_path";
constexpr char SYNC_ADMIN_INFO_INTERVAL[] = "sync_admin_info_interval_us";
constexpr char SYNC_TOPIC_INFO_INTERVAL[] = "sync_topic_info_interval_us";
constexpr char FORCE_SYNC_LEADER_INFO_INTERVAL[] = "force_sync_leader_info_interval_us";
constexpr char SYNC_HEARTBEAT_INTERVAL[] = "sync_heartbeat_interval_us";
constexpr char ADJUST_RESOURCE_DURATION_MS[] = "adjust_resource_duration_ms";
constexpr char FORCE_SCHEDULE_TIME[] = "force_schedule_time_sec";
constexpr char RELEASE_DEAD_WORKER_TIME[] = "release_dead_worker_time_sec";
constexpr char ADMIN_IO_THREAD_NUM[] = "admin_io_thread_num";
constexpr char ADMIN_THREAD_NUM[] = "admin_thread_num";
constexpr char ADMIN_THREAD_QUEUE_SIZE[] = "admin_thread_queue_size";
constexpr char ADMIN_HTTP_THREAD_NUM[] = "admin_http_thread_num";
constexpr char ADMIN_HTTP_THREAD_QUEUE_SIZE[] = "admin_http_thread_queue_size";
constexpr char RECORD_LOCAL_FILE[] = "record_local_file";
constexpr char TOPIC_RESOURCE_LIMIT[] = "topic_resource_limit";
constexpr char WORKER_PARTITION_LIMIT[] = "worker_partition_limit";
constexpr char MIN_MAX_PARTITION_BUFFER_SIZE_LIMIT[] = "min_max_partition_buffer_size_mb";
constexpr char TOPIC_GROUP_VEC[] = "topic_group_vec";
constexpr char TOPIC_OWNER_VEC[] = "topic_owner_vec";
constexpr char VETICAL_BROKERCNT_VEC[] = "vetical_brokercnt_vec";
constexpr char BACKUP_META_PATH[] = "backup_topicmeta_path";
constexpr char RESERVE_BACK_META_COUNT[] = "reserve_back_meta_count";
constexpr char AUTO_UPDATE_PART_RESOURCE[] = "auto_update_part_resource";
constexpr char NOUSE_TOPIC_EXPIRE_TIME_SEC[] = "nouse_topic_expire_time_sec";
constexpr char NOUSE_TOPIC_DELETE_INTERVAL_SEC[] = "nouse_topic_delete_interval_sec";
constexpr char MISS_TOPIC_RECOVER_INTERVAL_SEC[] = "miss_topic_recover_interval_sec";
constexpr char MAX_ALLOW_NOUSE_FILE_NUM[] = "max_allow_nouse_file_num";
constexpr char IS_LOCAL_MODE[] = "is_local_mode";
constexpr char TOPIC_SCHEDULE_TYPE[] = "topic_schedule_type";
constexpr char ENABLE_MULTI_TRHEAD_DISPATCH_TASK[] = "enable_multi_thread_dispatch_task";
constexpr char DISPATCH_TASK_THREAD_NUM[] = "dispatch_task_thread_num";
constexpr char DEAL_ERROR_BROKER[] = "deal_error_broker";
constexpr char ERROR_BROKER_DEAL_RATIO[] = "error_broker_deal_ratio";
constexpr char REPORT_ZOMBIE_URL[] = "report_zombie_url";
constexpr char CS_ZFS_TIMEOUT[] = "cs_zfs_timeout_us";
constexpr char CS_COMMIT_DELAY_THRESHOLD_SEC[] = "cs_commit_delay_threshold_sec";
constexpr char BROKER_UNKNOWN_TIMEOUT[] = "broker_unknown_timeout_us";
constexpr char DEAD_BROKER_TIMEOUT_SEC[] = "dead_broker_timeout_sec";
constexpr char CHG_TOPIC_PARTCNT_INTERVAL_US[] = "chg_topic_partcnt_interval_us";
constexpr char MAX_RESTART_COUNT_IN_LOCAL[] = "max_restart_count_in_local";
constexpr char HEARTBEAT_THREAD_NUM[] = "heartbeat_thread_num";
constexpr char HEARTBEAT_QUEUE_SIZE[] = "heartbeat_queue_size";
constexpr char CLEAN_AT_DELETE_TOPIC_PATTERNS[] = "clean_at_delete_topic_patterns";
constexpr char META_INFO_REPLICATE_INTERVAL[] = "meta_info_replicate_interval";

// BROKER
constexpr char DFS_ROOT_PATH[] = "data_root_path";
constexpr char IP_MAP_FILE[] = "ip_map_file";
constexpr char LOG_SAMPLE_COUNT[] = "log_sample_count";
constexpr char CLOSE_FORCE_LOG[] = "close_force_log";
constexpr char MAX_GET_MESSAGE_SIZE_KB[] = "max_get_message_size_kb";
constexpr char HOLD_NO_DATA_REQUEST_TIME_MS[] = "hold_no_data_request_time_ms";
constexpr char NO_DATA_REQUEST_NOTFIY_INTERVAL_MS[] = "no_data_request_notfiy_interval_ms";
constexpr char EXTEND_DFS_ROOT_PATH[] = "extend_data_root_path";
constexpr char TODEL_DFS_ROOT_PATH[] = "todel_data_root_path";
constexpr char THREAD_NUM[] = "thread_num";
constexpr char REPORT_METRIC_THREAD_NUM[] = "report_metric_thread_num";
constexpr char THREAD_QUEUE_SIZE[] = "thread_queue_size";
constexpr char HTTP_THREAD_NUM[] = "http_thread_num";
constexpr char HTTP_THREAD_QUEUE_SIZE[] = "http_thread_queue_size";

constexpr char IO_THREAD_NUM[] = "io_thread_num";
constexpr char ANET_TIMEOUT_LOOP_INTERVAL_MS[] = "anet_timeout_loop_interval_ms";
constexpr char EXCLUSIVE_LISTEN_THREAD[] = "exclusive_listen_thread";
constexpr char PROMOTE_IO_THREAD_PRIORITY[] = "promote_io_thread_priority";
constexpr char MAX_READ_THREAD_NUM[] = "max_read_thread_num";
constexpr char MAX_WRITE_THREAD_NUM[] = "max_write_thread_num";

constexpr char TOTAL_BUFFER_SIZE_MB[] = "total_buffer_size_mb";
constexpr char PARTITION_MIN_BUFFER_SIZE_MB[] = "partition_min_buffer_size_mb";
constexpr char PARTITION_MAX_BUFFER_SIZE_MB[] = "partition_max_buffer_size_mb";
constexpr char BUFFER_MIN_RESERVE_SIZE_MB[] = "buffer_min_reserve_size_mb";
constexpr char FILE_BUFFER_USE_RATIO[] = "file_buffer_use_ratio";
constexpr char FILE_META_BUFFER_USE_RATIO[] = "file_meta_buffer_use_ratio";
constexpr char BUFFER_BLOCK_SIZE_MB[] = "buffer_block_size_mb";

constexpr char COMMIT_MIN_COPY[] = "commit_min_copy";
constexpr char COMMIT_MAX_COPY[] = "commit_max_copy";
constexpr char COMMIT_BLOCK_SIZE_MB[] = "commit_block_size_mb";
constexpr char COMMIT_INTERVAL_SEC[] = "commit_interval_sec";
constexpr char COMMIT_INTERVAL_FOR_MEMORY_PREFER_TOPIC_SEC[] = "commit_interval_for_memory_perfer_topic_sec";
constexpr char COMMIT_INTERVAL_WHEN_DELAY_SEC[] = "commit_interval_when_delay_sec";
constexpr char COMMIT_INTERVAL_AS_ERROR_MS[] = "commit_interval_as_error_ms";

constexpr char COMMIT_THREAD_NUM[] = "commit_thread_num";
constexpr char COMMIT_QUEUE_SIZE[] = "commit_queue_size";
constexpr char LOAD_THREAD_NUM[] = "load_thread_num";
constexpr char LOAD_QUEUE_SIZE[] = "load_queue_size";
constexpr char UNLOAD_THREAD_NUM[] = "unload_thread_num";
constexpr char UNLOAD_QUEUE_SIZE[] = "unload_queue_size";
constexpr char FILE_SPLIT_SIZE_MB[] = "file_split_size_mb";
constexpr char FILE_SPLIT_MIN_SIZE_MB[] = "file_split_min_size_mb";
constexpr char FILE_SPLIT_INTERVAL_SEC[] = "file_split_interval_sec";
constexpr char CLOSE_NO_WRITE_FILE_SEC[] = "close_no_write_file_sec";
constexpr char FILE_CHANGE_FOR_DFS_ERROR_SEC[] = "file_change_for_dfs_error_sec";

constexpr char CONFIG_UNLIMITED[] = "config_unlimited";
constexpr char OBSOLETE_FILE_TIME_INTERVAL_HOUR[] = "obsolete_file_time_interval_hour";
constexpr char OBSOLETE_FILE_TIME_INTERVAL_SEC[] = "obsolete_file_time_interval_sec";
constexpr char RESERVED_FILE_COUNT[] = "reserved_file_count";
constexpr char DEL_OBSOLETE_FILE_INTERVAL_SEC[] = "del_obsolete_file_interval_sec";
constexpr char RANDOM_DEL_OBSOLETE_FILE_INTERVAL[] = "random_del_obsolete_file_interval";
constexpr char CANDIDATE_OBSOLETE_FILE_INTERVAL_SEC[] = "candidate_obsolete_file_interval_sec";
constexpr char REQUEST_TIMEOUT[] = "request_timeout_sec";
constexpr char MAX_MESSAGE_COUNT_IN_ONE_FILE[] = "max_message_count_in_one_file";
constexpr char CACHE_META_FILE_COUNT[] = "cache_meta_file_count";
constexpr char MAX_WAIT_FILE_LOCK_TIME[] = "max_wait_file_lock_time";
constexpr char CONCURRENT_READ_FILE_LIMIT[] = "concurrent_read_file_limit";
constexpr char MESSAGE_FORMAT[] = "message_format";
constexpr char RECYCLE_PERCENT[] = "recycle_percent";
constexpr char SUPPORT_MERGE_MSG[] = "support_merge_msg";
constexpr char SUPPORT_FB[] = "support_fb";
constexpr char CHECK_FIELD_FILTER_MSG[] = "check_field_filter_msg";
constexpr char ONE_FILE_FD_NUM[] = "one_file_fd_num";
constexpr char CACHE_FILE_RESERVE_TIME[] = "cache_file_reserve_time_sec";
constexpr char CACHE_BLOCK_RESERVE_TIME[] = "cache_block_reserve_time_sec";
constexpr char OBSOLETE_READER_INTERVAL_SEC[] = "obsolete_reader_interval_sec";
constexpr char OBSOLETE_READER_METRIC_INTERVAL_SEC[] = "obsolete_reader_metric_interval_sec";
constexpr char ADMIN_REQUEST_PROCESS_TIME_RATIO[] = "admin_request_process_time_ratio";
constexpr char COMMIT_THREAD_LOOP_INTERVAL_MS[] = "commit_thread_loop_interval_ms";
constexpr char DEFAULT_GROUP_NAME[] = "default"; // 5s
constexpr char READ_QUEUE_SIZE[] = "read_queue_size";
constexpr char STATUS_REPORT_INTERVAL_SEC[] = "status_report_interval_sec";
constexpr char MAX_READ_SIZE_MB_SEC[] = "max_read_size_mb_sec";
constexpr char ENABLE_FAST_RECOVER[] = "enable_fast_recover";
constexpr char HEARTBEAT_INTERVAL_IN_US[] = "heartbeat_interval_in_us";
constexpr char FORBID_CAMPAIGN_TIME_MS[] = "forbid_campaign_time_ms";
constexpr char LONG_PERIOD_INTERVAL_SEC[] = "long_period_interval_sec";
constexpr char TIMESTAMP_OFFSET_IN_US[] = "timestamp_offset_in_us";

// default configs
constexpr uint16_t DEFAULT_AMONITOR_PORT = 10086;
constexpr int DEFAULT_THREAD_NUM = 16;
constexpr int DEFAULT_IO_THREAD_NUM = 1;
constexpr double DEFAULT_READ_THREAD_RATIO = 0.8;
constexpr double DEFAULT_WRITE_THREAD_RATIO = 0.8;
constexpr int DEFAULT_QUEUE_SIZE = 200;
constexpr int DEFAULT_CLEAR_POLLING_THREAD_NUM = 4;
constexpr int DEFAULT_CLEAR_POLLING_QUEUE_SIZE = 2000;
constexpr int DEFAULT_READ_QUEUE_SIZE = 1000;
constexpr int64_t DEFAULT_PARTITION_MIN_BUFFER_SIZE = 8 << 20;   // 8M
constexpr int64_t DEFAULT_PARTITION_MAX_BUFFER_SIZE = 128 << 20; // 128M
constexpr int64_t DEFAULT_BUFFER_BLOCK_SIZE = 1 << 20;           // 1M
constexpr int64_t MAX_BUFFER_BLOCK_SIZE = 31 << 20;              // 31M 5bit
constexpr int64_t DEFAULT_BUFFER_MIN_RESERVE_SIZE = 0;
constexpr double DEFAULT_BUFFER_MIN_RESERVE_RATIO = 0.1; // 1M
constexpr double DEFAULT_FILE_BUFFER_USE_RATIO = 0.05;
constexpr double DEFAULT_FILE_META_BUFFER_USE_RATIO = 0.3;
constexpr int64_t DEFAULT_DFS_COMMIT_BLOCK_SIZE = 16 << 20;                 // 16M
constexpr double DEFAULT_DFS_COMMIT_INTERVAL = 2;                           // 2s
constexpr double DEFAULT_DFS_COMMIT_INTERVAL_FOR_MEMORY_PREFER_TOPIC = 120; // 120s
constexpr double DEFAULT_DFS_COMMIT_INTERVAL_WHEN_DELAY = 600;              // 600s
constexpr double DEFAULT_COMMIT_INTERVAL_AS_ERROR = 5000;                   // 5s
constexpr int DEFAULT_DFS_COMMIT_THREAD_NUM = 10;
constexpr int DEFAULT_DFS_COMMIT_QUEUE_SIZE = 1000;
constexpr int DEFAULT_LOAD_THREAD_NUM = 1;
constexpr int DEFAULT_LOAD_QUEUE_SIZE = 100;
constexpr int DEFAULT_UNLOAD_THREAD_NUM = 1;
constexpr int DEFAULT_UNLOAD_QUEUE_SIZE = 100;
constexpr int64_t DEFAULT_DFS_FILE_SIZE = 2ll << 30;      // 2G
constexpr int64_t DEFAULT_DFS_FILE_MIN_SIZE = 1ll << 28;  // 256M
constexpr int64_t DEFAULT_DFS_FILE_SPLIT_TIME = 1800;     // 30min
constexpr int64_t DEFAULT_CLOSE_NO_WRITE_FILE_TIME = 600; // 10min
constexpr double DEFAULT_FILE_CHANGE_FOR_DFS_ERROR = 300; // 5min
constexpr int64_t DEFAULT_OBSOLETE_FILE_TIME_INTERVAL = std::numeric_limits<int64_t>::max();
constexpr int32_t DEFAULT_RESERVED_FILE_COUNT = std::numeric_limits<int32_t>::max();
constexpr int64_t DEFAULT_DEL_OBSOLETE_FILE_INTERVAL = 1000 * 1000 * 300;                 // 5 min
constexpr int64_t DEFAULT_CANDIDATE_OBSOLETE_FILE_INTERVAL = (int64_t)3600 * 1000 * 1000; // 1 hour
constexpr int64_t DEFAULT_REQUEST_TIMEOUT = (int64_t)30 * 1000 * 1000;                    // 30s
constexpr int64_t DEFAULT_MAX_LONG_POLLING_RETRY_TIMES = 10;
constexpr int64_t DEFAULT_MAX_MESSAGE_COUNT_IN_ONE_FILE = (int64_t)1024 * 1024;
constexpr int64_t DEFAULT_MAX_PERMISSION_WAIT_TIME = (int64_t)10 * 1000 * 1000; // 10s
constexpr uint32_t DEFAULT_CACHE_META_COUNT = 5;
constexpr uint32_t DEFAULT_CONCURRENT_READ_FILE_LIMIT = 5;
constexpr double DEFAULT_DECSION_THRESHOLD = 0.5;
constexpr int32_t DEFAULT_LEADER_LEASE_TIME = 60;                      // 60s
constexpr int32_t DEFAULT_LEADER_LOOP_INTERVAL = 2;                    // 2s
constexpr int64_t DEFAULT_SYNC_ADMIN_INFO_INTERVAL = 2 * 1000 * 1000;  // 2s
constexpr int64_t DEFAULT_SYNC_TOPIC_INFO_INTERVAL = 5 * 1000 * 1000;  // 5s
constexpr int64_t DEFAULT_SYNC_HEARTBEAT_INTERVAL = 300 * 1000 * 1000; // 300s
constexpr int64_t DEFAULT_ADJUST_RESOURCE_DURATION = 20 * 1000;        // 20s
constexpr double DEFAULT_RECYCLE_PERCENT = 0.1;                        // 10%
constexpr bool DEFAULT_SUPPORT_MERGE_MSG = true;
constexpr bool DEFAULT_SUPPORT_FB = true;
constexpr bool DEFAULT_CHECK_FIELD_FILTER_MSG = true;
constexpr int64_t DEFAULT_RELEASE_DEAD_WORKER_TIME = 600; // 600s
constexpr int64_t DEFAULT_TOPIC_RESOURCE_LIMIT = -1;
constexpr int64_t DEFAULT_WORKER_PARTITION_LIMIT = -1;
constexpr uint32_t DEFAULT_ONE_FILE_FD_NUM = 1;
constexpr uint32_t DEFAULT_CACHE_FILE_RESERVE_TIME = 300;
constexpr uint32_t DEFAULT_CACHE_BLOCK_RESERVE_TIME = 120;
constexpr int64_t DEFAULT_MIN_MAX_PARTITION_BUFFER = 256;        // 256 MB
constexpr int64_t DEFAULT_OBSOLETE_READER_INTERVAL = 60000000;   // 1min
constexpr int64_t DEFAULT_OBSOLETE_METRIC_INTERVAL = 60000000;   // 1min
constexpr int64_t DEFAULT_ADMIN_PROCESS_REQUEST_TIME_RATIO = 60; // 60%
constexpr int64_t DEFAULT_COMMIT_THREAD_LOOP_INTERVAL_MS = 20;   // 20ms
constexpr uint32_t DEFAULT_LOG_SAMPLE_COUNT = 100;
constexpr int64_t DEFAULT_NO_DATA_REQUEST_NOTFIY_INTERVAL_MS = 1000;    // 1000 ms
constexpr int32_t DEFAULT_MAX_READ_SIZE_MB_SEC = 128;                   // 128MB/s
constexpr double DEFAULT_ADMIN_ERROR_BROKER_DEAL_RATIO = 0.1;           // 10%
constexpr int64_t DEFAULT_ADMIN_ZFS_TIMEOUT = 5 * 1000 * 1000;          // 5s
constexpr int64_t DEFAULT_ADMIN_COMMIT_DELAY_THRESHOLD = 1800;          // 1800s
constexpr int64_t DEFAULT_BROKER_UNKNOWN_TIMEOUT = 5 * 1000 * 1000;     // 5s
constexpr int64_t DEFAULT_ADMIN_FORCE_SYNC_INTERVAL = 30 * 1000 * 1000; // 30s
constexpr uint32_t DEFAULT_HEARTBEAT_THREAD_NUM = 1;
constexpr uint32_t DEFAULT_HEARTBEAT_QUEUE_SIZE = 1000;
constexpr uint32_t DEFAULT_META_INFO_REPLICATOR_INTERVAL = 30 * 1000 * 1000;
constexpr int64_t DEFAULT_MAX_TOPIC_ACL_SYNC_INTERVAL_US = 10 * 1000 * 1000; // 10s
} // namespace config
} // namespace swift
