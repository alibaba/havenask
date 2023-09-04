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

#include <stddef.h>
#include <stdint.h>

namespace isearch {

constexpr double TIMEOUT_PERCENTAGE = 0.95;      // 95%
constexpr double SEEK_TIMEOUT_PERCENTAGE = 0.70; // 70%

constexpr int32_t QRS_ARPC_CONNECTION_TIMEOUT = 60000; // ms
constexpr uint64_t QRS_ARPC_REQUEST_QUEUE_SIZE = 100;
constexpr uint64_t QRS_RETURN_HITS_LIMIT = 5000;
constexpr uint64_t QRS_REPLICA_COUNT = 1;
constexpr uint32_t THREAD_INIT_ROUND_ROBIN = 8;
constexpr uint32_t RETURN_HIT_REWRITE_THRESHOLD = 320;
constexpr double RETURN_HIT_REWRITE_RATIO = -1.0;
constexpr size_t POOL_TRUNK_SIZE = 10;         // 10 M
constexpr size_t POOL_RECYCLE_SIZE_LIMIT = 20; // 20 M
constexpr size_t POOL_MAX_COUNT = 200;
constexpr size_t CAVA_ALLOC_SIZE_LIMIT = 40;  // 40 M
constexpr size_t CAVA_INIT_SIZE_LIMIT = 1024; // 1024 M
constexpr size_t CAVA_MODULE_CACHE_SIZE_LIMIT = 500000;
constexpr size_t CAVA_MODULE_CACHE_SIZE_DEFAULT = 100000;
constexpr char HA3_CAVA_CONF[] = "../binary/usr/local/etc/ha3/ha3_cava_config.json";
constexpr char HA3_CAVA_PLUGINS[] = "../binary/usr/local/share/cava/plugins";
constexpr char SQL_FUNCTION_CONF[] = "/usr/local/etc/ha3/sql_function.json";
constexpr char DEFAULT_RESOURCE_TYPE[] = "default";

constexpr uint64_t PROXY_ARPC_CONNECTION_TIMEOUT = 60000; // ms
constexpr uint64_t PROXY_ARPC_REQUEST_QUEUE_SIZE = 100;

constexpr char USER_NAME[] = "UserName";
constexpr char SERVICE_NAME[] = "ServiceName";

constexpr char QRS_ROLE_NAME[] = "QueryResultServerRole";
constexpr char PROXY_ROLE_NAME[] = "ProxyRole";
constexpr char SEARCHER_ROLE_NAME[] = "SearcherRole";
constexpr char ADMIN_ROLE_NAME[] = "AdminRole";
constexpr char INDEX_ROLE_NAME[] = "IndexRole";

//////////// Define Config Filename & section //////////////////
constexpr char HA3_VERSION_CONFIG_FILE_NAME[] = "ha3_version.json";
constexpr char HA_CONFIG_FILE_NAME[] = "ha.conf";
constexpr char ZONE_CONF_JSON_FILE[] = "zone_conf.json";
constexpr char CLUSTER_CONFIG_DIR_NAME[] = "zones";
constexpr char CLUSTER_JSON_FILE_SUFFIX[] = "_biz.json";
constexpr char BIZ_JSON_FILE[] = "biz.json";
constexpr char QRS_BIZ_JSON_FILE[] = "qrs_biz.json";
constexpr char DEFAULT_BIZ_NAME[] = "default";
constexpr char DEFAULT_SQL_BIZ_NAME[] = "default_sql";
constexpr char DEFAULT_QRS_SQL_BIZ_NAME[] = "qrs.default_sql";
constexpr char CLUSTER_CONFIG_INFO_SECTION[] = "cluster_config";
constexpr char BUILD_CONFIG_DIR_NAME[] = "build";
constexpr char BUILD_JSON_FILE_SUFFIX[] = "_build.json";
constexpr char QUERY_INFO_SECTION[] = "query_config";
constexpr char DOC_PROCESS_CHAIN_CONFIG_SECTION[] = "doc_process_chain_config";
constexpr char MODULE_TAG[] = "modules";
constexpr char DOC_PROCESSOR_CHAIN_TAG[] = "document_processor_chain";
constexpr char SCHEMA_JSON_FILE_SUFFIX[] = "_schema.json";
constexpr char SCHEMA_CONFIG_PATH[] = "schemas";
constexpr char TABLE_CLUSTER_JSON_FILE_SUFFIX[] = "_cluster.json";
constexpr char TABLE_CLUSTER_CONFIG_PATH[] = "clusters";
constexpr char ANALYZER_CONFIG_FILE[] = "analyzer.json";
constexpr char ALITOKENIZER_CONF_FILE[] = "AliTokenizer.conf";
constexpr char QRS_CONFIG_JSON_FILE[] = "qrs.json";
constexpr char SQL_CONFIG_JSON_FILE[] = "sql.json";
constexpr char SQL_FUNCTION_JSON_FILE_SUFFIX[] = "_function.json";
constexpr char SQL_LOGICTABLE_JSON_FILE_SUFFIX[] = "_logictable.json";
constexpr char SQL_LAYER_TABLE_JSON_FILE_SUFFIX[] = "_layer_table.json";
constexpr char SQL_REMOTE_TABLE_JSON_FILE_SUFFIX[] = "_remote_table.json";
constexpr char SQL_LOGICTABLE_TYPE[] = "logical";
constexpr char SQL_CONFIG_PATH[] = "sql";
constexpr char QRS_RULE_INFO_SECTION[] = "qrs_rule";
constexpr char QRS_QUERY_CACHE_INFO_SECTION[] = "qrs_query_cache";
constexpr char QRS_RESULT_COMPRESS[] = "qrs_result_compress";
constexpr char QRS_REQUEST_COMPRESS[] = "qrs_request_compress";
constexpr char RANK_PROFILE_SECTION[] = "rankprofile_config";
constexpr char SUMMARY_CONFIG_SECTION[] = "summary_profile_config";
constexpr char PLUGIN_PATH[] = "plugins/";
constexpr char ACTIVE_FILENAME[] = "active";
constexpr char INDEX_DATA_DIRNAME[] = "runtimedata";
constexpr char INDEX_VERSION_PREFIX[] = "generation";
constexpr char PARTITION_PREFIX[] = "partition";
constexpr char ACTIVE_FILE_NAME[] = "active";
constexpr char NAME_SPLITER[] = "_";
constexpr char INHERIT_FROM_SECTION[] = "inherit_from";
constexpr char INHERIT_FROM_KEY[] = "inherit_from";
constexpr char INDEX_OPTION_CONFIG_SECTION[] = "index_option_config";
constexpr char BUILD_OPTION_CONFIG_SECTION[] = "build_option_config";
constexpr char AGGREGATE_SAMPLER_CONFIG_SECTION[] = "aggregate_sampler_config";
constexpr char FUNCTION_CONFIG_SECTION[] = "function_config";
constexpr char SEARCHER_CACHE_CONFIG_SECTION[] = "searcher_cache_config";
constexpr char CAVA_CONFIG_SECTION[] = "cava_config";
constexpr char TURING_OPTIONS_SECTION[] = "turing_options_config";
constexpr char FINAL_SORTER_CONFIG_SECTION[] = "final_sorter_config";
constexpr char SEARCH_OPTIMIZER_CONFIG_SECTION[] = "search_optimizer_config";
constexpr char SERVICE_DEGRADATION_CONFIG_SECTION[] = "service_degradation_config";
constexpr char ANOMALY_PROCESS_CONFIG_SECTION[] = "multi_call_config";
constexpr char ANOMALY_PROCESS_CONFIG_SECTION_T[] = "multi_call_config_t";
constexpr char ANOMALY_PROCESS_CONFIG_SECTION_SQL[] = "multi_call_config_sql";
constexpr char ADMIN_CONFIG_JSON_FILE[] = "admin.json";
constexpr char HIPPO_CONFIG_JSON_FILE[] = "hippo.json";
constexpr char SWIFT_TOPIC_NAME[] = "swift_topic_name";
constexpr char SWIFT_ZOOKEEPER_ROOT[] = "swift_zookeeper_root";
constexpr char SWIFT_MESSAGE_MASK[] = "swift_message_mask";
constexpr char SWIFT_MESSAGE_MASK_RESULT[] = "swift_message_mask_result";
constexpr char SWIFT_READER_CONFIG[] = "swift_reader_config";
constexpr char SWIFT_FILTER_MODE[] = "swift_filter_mode";
constexpr char SWIFT_FILTER_NORMAL_MODE[] = "normal_mode";
constexpr char SWIFT_FILTER_GENERATION_MODE[] = "generation_mode";
constexpr char SWIFT_FILTER_GENERATION_META_MODE[] = "generation_meta_mode";
constexpr char SWIFT_NEED_FIELD_FILTER[] = "need_field_filter";
constexpr char SWIFT_REQUIRED_FIELD_NAMES[] = "required_field_names";
constexpr char ZONE_BIZ_NAME_SPLITTER[] = ".";
constexpr char HA3_DEFAULT_AGG_PREFIX[] = "default_agg_";
constexpr char HA3_DEFAULT_AGG[] = "default_agg";
constexpr char HA3_PARA_SEARCH_PREFIX[] = "para_search_";
constexpr char HA3_USER_SEARCH[] = "user_search";
constexpr char HA3_LOCAL_QRS_BIZ_NAME_SUFFIX[] = "_qrs_local";
constexpr char BIZ_TYPE[] = "biz_type";
constexpr char MODEL_BIZ_TYPE[] = "model_biz";
constexpr char SQL_BIZ_TYPE[] = "sql_biz";
constexpr char SEARCHER_SQL_BIZ_TYPE[] = "sql_searcher_biz";
constexpr char SEARCHER_BIZ_TYPE[] = "searcher_biz";
constexpr char AGG_BIZ_TYPE[] = "agg_biz";
constexpr char PARA_BIZ_TYPE[] = "para_biz";

//////////// Define index status /////////////////////
constexpr char INDEX_STATUS_AVALIABLE[] = "available";
constexpr char INDEX_STATUS_BAD[] = "bad";

constexpr char INDEX_STATUS_FILE_NAME[] = "index_status";

constexpr char SEARCHER_BIN_NAME[] = "searcher_worker";
constexpr char INDEXHOLDER_BIN_NAME[] = "index_worker";
constexpr char PROXY_BIN_NAME[] = "proxy_worker";
constexpr char QRS_BIN_NAME[] = "qrs_worker";

constexpr char DEFAULT_QRS_HEALTHCHECK_FILE[] = "qrs.status";

#define JSONIZE(json, configName, value) json.Jsonize(configName, value, value)

}; // namespace isearch
