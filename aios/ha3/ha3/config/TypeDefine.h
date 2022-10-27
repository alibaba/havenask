#ifndef ISEARCH_TYPEDEFINE_H
#define ISEARCH_TYPEDEFINE_H

#include <ha3/common.h>

namespace isearch {

static const double TIMEOUT_PERCENTAGE = 0.95; // 95%
static const double SEEK_TIMEOUT_PERCENTAGE =  0.70; // 70%

static const int32_t QRS_ARPC_CONNECTION_TIMEOUT = 60000; //ms
static const uint64_t QRS_ARPC_REQUEST_QUEUE_SIZE = 100;
static const uint64_t QRS_RETURN_HITS_LIMIT = 5000;
static const uint64_t QRS_REPLICA_COUNT = 1;
static const uint32_t THREAD_INIT_ROUND_ROBIN = 8;
static const uint32_t RETURN_HIT_REWRITE_THRESHOLD = 320;
static const double RETURN_HIT_REWRITE_RATIO = -1.0;
static const size_t POOL_TRUNK_SIZE = 10; // 10 M
static const size_t POOL_RECYCLE_SIZE_LIMIT = 20; // 20 M
static const size_t POOL_MAX_COUNT = 200;
static const size_t CAVA_ALLOC_SIZE_LIMIT = 40; // 40 M
static const size_t CAVA_INIT_SIZE_LIMIT = 1024; // 1024 M
static const size_t CAVA_MODULE_CACHE_SIZE_LIMIT = 500000;
static const size_t CAVA_MODULE_CACHE_SIZE_DEFAULT = 100000;
static const std::string HA3_CAVA_CONF = "../binary/usr/local/etc/ha3/ha3_cava_config.json";
static const std::string HA3_CAVA_PLUGINS = "../binary/usr/local/share/cava/plugins";
static const std::string SQL_FUNCTION_CONF = "/usr/local/etc/ha3/sql_function.json";
static const std::string DEFAULT_RESOURCE_TYPE = "default";

static const uint64_t PROXY_ARPC_CONNECTION_TIMEOUT = 60000; //ms
static const uint64_t PROXY_ARPC_REQUEST_QUEUE_SIZE = 100;

static const std::string USER_NAME = "UserName";
static const std::string SERVICE_NAME = "ServiceName";

static const std::string QRS_ROLE_NAME = "QueryResultServerRole";
static const std::string PROXY_ROLE_NAME = "ProxyRole";
static const std::string SEARCHER_ROLE_NAME = "SearcherRole";
static const std::string ADMIN_ROLE_NAME = "AdminRole";
static const std::string INDEX_ROLE_NAME = "IndexRole";

//////////// Define Config Filename & section //////////////////
static const std::string HA3_VERSION_CONFIG_FILE_NAME = "ha3_version.json";
static const std::string HA_CONFIG_FILE_NAME = "ha.conf";
static const std::string ZONE_CONF_JSON_FILE = "zone_conf.json";
static const std::string CLUSTER_CONFIG_DIR_NAME = "zones";
static const std::string CLUSTER_JSON_FILE_SUFFIX = "_biz.json";
static const std::string BIZ_JSON_FILE = "biz.json";
static const std::string QRS_BIZ_JSON_FILE = "qrs_biz.json";
static const std::string DEFAULT_BIZ_NAME = "default";
static const std::string DEFAULT_SQL_BIZ_NAME = "default_sql";
static const std::string CLUSTER_CONFIG_INFO_SECTION = "cluster_config";
static const std::string BUILD_CONFIG_DIR_NAME = "build";
static const std::string BUILD_JSON_FILE_SUFFIX = "_build.json";
static const std::string QUERY_INFO_SECTION = "query_config";
static const std::string DOC_PROCESS_CHAIN_CONFIG_SECTION = "doc_process_chain_config";
static const std::string MODULE_TAG = "modules";
static const std::string DOC_PROCESSOR_CHAIN_TAG = "document_processor_chain";
static const std::string SCHEMA_JSON_FILE_SUFFIX = "_schema.json";
static const std::string SCHEMA_CONFIG_PATH = "schemas";
static const std::string TABLE_CLUSTER_JSON_FILE_SUFFIX = "_cluster.json";
static const std::string TABLE_CLUSTER_CONFIG_PATH = "clusters";
static const std::string ANALYZER_CONFIG_FILE = "analyzer.json";
static const std::string ALITOKENIZER_CONF_FILE = "AliTokenizer.conf";
static const std::string QRS_CONFIG_JSON_FILE = "qrs.json";
static const std::string SQL_CONFIG_JSON_FILE = "sql.json";
static const std::string SQL_FUNCTION_JSON_FILE_SUFFIX = "_function.json";
static const std::string SQL_LOGICTABLE_JSON_FILE_SUFFIX = "_logictable.json";
static const std::string SQL_LOGICTABLE_TYPE = "logical";
static const std::string SQL_CONFIG_PATH = "sql";
static const std::string QRS_RULE_INFO_SECTION = "qrs_rule";
static const std::string QRS_QUERY_CACHE_INFO_SECTION = "qrs_query_cache";
static const std::string QRS_RESULT_COMPRESS = "qrs_result_compress";
static const std::string QRS_REQUEST_COMPRESS = "qrs_request_compress";
static const std::string RANK_PROFILE_SECTION = "rankprofile_config";
static const std::string SUMMARY_CONFIG_SECTION = "summary_profile_config";
static const std::string CONFIGURATIONS_DIRNAME = "configurations";
static const std::string CONFIGURATION_VERSION_PREFIX = "configuration";
static const std::string PLUGIN_PATH = "plugins/";
static const std::string ACTIVE_FILENAME = "active";
static const std::string INDEX_DATA_DIRNAME = "runtimedata";
static const std::string INDEX_VERSION_PREFIX = "generation";
static const std::string PARTITION_PREFIX = "partition";
static const std::string ACTIVE_FILE_NAME = "active";
static const std::string NAME_SPLITER = "_";
static const std::string INHERIT_FROM_SECTION = "inherit_from";
static const std::string INHERIT_FROM_KEY = "inherit_from";
static const std::string INDEX_OPTION_CONFIG_SECTION = "index_option_config";
static const std::string BUILD_OPTION_CONFIG_SECTION = "build_option_config";
static const std::string AGGREGATE_SAMPLER_CONFIG_SECTION = "aggregate_sampler_config";
static const std::string FUNCTION_CONFIG_SECTION = "function_config";
static const std::string SEARCHER_CACHE_CONFIG_SECTION = "searcher_cache_config";
static const std::string CAVA_CONFIG_SECTION = "cava_config";
static const std::string TURING_OPTIONS_SECTION = "turing_options_config";
static const std::string FINAL_SORTER_CONFIG_SECTION = "final_sorter_config";
static const std::string SEARCH_OPTIMIZER_CONFIG_SECTION = "search_optimizer_config";
static const std::string SERVICE_DEGRADATION_CONFIG_SECTION = "service_degradation_config";
static const std::string ANOMALY_PROCESS_CONFIG_SECTION = "multi_call_config";
static const std::string ANOMALY_PROCESS_CONFIG_SECTION_T = "multi_call_config_t";
static const std::string ANOMALY_PROCESS_CONFIG_SECTION_SQL = "multi_call_config_sql";
static const std::string ADMIN_CONFIG_JSON_FILE = "admin.json";
static const std::string HIPPO_CONFIG_JSON_FILE = "hippo.json";
static const std::string SWIFT_TOPIC_NAME = "swift_topic_name";
static const std::string SWIFT_ZOOKEEPER_ROOT = "swift_zookeeper_root";
static const std::string SWIFT_MESSAGE_MASK = "swift_message_mask";
static const std::string SWIFT_MESSAGE_MASK_RESULT = "swift_message_mask_result";
static const std::string SWIFT_READER_CONFIG = "swift_reader_config";
static const std::string SWIFT_FILTER_MODE = "swift_filter_mode";
static const std::string SWIFT_FILTER_NORMAL_MODE = "normal_mode";
static const std::string SWIFT_FILTER_GENERATION_MODE = "generation_mode";
static const std::string SWIFT_FILTER_GENERATION_META_MODE = "generation_meta_mode";
static const std::string SWIFT_NEED_FIELD_FILTER = "need_field_filter";
static const std::string SWIFT_REQUIRED_FIELD_NAMES = "required_field_names";
static const std::string ZONE_BIZ_NAME_SPLITTER = ".";
static const std::string HA3_DEFAULT_AGG_PREFIX = "default_agg_";
static const std::string HA3_DEFAULT_AGG = "default_agg";
static const std::string HA3_PARA_SEARCH_PREFIX = "para_search_";
static const std::string HA3_DEFAULT_SQL = "default_sql";
static const std::string HA3_USER_SEARCH = "user_search";

//////////// Define index status /////////////////////
static const std::string INDEX_STATUS_AVALIABLE = "available";
static const std::string INDEX_STATUS_BAD = "bad";

static const std::string INDEX_STATUS_FILE_NAME = "index_status";

static const std::string SEARCHER_BIN_NAME = "searcher_worker";
static const std::string INDEXHOLDER_BIN_NAME = "index_worker";
static const std::string PROXY_BIN_NAME = "proxy_worker";
static const std::string QRS_BIN_NAME = "qrs_worker";

static const std::string DEFAULT_QRS_HEALTHCHECK_FILE = "qrs.status";

#define JSONIZE(json, configName, value)        \
    json.Jsonize(configName, value, value)

};

#endif //ISEARCH_TYPEDEFINE_H
