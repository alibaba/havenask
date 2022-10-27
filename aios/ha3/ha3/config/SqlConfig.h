#ifndef ISEARCH_SQLCONFIG_H
#define ISEARCH_SQLCONFIG_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/legacy/jsonizable.h>
#include <multi_call/config/MultiCallConfig.h>
#include <ha3/config/SqlAggPluginConfig.h>
#include <ha3/config/SqlTvfPluginConfig.h>
#include <iquan_common/config/ClientConfig.h>
#include <iquan_common/config/JniConfig.h>
#include <iquan_common/config/WarmupConfig.h>
#include <iquan_common/config/KMonConfig.h>

BEGIN_HA3_NAMESPACE(config);

static const size_t DEFAULT_PARALLEL_NUM = 1;
static const size_t DEFAULT_SQL_TIMEOUT = 1000; // ms
static const double DEFAULT_SQL_TIMEOUT_FACTOR = 0.5;
static const double DEFAULT_SUB_GRAPH_TIMEOUT_FACTOR = 0.9;
static const uint32_t DEFAULT_SUB_GRAPH_THREAD_LIMIT = 10;
static const uint32_t DEFAULT_MAIN_GRAPH_THREAD_LIMIT = 5;
static const std::string DEFAULT_IQUAN_PLAN_PREPARE_LEVEL = "jni.post.optimize";

class SqlConfig : public autil::legacy::Jsonizable {
public:
    SqlConfig()
        : parallelNum(DEFAULT_PARALLEL_NUM)
        , sqlTimeout(DEFAULT_SQL_TIMEOUT)
        , logSearchInfoThreshold(0)
        , timeoutFactor(DEFAULT_SQL_TIMEOUT_FACTOR)
        , subGraphTimeoutFactor(DEFAULT_SUB_GRAPH_TIMEOUT_FACTOR)
        , subGraphThreadLimit(DEFAULT_SUB_GRAPH_THREAD_LIMIT)
        , mainGraphThreadLimit(DEFAULT_MAIN_GRAPH_THREAD_LIMIT)
        , lackResultEnable(false)
        , iquanPlanPrepareLevel(DEFAULT_IQUAN_PLAN_PREPARE_LEVEL)
        , iquanPlanCacheEnable(false)
    {}
    ~SqlConfig() {}
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
        json.Jsonize("catalog_name", catalogName, catalogName);
        json.Jsonize("db_name", dbName, dbName);
        json.Jsonize("output_format", outputFormat, outputFormat);
        json.Jsonize("parallel_num", parallelNum, parallelNum);
        json.Jsonize("sql_timeout", sqlTimeout, sqlTimeout);
        json.Jsonize("log_search_info_threshold", logSearchInfoThreshold, logSearchInfoThreshold);
        json.Jsonize("timeout_factor", timeoutFactor, timeoutFactor);
        json.Jsonize("sub_graph_timeout_factor", subGraphTimeoutFactor, subGraphTimeoutFactor);
        json.Jsonize("sub_graph_thread_limit", subGraphThreadLimit, subGraphThreadLimit);
        json.Jsonize("main_graph_thread_limit", mainGraphThreadLimit, mainGraphThreadLimit);
        json.Jsonize("lack_result_enable", lackResultEnable, lackResultEnable);
        json.Jsonize("iquan_plan_prepare_level", iquanPlanPrepareLevel, iquanPlanPrepareLevel);
        json.Jsonize("iquan_plan_cache_enable", iquanPlanCacheEnable, iquanPlanCacheEnable);
        json.Jsonize("parallel_tables", parallelTables, parallelTables);


        json.Jsonize("sql_agg_plugin_config", sqlAggPluginConfig, sqlAggPluginConfig);
        json.Jsonize("sql_tvf_plugin_config", sqlTvfPluginConfig, sqlTvfPluginConfig);
        json.Jsonize("iquan_client_config", clientConfig, clientConfig);
        json.Jsonize("iquan_jni_config", jniConfig, jniConfig);
        json.Jsonize("iquan_warmup_config", warmupConfig, warmupConfig);
        json.Jsonize("iquan_kmon_config", kmonConfig, kmonConfig);
    }
public:
    std::string outputFormat;
    std::string catalogName;
    std::string dbName;
    size_t parallelNum;
    size_t sqlTimeout;
    size_t logSearchInfoThreshold;
    double timeoutFactor;
    double subGraphTimeoutFactor;
    uint32_t subGraphThreadLimit;
    uint32_t mainGraphThreadLimit;
    bool lackResultEnable;
    std::string iquanPlanPrepareLevel;
    bool iquanPlanCacheEnable;
    std::vector<std::string> parallelTables;

    SqlAggPluginConfig sqlAggPluginConfig;
    SqlTvfPluginConfig sqlTvfPluginConfig;
    iquan::ClientConfig clientConfig;
    iquan::JniConfig jniConfig;
    iquan::WarmupConfig warmupConfig;
    iquan::KMonConfig kmonConfig;
    // todo: add iquan::ExecConfig
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(SqlConfig);

END_HA3_NAMESPACE(config);

#endif //ISEARCH_SQLCONFIG_H
