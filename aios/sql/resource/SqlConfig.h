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

#include <algorithm>
#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <stddef.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "autil/legacy/jsonizable.h"
#include "iquan/common/catalog/LayerTableModel.h"
#include "iquan/common/catalog/TableModel.h"

namespace sql {

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
        , resultAllowSoftFailure(false)
        , iquanPlanPrepareLevel(DEFAULT_IQUAN_PLAN_PREPARE_LEVEL)
        , iquanPlanCacheEnable(true)
        , innerDocIdOptimizeEnable(false)
        , enableScanTimeout(true)
        , asyncScanConcurrency(std::numeric_limits<size_t>::max())
        , targetWatermarkTimeoutRatio(0.5f)
        , cteOptVer(0)
        , slowQueryFactory(-1)
        , needPrintSlowLog(false)
        , needPrintErrorLog(false)
        , enableTurboJet(false) {}

    ~SqlConfig() {}

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
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
        json.Jsonize("result_allow_soft_failure", resultAllowSoftFailure, resultAllowSoftFailure);
        json.Jsonize("iquan_plan_prepare_level", iquanPlanPrepareLevel, iquanPlanPrepareLevel);
        json.Jsonize("iquan_plan_cache_enable", iquanPlanCacheEnable, iquanPlanCacheEnable);
        json.Jsonize(
            "inner_docid_optimize_enable", innerDocIdOptimizeEnable, innerDocIdOptimizeEnable);
        json.Jsonize("parallel_tables", parallelTables, parallelTables);
        json.Jsonize("summary_tables", summaryTables, summaryTables);
        json.Jsonize("sql_runtime_task_queue", sqlRuntimeTaskQueue, sqlRuntimeTaskQueue);
        json.Jsonize("logic_tables", logicTables, logicTables);
        json.Jsonize("layer_tables", layerTables, layerTables);
        json.Jsonize("remote_tables", remoteTables, remoteTables);
        json.Jsonize("enable_scan_timeout", enableScanTimeout, enableScanTimeout);
        json.Jsonize("async_scan_concurrency", asyncScanConcurrency, asyncScanConcurrency);
        json.Jsonize("target_watermark_timeout_ratio",
                     targetWatermarkTimeoutRatio,
                     targetWatermarkTimeoutRatio);
        json.Jsonize("cte_opt_ver", cteOptVer, cteOptVer);
        json.Jsonize("need_print_slow_log_factory", slowQueryFactory, slowQueryFactory);
        needPrintSlowLog = slowQueryFactory > 0.0;
        json.Jsonize("need_print_error_log", needPrintErrorLog, needPrintErrorLog);
        json.Jsonize("enable_turbojet", enableTurboJet, enableTurboJet);
        std::map<std::string, std::vector<std::string>> dbNameAliasMap;
        json.Jsonize("db_name_alias", dbNameAliasMap, dbNameAliasMap);
        for (const auto &pair : dbNameAliasMap) {
            for (const auto &db : pair.second) {
                dbNameAlias[db] = pair.first;
            }
        }
        json.Jsonize("table_name_alias", tableNameAlias, tableNameAlias);
    }

public:
    std::string catalogName;
    std::string dbName;
    std::string outputFormat;
    size_t parallelNum;
    size_t sqlTimeout;
    size_t logSearchInfoThreshold;
    double timeoutFactor;
    double subGraphTimeoutFactor;
    uint32_t subGraphThreadLimit;
    uint32_t mainGraphThreadLimit;
    bool resultAllowSoftFailure;
    std::string iquanPlanPrepareLevel;
    bool iquanPlanCacheEnable;
    bool innerDocIdOptimizeEnable;
    std::vector<std::string> parallelTables;
    std::vector<std::string> summaryTables;
    std::string sqlRuntimeTaskQueue;
    std::vector<iquan::TableModel> logicTables;
    std::vector<iquan::LayerTableModel> layerTables;
    std::vector<iquan::TableModel> remoteTables;
    bool enableScanTimeout;
    size_t asyncScanConcurrency;
    double targetWatermarkTimeoutRatio;
    int cteOptVer;
    double slowQueryFactory;
    bool needPrintSlowLog;
    bool needPrintErrorLog;
    bool enableTurboJet;
    std::map<std::string, std::string> dbNameAlias;
    std::map<std::string, std::vector<std::string>> tableNameAlias;
};

typedef std::shared_ptr<SqlConfig> SqlConfigPtr;

} // namespace sql
