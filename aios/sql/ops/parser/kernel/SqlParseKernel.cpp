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
#include "sql/ops/parser/SqlParseKernel.h"

#include <engine/NaviConfigContext.h>
#include <exception>
#include <iosfwd>
#include <utility>
#include <vector>

#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/URLUtil.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/fast_jsonizable_dec.h"
#include "autil/legacy/json.h"
#include "iquan/common/Common.h"
#include "iquan/common/Status.h"
#include "iquan/config/JniConfig.h"
#include "iquan/jni/DynamicParams.h"
#include "iquan/jni/Iquan.h"
#include "iquan/jni/IquanDqlResponse.h"
#include "kmonitor/client/MetricMacro.h"
#include "kmonitor/client/MetricsReporter.h"
#include "kmonitor/client/core/MutableMetric.h"
#include "lockless_allocator/MallocPoolScope.h"
#include "navi/builder/KernelDefBuilder.h"
#include "navi/common.h"
#include "navi/engine/Data.h"
#include "navi/engine/Kernel.h"
#include "navi/engine/KernelComputeContext.h"
#include "navi/engine/KernelConfigContext.h"
#include "sql/common/KvPairParser.h"
#include "sql/common/Log.h"
#include "sql/common/common.h"
#include "sql/data/SqlPlanData.h"
#include "sql/data/SqlPlanType.h"
#include "sql/data/SqlQueryRequest.h"
#include "sql/data/SqlRequestData.h"
#include "sql/data/SqlRequestType.h"
#include "sql/proto/SqlSearchInfoCollector.h"
#include "sql/resource/SqlConfig.h"

namespace kmonitor {
class MetricsTags;
} // namespace kmonitor

namespace navi {
class KernelInitContext;
} // namespace navi

using namespace std;
using namespace autil;
using namespace navi;
using namespace iquan;

namespace sql {

class SqlParseMetrics : public kmonitor::MetricsGroup {
public:
    bool init(kmonitor::MetricsGroupManager *manager) override {
        REGISTER_QPS_MUTABLE_METRIC(_cacheHitQps, "cacheHitQps");
        REGISTER_LATENCY_MUTABLE_METRIC(_planTime, "planTime");
        return true;
    }
    void report(const kmonitor::MetricsTags *tags, const SqlParseMetricsCollector *collector) {
        if (collector->cacheHit) {
            REPORT_MUTABLE_QPS(_cacheHitQps);
        }
        REPORT_MUTABLE_METRIC(_planTime, autil::US_TO_MS(collector->planTime));
    }

private:
    kmonitor::MutableMetric *_cacheHitQps = nullptr;
    kmonitor::MutableMetric *_planTime = nullptr;
};

SqlParseKernel::SqlParseKernel()
    : _enableTurboJet(false) {}

SqlParseKernel::~SqlParseKernel() {}

void SqlParseKernel::def(navi::KernelDefBuilder &builder) const {
    builder.name("sql.SqlParseKernel")
        .input("input0", SqlRequestType::TYPE_ID)
        .output("output0", SqlPlanType::TYPE_ID);
}

bool SqlParseKernel::config(navi::KernelConfigContext &ctx) {
    std::map<std::string, std::vector<std::string>> dbNameAliasMap;
    NAVI_JSONIZE(ctx, "db_name", _dbName, _dbName);
    NAVI_JSONIZE(ctx, "db_name_alias", dbNameAliasMap, dbNameAliasMap);
    for (const auto &pair : dbNameAliasMap) {
        for (const auto &db : pair.second) {
            _dbNameAlias[db] = pair.first;
        }
    }
    NAVI_JSONIZE(ctx, "enable_turbojet", _enableTurboJet, _enableTurboJet);
    return true;
}

navi::ErrorCode SqlParseKernel::init(navi::KernelInitContext &context) {
    return navi::EC_NONE;
}

navi::ErrorCode SqlParseKernel::compute(navi::KernelComputeContext &ctx) {
    DisablePoolScope disableScope;
    ScopedTime2 timer;
    const auto &collector = _sqlSearchInfoCollectorR->getCollector();
    collector->setSqlPlanStartTime(timer.begin_us());

    navi::PortIndex inputIndex(0, navi::INVALID_INDEX);
    bool isEof = false;
    navi::DataPtr data;
    ctx.getInput(inputIndex, data, isEof);
    if (data == nullptr) {
        return navi::EC_ABORT;
    }
    SqlRequestData *sqlRequestData = dynamic_cast<SqlRequestData *>(data.get());
    if (!sqlRequestData) {
        return navi::EC_ABORT;
    }
    SqlQueryRequest *sqlQueryRequest = sqlRequestData->getSqlRequest().get();
    if (!sqlQueryRequest) {
        SQL_LOG(ERROR, "sql request is null");
        return navi::EC_ABORT;
    }
    iquan::SqlPlanPtr sqlPlan;
    iquan::IquanDqlRequestPtr iquanRequest;
    if (!parseSqlPlan(sqlQueryRequest, iquanRequest, sqlPlan)) {
        SQL_LOG(ERROR, "parse sql plan failed, sql [%s]", sqlQueryRequest->getSqlQuery().c_str());
        return navi::EC_ABORT;
    }
    _metricsCollector.planTime = timer.done_us();
    SQL_LOG(DEBUG,
            "parse sql plan use [%ld] us, cacheHit[%d]",
            _metricsCollector.planTime,
            _metricsCollector.cacheHit);
    collector->setSqlPlanTime(_metricsCollector.planTime, _metricsCollector.cacheHit);
    reportMetrics();
    navi::PortIndex index(0, navi::INVALID_INDEX);
    SqlPlanDataPtr sqlPlanData(new SqlPlanData(iquanRequest, sqlPlan));
    ctx.setOutput(index, sqlPlanData, true);
    return navi::EC_NONE;
}

bool SqlParseKernel::parseSqlPlan(const sql::SqlQueryRequest *sqlRequest,
                                  iquan::IquanDqlRequestPtr &iquanRequest,
                                  iquan::SqlPlanPtr &sqlPlan) {
    iquanRequest.reset(new iquan::IquanDqlRequest());
    if (!transToIquanRequest(sqlRequest, *iquanRequest)) {
        string errorMsg = "get invalid sql params";
        SQL_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    iquanRequest->defaultCatalogName = getDefaultCatalogName(sqlRequest);
    iquanRequest->defaultDatabaseName = getDefaultDatabaseName(sqlRequest);
    if (!iquanRequest->isValid()) {
        string errorMsg
            = "iquan request is invalid, content: " + FastToJsonString(iquanRequest, true);
        SQL_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    if (_iquanR == nullptr || _iquanR->getSqlClient() == nullptr) {
        return false;
    }
    iquan::IquanDqlResponse iquanResponse;
    iquan::PlanCacheStatus planCacheStatus;
    iquan::Status status
        = _iquanR->getSqlClient()->query(*iquanRequest, iquanResponse, planCacheStatus);
    if (!status.ok()) {
        string errorMsg = "failed to get sql plan, error message is : " + status.errorMessage();
        SQL_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    } else if (iquanResponse.errorCode != 0) {
        string errorMsg = "generate sql plan failed, iquan error code is "
                          + autil::StringUtil::toString(iquanResponse.errorCode) + "\n"
                          + iquanResponse.errorMsg;
        SQL_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    _metricsCollector.cacheHit = planCacheStatus.planGet;
    sqlPlan.reset(new iquan::SqlPlan(iquanResponse.sqlPlan));
    // reserve for plan optimize
    sqlPlan->innerDynamicParams.reserveOneSqlParams();
    return true;
}

bool SqlParseKernel::transToIquanRequest(const sql::SqlQueryRequest *sqlRequest,
                                         iquan::IquanDqlRequest &iquanRequest) {
    iquanRequest.sqls.push_back(sqlRequest->getSqlQuery());
    // 1. parse dynamic params
    if (!addIquanDynamicParams(sqlRequest, iquanRequest.dynamicParams)) {
        return false;
    }

    // 2. parse iquan params.
    addIquanSqlParams(sqlRequest, iquanRequest);

    // 3. add iquan.plan.format.version
    auto iter = iquanRequest.sqlParams.find(std::string(SQL_IQUAN_PLAN_FORMAT_VERSION));
    if (iter == iquanRequest.sqlParams.end()) {
        iquanRequest.sqlParams[SQL_IQUAN_PLAN_FORMAT_VERSION]
            = string(SQL_DEFAULT_VALUE_IQUAN_PLAN_FORMAT_VERSION);
    }

    // 4. send the suffix of summary table to iquan
    iquanRequest.sqlParams[SQL_IQUAN_OPTIMIZER_TABLE_SUMMARY_SUFFIX] = SUMMARY_TABLE_SUFFIX;
    return true;
}

bool SqlParseKernel::addIquanDynamicParams(const sql::SqlQueryRequest *sqlRequest,
                                           iquan::DynamicParams &dynamicParams) {
    string dynamicParamsStr;
    bool retValue = sqlRequest->getValue(SQL_DYNAMIC_PARAMS, dynamicParamsStr);
    if (!retValue) {
        return true;
    }
    string urlencodeDataStr;
    if (sqlRequest->getValue(SQL_URLENCODE_DATA, urlencodeDataStr) && urlencodeDataStr == "true") {
        dynamicParamsStr = URLUtil::decode(dynamicParamsStr);
    }
    SQL_LOG(TRACE3, "dynamic params str[%s]", dynamicParamsStr.c_str());
    try {
        FastFromJsonString(dynamicParams._array, dynamicParamsStr);
    } catch (const std::exception &e) {
        SQL_LOG(ERROR,
                "parse json string [%s] failed, exception [%s]",
                dynamicParamsStr.c_str(),
                e.what());
        return false;
    }
    SQL_LOG(
        TRACE3, "parsed dynamic params[%s]", FastToJsonString(dynamicParams._array, true).c_str());
    if (!addIquanDynamicKVParams(sqlRequest, dynamicParams)) {
        return false;
    }
    addIquanHintParams(sqlRequest, dynamicParams);
    return true;
}

bool SqlParseKernel::addIquanDynamicKVParams(const sql::SqlQueryRequest *sqlRequest,
                                             iquan::DynamicParams &dynamicParams) {
    string dynamicKVParamsStr;
    bool retValue = sqlRequest->getValue(SQL_DYNAMIC_KV_PARAMS, dynamicKVParamsStr);
    if (!retValue) {
        return true;
    }
    string urlencodeDataStr;
    if (sqlRequest->getValue(SQL_URLENCODE_DATA, urlencodeDataStr) && urlencodeDataStr == "true") {
        dynamicKVParamsStr = URLUtil::decode(dynamicKVParamsStr);
    }
    SQL_LOG(TRACE3, "dynamic kv params str[%s]", dynamicKVParamsStr.c_str());
    try {
        FastFromJsonString(dynamicParams._map, dynamicKVParamsStr);
    } catch (const std::exception &e) {
        SQL_LOG(ERROR,
                "parse dynamic_kv_params [%s] failed, exception [%s]",
                dynamicKVParamsStr.c_str(),
                e.what());
        return false;
    }
    return true;
}

void SqlParseKernel::addIquanHintParams(const sql::SqlQueryRequest *sqlRequest,
                                        iquan::DynamicParams &dynamicParams) {
    string hintParamsStr;
    bool retValue = sqlRequest->getValue(SQL_HINT_PARAMS, hintParamsStr);
    if (!retValue) {
        return;
    }
    KvPairParser::parse(
        hintParamsStr, SQL_HINT_KV_PAIR_SPLIT, SQL_HINT_KV_SPLIT, dynamicParams._hint);
    return;
}

bool SqlParseKernel::addIquanSqlParams(const sql::SqlQueryRequest *sqlRequest,
                                       iquan::IquanDqlRequest &iquanRequest) {
    const std::unordered_map<std::string, std::string> &kvPair = sqlRequest->getSqlParams();
    auto iter = kvPair.begin();
    auto end = kvPair.end();

    for (; iter != end; iter++) {
        if (StringUtil::startsWith(iter->first, SQL_IQUAN_SQL_PARAMS_PREFIX)) {
            iquanRequest.sqlParams[iter->first] = iter->second;
        } else if (StringUtil::startsWith(iter->first, SQL_IQUAN_EXEC_PARAMS_PREFIX)) {
            iquanRequest.execParams[iter->first] = iter->second;
        }
    }
    addUserKv(kvPair, iquanRequest.execParams);
    const auto &sqlConfig = _sqlConfigResource->getSqlConfig();
    if (iquanRequest.sqlParams.find(SQL_IQUAN_PLAN_CACHE_ENABLE) == iquanRequest.sqlParams.end()) {
        iquanRequest.sqlParams[SQL_IQUAN_PLAN_CACHE_ENABLE]
            = sqlConfig.iquanPlanCacheEnable ? string("true") : string("false");
    }
    if (iquanRequest.sqlParams.find(SQL_IQUAN_PLAN_PREPARE_LEVEL) == iquanRequest.sqlParams.end()) {
        iquanRequest.sqlParams[SQL_IQUAN_PLAN_PREPARE_LEVEL] = sqlConfig.iquanPlanPrepareLevel;
    }
    if (iquanRequest.sqlParams.find(SQL_IQUAN_CTE_OPT_VER) == iquanRequest.sqlParams.end()) {
        iquanRequest.sqlParams[SQL_IQUAN_CTE_OPT_VER] = sqlConfig.cteOptVer;
    }

    if (iquanRequest.sqlParams.find(SQL_IQUAN_OPTIMIZER_TURBOJET_ENABLE)
        == iquanRequest.sqlParams.end()) {
        iquanRequest.sqlParams[SQL_IQUAN_OPTIMIZER_TURBOJET_ENABLE] = _enableTurboJet;
    }

    if (iquanRequest.execParams.find(SQL_IQUAN_EXEC_PARAMS_SOURCE_ID)
        == iquanRequest.execParams.end()) {
        iquanRequest.execParams[SQL_IQUAN_EXEC_PARAMS_SOURCE_ID]
            = StringUtil::toString(TimeUtility::currentTimeInNanoSeconds());
    }
    if (iquanRequest.execParams.find(SQL_IQUAN_EXEC_PARAMS_SOURCE_SPEC)
        == iquanRequest.execParams.end()) {
        iquanRequest.execParams[SQL_IQUAN_EXEC_PARAMS_SOURCE_SPEC] = SQL_SOURCE_SPEC_EMPTY;
    }
    if (iquanRequest.execParams.find(SQL_IQUAN_EXEC_PARAMS_TASK_QUEUE)
        == iquanRequest.execParams.end()) {
        iquanRequest.execParams[SQL_IQUAN_EXEC_PARAMS_TASK_QUEUE] = sqlConfig.sqlRuntimeTaskQueue;
    }

    return true;
}

void SqlParseKernel::addUserKv(const std::unordered_map<std::string, std::string> &kvPair,
                               map<string, string> &execParams) {
    string userKv;
    auto iter = kvPair.find(SQL_FORBIT_MERGE_SEARCH_INFO);
    if (iter != kvPair.end()) {
        userKv = iter->first + "#" + iter->second + ",";
    }
    iter = kvPair.find(SQL_FORBIT_SEARCH_INFO);
    if (iter != kvPair.end()) {
        userKv = iter->first + "#" + iter->second + ",";
    }
    if (!userKv.empty()) {
        auto iter2 = execParams.find(SQL_IQUAN_EXEC_PARAMS_USER_KV);
        if (iter2 != execParams.end()) {
            iter2->second = iter2->second + "," + userKv;
        } else {
            execParams[SQL_IQUAN_EXEC_PARAMS_USER_KV] = userKv;
        }
    }
}

std::string SqlParseKernel::getDefaultCatalogName(const sql::SqlQueryRequest *sqlRequest) {
    std::string catalogName;
    if (sqlRequest->getValue(SQL_CATALOG_NAME, catalogName)) {
        return catalogName;
    }
    if (!_sqlConfigResource->getSqlConfig().catalogName.empty()) {
        return _sqlConfigResource->getSqlConfig().catalogName;
    }
    return SQL_DEFAULT_CATALOG_NAME;
}

std::string SqlParseKernel::getDefaultDatabaseName(const sql::SqlQueryRequest *sqlRequest) {
    std::string databaseName("");
    if (!sqlRequest->getValue(SQL_DATABASE_NAME, databaseName)) {
        if (!_dbName.empty()) {
            databaseName = _dbName;
        }
    }
    auto it = _dbNameAlias.find(databaseName);
    if (it != _dbNameAlias.end()) {
        databaseName = it->second;
    }
    return databaseName;
}

bool SqlParseKernel::getOutputSqlPlan(const sql::SqlQueryRequest *sqlRequest) {
    if (sqlRequest == nullptr) {
        return false;
    }
    std::string info;
    sqlRequest->getValue(SQL_GET_SQL_PLAN, info);
    if (info.empty()) {
        return false;
    }
    bool res = false;
    StringUtil::fromString(info, res);
    return res;
}

void SqlParseKernel::reportMetrics() const {
    if (_queryMetricReporterR) {
        static const std::string path = "sql.builtin.ops.parse";
        auto opMetricsReporter = _queryMetricReporterR->getReporter()->getSubReporter(path, {});
        opMetricsReporter->report<SqlParseMetrics>(nullptr, &_metricsCollector);
    }
}

REGISTER_KERNEL(SqlParseKernel);

} // namespace sql
