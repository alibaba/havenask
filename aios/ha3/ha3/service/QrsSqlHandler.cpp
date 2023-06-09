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
#include "ha3/service/QrsSqlHandler.h"

#include <algorithm>
#include <cstddef>
#include <map>
#include <memory>
#include <unordered_map>
#include <utility>

#include "aios/network/gig/multi_call/common/ErrorCode.h"
#include "aios/network/gig/multi_call/rpc/GigClosure.h"
#include "aios/network/gig/multi_call/util/FileRecorder.h"
#include "alog/Logger.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/URLUtil.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "autil/mem_pool/Pool.h"
#include "ha3/common/ErrorResult.h"
#include "ha3/common/TimeoutTerminator.h"
#include "ha3/common/Tracer.h"
#include "ha3/config/SqlConfig.h"
#include "ha3/monitor/SessionMetricsCollector.h"
#include "ha3/proto/BasicDefs.pb.h"
#include "ha3/sql/common/KvPairParser.h"
#include "ha3/sql/common/TracerAdapter.h"
#include "ha3/sql/common/common.h"
#include "ha3/sql/config/NaviConfigAdapter.h"
#include "ha3/sql/data/SqlPlanData.h"
#include "ha3/sql/data/SqlQueryRequest.h"
#include "ha3/sql/data/SqlRequestData.h"
#include "ha3/sql/data/TableData.h"
#include "ha3/sql/framework/QrsSessionSqlRequest.h"
#include "ha3/sql/framework/QrsSessionSqlResult.h"
#include "ha3/sql/ops/agg/SqlAggPluginConfig.h"
#include "ha3/sql/proto/SqlSearchInfo.pb.h"
#include "ha3/sql/proto/SqlSearchInfoCollector.h"
#include "ha3/sql/resource/MetaInfoResource.h"
#include "ha3/sql/resource/SqlRequestInfoR.h"
#include "ha3/turing/common/ModelConfig.h"
#include "ha3/turing/qrs/QrsSqlBiz.h"
#include "ha3/util/TypeDefine.h"
#include "iquan/common/Common.h"
#include "iquan/common/Status.h"
#include "iquan/config/ExecConfig.h"
#include "iquan/config/JniConfig.h"
#include "iquan/jni/Iquan.h"
#include "iquan/jni/IquanDqlRequest.h"
#include "iquan/jni/IquanDqlResponse.h"
#include "kmonitor/client/core/MetricsTags.h"
#ifndef AIOS_OPEN_SOURCE
#include "lockless_allocator/MallocPoolScope.h"
#endif
#include "navi/builder/GraphBuilder.h"
#include "navi/common.h"
#include "navi/engine/Data.h"
#include "navi/engine/Navi.h"
#include "navi/engine/ResourceMap.h"
#include "navi/log/LoggingEvent.h"
#include "navi/proto/GraphDef.pb.h"
#include "navi/proto/GraphVis.pb.h"
#include "navi/util/NaviClosure.h"
#include "suez/turing/common/BizInfo.h"
#include "suez/turing/expression/cava/common/CavaConfig.h"
#include "suez/turing/expression/cava/common/SuezCavaAllocator.h"
#include "suez/turing/search/SearchContext.h"
#include "table/Row.h"
#include "table/Table.h"
#include "table/TableUtil.h"

using namespace std;
using namespace autil;
using namespace table;
using namespace navi;
using namespace multi_call;
using namespace autil::legacy;
using namespace kmonitor;

using namespace isearch::common;
using namespace isearch::monitor;
using namespace isearch::proto;
using namespace isearch::turing;
using namespace isearch::sql;

namespace isearch {
namespace service {
AUTIL_LOG_SETUP(ha3, QrsSqlHandler);

RunGraphClosure::RunGraphClosure(QrsSqlHandler *handler)
    : _handler(handler) {}

void RunGraphClosure::run(navi::NaviUserResultPtr userResult) {
    _handler->runGraphCallback(userResult);
}

QrsSqlHandler::QrsSqlHandler(const turing::QrsSqlBizPtr &qrsSqlBiz,
                             const multi_call::QuerySessionPtr &querySession,
                             common::TimeoutTerminator *timeoutTerminator)
    : _runGraphClosure(this)
    , _sessionRequest(NULL)
    , _sessionResult(NULL)
    , _qrsSqlBiz(qrsSqlBiz)
    , _querySession(querySession)
    , _timeoutTerminator(timeoutTerminator)
    , _finalOutputNum(0)
    , _useGigSrc(false) {}

bool QrsSqlHandler::checkTimeOutWithError(common::ErrorResult &errorResult, ErrorCode errorCode) {
    int64_t leftTime = _timeoutTerminator->getLeftTime();
    if (leftTime <= 0) {
        errorResult.resetError(errorCode);
        return false;
    }
    return true;
}

void QrsSqlHandler::process(const QrsSessionSqlRequest *sessionRequest,
                            QrsSessionSqlResult *sessionResult,
                            CallbackType callback) {
    _callback = std::move(callback);
    _sessionRequest = sessionRequest;
    _sessionResult = sessionResult;
    _sessionResult->searchInfoLevel = parseSearchInfoLevel(*_sessionRequest);
    _sessionResult->resultCompressType = parseResultCompressType(*_sessionRequest);
    runGraph();
}

void QrsSqlHandler::runGraph() {
    _beginTime = TimeUtility::currentTime();
    auto navi = _qrsSqlBiz->getNavi();
    if (navi == nullptr) {
        auto &errorResult = _sessionResult->errorResult;
        const string runGraphDesc = "get navi failed";
        AUTIL_LOG(ERROR, "get navi failed: %s", runGraphDesc.c_str());
        errorResult.resetError(ERROR_SQL_RUN_GRAPH, "run graph failed. " + runGraphDesc);
        afterRun();
        return;
    }
    GraphId graphId;
    unique_ptr<GraphDef> graphDef = buildRunSqlGraph(DEFAULT_QRS_SQL_BIZ_NAME, graphId);
    if (graphDef == nullptr) {
        auto &errorResult = _sessionResult->errorResult;
        const string runGraphDesc = "build run sql graph failed.";
        AUTIL_LOG(ERROR, "graph def invalid: %s", runGraphDesc.c_str());
        errorResult.resetError(ERROR_SQL_RUN_GRAPH, "run graph failed. " + runGraphDesc);
        afterRun();
        return;
    }
    SqlRequestDataPtr sqlRequestData(new SqlRequestData(_sessionRequest->sqlRequest));
    _runGraphParams = makeRunGraphParams(*_sessionRequest, graphId, sqlRequestData);

    SqlRequestInfoRPtr sqlRequestInfoR(new SqlRequestInfoR);
    sqlRequestInfoR->inQrs = true;
    sqlRequestInfoR->gigQuerySession = _querySession;
    sqlRequestInfoR->gdbPtr = this;

    ResourceMap resourceMap;
    resourceMap.addResource(sqlRequestInfoR);
    navi->runLocalGraphAsync(graphDef.release(), _runGraphParams, resourceMap, &_runGraphClosure);
}

bool QrsSqlHandler::processResult(NaviUserResultPtr naviUserResult, string &runGraphDesc) {
    if (!naviUserResult) {
        AUTIL_LOG(WARN,
                  "run local graph failed, return user result value is nullptr, check navi log.");
        return false;
    }
    auto naviResult = naviUserResult->getNaviResult();
    vector<navi::DataPtr> tableDataVec;
    vector<navi::DataPtr> sqlPlanDataVec;
    auto getResRet = getResult(naviUserResult, tableDataVec, sqlPlanDataVec);

    _rpcInfoMap = naviResult->stealRpcInfoMap();

    auto *metaInfoResource
        = dynamic_cast<MetaInfoResource *>(naviUserResult->getMetaInfoResource());
    if (metaInfoResource) {
        auto *searchInfoCollector = metaInfoResource->getMergeInfoCollector();
        assert(searchInfoCollector != nullptr && "info collector must not be null");

        auto graphMetricTime = naviResult->getGraphMetricTime();
        searchInfoCollector->setSqlExecTime(graphMetricTime.queueUs, graphMetricTime.computeUs);

        // collect navi perf info
        if (_runGraphParams.collectMetric() || _runGraphParams.collectPerf()) {
            auto *naviPerfInfo = searchInfoCollector->addNaviPerfInfo();
            naviPerfInfo->set_version(1);
            naviPerfInfo->set_rolename("qrs");
            std::shared_ptr<navi::GraphVisDef> visProto = naviResult->getGraphVisData();
            const string &visStr = visProto->SerializeAsString();
            naviPerfInfo->set_graphvis(visStr);
            multi_call::FileRecorder::newRecord(
                visStr, 5,
                "timeline",
                "qrs_graph_vis_" + StringUtil::toString(getpid()));
        }
        searchInfoCollector->setSqlRunForkGraphEndTime(TimeUtility::currentTime());
        _searchInfo = searchInfoCollector->getSqlSearchInfo();
        RunSqlTimeInfo *timeInfo = _searchInfo.mutable_runsqltimeinfo();
        timeInfo->set_sqlrunforkgraphtime(timeInfo->sqlrunforkgraphendtime() -
                timeInfo->sqlrunforkgraphbegintime());
        timeInfo->set_sqlrungraphtime(timeInfo->sqlrunforkgraphendtime() - _beginTime);

        _sessionRequest->metricsCollectorPtr->setSqlPlanTime(timeInfo->sqlplantime());
        _sessionRequest->metricsCollectorPtr->setSqlPlan2GraphTime(
            timeInfo->sqlplan2graphtime());
        _sessionRequest->metricsCollectorPtr->setSqlRunGraphTime(
            timeInfo->sqlrungraphtime());
    }
    if (!getResRet) {
        runGraphDesc = "wait sql result failed, ec: "
                       + std::string(navi::NaviResult::getErrorString(naviResult->ec))
                       + ", msg: " + naviResult->errorEvent.message;
        AUTIL_LOG(WARN, "run sql has error: %s", runGraphDesc.c_str());
    }
    bool outputSqlPlan = getOutputSqlPlan(*_sessionRequest);
    string traceLevel = getTraceLevel(*_sessionRequest);
    if (!traceLevel.empty() &&navi::getLevelByString(traceLevel) >= navi::LOG_LEVEL_DEBUG) {
        outputSqlPlan = true;
    }
    if (!fillSessionSqlResult(
            naviResult, tableDataVec, sqlPlanDataVec, outputSqlPlan, *_sessionResult, runGraphDesc)) {
        return false;
    }

    _finalOutputNum = _sessionResult->getTableRowCount();
    runGraphDesc = _sessionResult->naviResult->errorEvent.message;
    return true;
}

bool QrsSqlHandler::getResult(const navi::NaviUserResultPtr &result,
                               vector<navi::DataPtr> &tableDataVec,
                               vector<navi::DataPtr> &sqlPlanDataVec)
{
    while (true) {
        navi::NaviUserData data;
        bool eof = false;
        if (result->nextData(data, eof)) {
            if (data.data) {
                if (data.name == "sql_result") {
                    tableDataVec.push_back(data.data);
                } else if (data.name == "sql_plan") {
                    sqlPlanDataVec.push_back(data.data);
                }
            }
        }
        if (eof) {
            break;
        }
    }
    return tableDataVec.size() != 0;
}

void QrsSqlHandler::tryFillSessionSqlPlan(
        const vector<navi::DataPtr> &sqlPlanDataVec,
        bool outputSqlPlan,
        QrsSessionSqlResult &sessionResult,
        std::vector<std::string> &runGraphDescVec)
{
    if (sqlPlanDataVec.size() != 1) {
        runGraphDescVec.emplace_back("unexpected sql plan size ["
                                     + autil::StringUtil::toString(sqlPlanDataVec.size())
                                     + "] != 1");
        return;
    }
    SqlPlanDataPtr sqlPlanData = dynamic_pointer_cast<SqlPlanData>(sqlPlanDataVec[0]);
    if (sqlPlanData == nullptr) {
        runGraphDescVec.emplace_back("get sql plan data failed.");
        return;
    }
    sessionResult.sqlQueryRequest = *(sqlPlanData->getIquanSqlRequest());
    if (outputSqlPlan && sqlPlanData->getSqlPlan()) {
        iquan::IquanDqlResponse sqlQueryResponse(*sqlPlanData->getSqlPlan());
        iquan::IquanDqlResponseWrapper responseWrapper;
        try {
            if (!responseWrapper.convert(sqlQueryResponse,
                            sessionResult.sqlQueryRequest.dynamicParams))
            {
                runGraphDescVec.emplace_back("convert sql plan failed.");
                return;
            }
            sessionResult.iquanResponseWrapper = responseWrapper;
        } catch (const std::exception& e) {
            AUTIL_LOG(ERROR, "EXCEPTION: %s", e.what());
        }
    }
}

bool QrsSqlHandler::fillSessionSqlResult(
        const navi::NaviResultPtr &result,
        const vector<navi::DataPtr> &tableDataVec,
        const vector<navi::DataPtr> &sqlPlanDataVec,
        bool outputSqlPlan,
        QrsSessionSqlResult &sessionResult,
        string &runGraphDesc)
{
    sessionResult.naviResult = result;
    std::vector<std::string> runGraphDescVec;
    if (result->ec != navi::EC_NONE) {
        runGraphDescVec.emplace_back("result has error ["
                                     + std::string(navi::NaviResult::getErrorString(result->ec))
                                     + "], error msg [" + result->errorEvent.message + "]");
    }
    tryFillSessionSqlPlan(sqlPlanDataVec, outputSqlPlan, sessionResult, runGraphDescVec);
    if (!runGraphDescVec.empty()) {
        runGraphDesc = std::move(runGraphDescVec[0]);
        return false;
    }
    if (tableDataVec.size() == 0) {
        runGraphDesc = "result table empty";
        return false;
    }
    TableDataPtr tableData = dynamic_pointer_cast<TableData>(tableDataVec[0]);
    if (tableData == nullptr || tableData->getTable() == nullptr) {
        runGraphDesc = "searcher reuslt output without table";
        return false;
    }
    TablePtr table = tableData->getTable();
    for (size_t i = 1; i < tableDataVec.size(); i++) {
        tableData = dynamic_pointer_cast<TableData>(tableDataVec[i]);
        if (tableData == nullptr || tableData->getTable() == nullptr) {
            runGraphDesc = "searcher reuslt output without table";
            return false;
        }
        TablePtr table1 = tableData->getTable();
        if (!table->merge(table1)) {
            runGraphDesc = "merge table failed, table1:" + table::TableUtil::toString(table, 2)
                           + " \n table2:" + TableUtil::toString(table1, 2);
            return false;
        }
    }
    sessionResult.table = table;
    return true;
}

string QrsSqlHandler::getTaskQueueName(const QrsSessionSqlRequest &sessionRequest) {
    std::string taskQueueName;
    if (sessionRequest.sqlRequest == nullptr) {
        return "";
    }
    if (sessionRequest.sqlRequest->getValue(SQL_IQUAN_EXEC_PARAMS_TASK_QUEUE, taskQueueName)) {
        return taskQueueName;
    }
    return "";
}

string QrsSqlHandler::getTraceLevel(const QrsSessionSqlRequest &sessionRequest) {
    std::string traceLevel;
    if (sessionRequest.sqlRequest == nullptr) {
        return "";
    }
    sessionRequest.sqlRequest->getValue(SQL_TRACE_LEVEL, traceLevel);
    autil::StringUtil::toUpperCase(traceLevel);
    return traceLevel;
}

QrsSessionSqlResult::SearchInfoLevel QrsSqlHandler::parseSearchInfoLevel(const QrsSessionSqlRequest &sessionRequest) {
    if (sessionRequest.sqlRequest) {
        std::string info;
        sessionRequest.sqlRequest->getValue(SQL_GET_SEARCH_INFO, info);
        if (!info.empty()) {
            if (info == "simple") {
                return QrsSessionSqlResult::SearchInfoLevel::SQL_SIL_SIMPLE;
            }
            bool ret;
            StringUtil::fromString(info, ret);
            if (ret || info == "full") {
                return QrsSessionSqlResult::SearchInfoLevel::SQL_SIL_FULL;
            }
        }
    }
    return QrsSessionSqlResult::SearchInfoLevel::SQL_SIL_NONE;
}

autil::CompressType
QrsSqlHandler::parseResultCompressType(const QrsSessionSqlRequest &sessionRequest) {
    if (sessionRequest.sqlRequest) {
        std::string compressType;
        sessionRequest.sqlRequest->getValue(SQL_GET_RESULT_COMPRESS_TYPE, compressType);
        if (!compressType.empty()) {
            auto convertedType = autil::convertCompressType(compressType);
            if (convertedType == autil::CompressType::INVALID_COMPRESS_TYPE) {
                AUTIL_LOG(WARN,
                          "Parse result compress type failed: invalid type [%s]",
                          compressType.c_str());
                return autil::CompressType::NO_COMPRESS;
            }
            return convertedType;
        }
    }
    return autil::CompressType::NO_COMPRESS;
}

RunGraphParams QrsSqlHandler::makeRunGraphParams(const sql::QrsSessionSqlRequest &request,
                                                 GraphId graphId,
                                                 DataPtr outData)
{

    map<string, string> userParams;
    (void)getUserParams(request, userParams);
    const string &traceLevel = getTraceLevel(request);
    const string &taskQueueName = getTaskQueueName(request);

    int64_t leftTimeInMs = _timeoutTerminator->getLeftTime() / 1000;
    navi::RunGraphParams runGraphParams;
    runGraphParams.setSinglePoolUsageLimit(_qrsSqlBiz->getSinglePoolUsageLimit());

    // navi perf
    auto iter = userParams.find(COLLECT_NAVI_TIMELINE);
    if (iter != userParams.end()) {
        runGraphParams.setCollectMetric(iter->second == "true");
    }
    iter = userParams.find(COLLECT_NAVI_PERF);
    if (iter != userParams.end()) {
        if (iter->second == "true") {
            runGraphParams.setCollectPerf(true);
            runGraphParams.setCollectMetric(true);
        }
    }

    runGraphParams.setThreadLimit(_qrsSqlBiz->getSqlConfig()->sqlConfig.mainGraphThreadLimit);
    runGraphParams.setTimeoutMs(leftTimeInMs);
    if (!traceLevel.empty()) {
        runGraphParams.setTraceLevel(traceLevel);
    }
    if (!taskQueueName.empty()) {
        runGraphParams.setTaskQueueName(taskQueueName);
    }
    OverrideData data;
    data.graphId = graphId;
    data.outputNode = "placehold";
    data.outputPort = PLACEHOLDER_OUTPUT_PORT;
    data.datas.push_back(outData);
    runGraphParams.overrideEdgeData(data);
    runGraphParams.setQuerySession(_querySession);
    return runGraphParams;
}

unique_ptr<GraphDef> QrsSqlHandler::buildRunSqlGraph(const string &bizName, GraphId &graphId) {
    try {
        auto graphDef = std::make_unique<GraphDef>();
        GraphBuilder builder(graphDef.get());
        graphId = builder.newSubGraph(bizName);
        builder.subGraph(graphId);
        auto placehold = builder.node("placehold").kernel(PLACEHOLDER_KERNEL_NAME);
        auto sqlParse = builder.node("sql_parse").kernel("SqlParseKernel");
        auto modelOptimize = builder.node("model_optimize").kernel("SqlModelOptimizeKernel");
        auto planTransform = builder.node("plan_transform").kernel("PlanTransformKernel");
        auto runSqlGraph = builder.node("run_sql_graph").kernel("RunSqlGraphKernel");

        sqlParse.in("input0").from(placehold.out(PLACEHOLDER_OUTPUT_PORT));
        modelOptimize.in("input0").from(sqlParse.out("output0"));
        planTransform.in("input0").from(modelOptimize.out("output0"));
        planTransform.in("input1").from(placehold.out(PLACEHOLDER_OUTPUT_PORT));
        runSqlGraph.in("input0").from(planTransform.out("output0"));
        runSqlGraph.out("output0").asGraphOutput("sql_result");
        sqlParse.out("output0").asGraphOutput("sql_plan");

        if (builder.ok()) {
            AUTIL_LOG(DEBUG, "graph def: %s", graphDef->ShortDebugString().c_str());
            return graphDef;
        } else {
            return {};
        }
    } catch (const autil::legacy::ExceptionBase &e) {
        AUTIL_LOG(ERROR, "build sql graph failed, msg [%s]", e.GetMessage().c_str());
        return {};
    }
}

bool QrsSqlHandler::getForbitMergeSearchInfo(const QrsSessionSqlRequest &sessionRequest) {
    std::string info;
    if (sessionRequest.sqlRequest == nullptr) {
        return false;
    }
    sessionRequest.sqlRequest->getValue(SQL_FORBIT_MERGE_SEARCH_INFO, info);
    if (info.empty()) {
        return false;
    }
    bool res = false;
    StringUtil::fromString(info, res);
    return res;
}

bool QrsSqlHandler::getForbitSearchInfo(const QrsSessionSqlRequest &sessionRequest) {
    std::string info;
    if (sessionRequest.sqlRequest == nullptr) {
        return false;
    }
    sessionRequest.sqlRequest->getValue(SQL_FORBIT_SEARCH_INFO, info);
    if (info.empty()) {
        return false;
    }
    bool res = false;
    StringUtil::fromString(info, res);
    return res;
}
bool QrsSqlHandler::getUserParams(const QrsSessionSqlRequest &sessionRequest, map<string, string> &userParam) {
    std::string info;
    if (sessionRequest.sqlRequest == nullptr) {
        return false;
    }
    sessionRequest.sqlRequest->getValue(SQL_IQUAN_EXEC_PARAMS_USER_KV, info);
    if (info.empty()) {
        return false;
    }
    const vector<string> &kvVec = StringUtil::split(info, ",", true);
    for (const auto &kv : kvVec) {
        const vector<string> &kvPair = StringUtil::split(kv, "#", true);
        if (kvPair.size() == 2) {
            userParam[kvPair[0]] = kvPair[1];
        }
    }
    return true;
}

bool QrsSqlHandler::getOutputSqlPlan(const QrsSessionSqlRequest &sessionRequest) {
    if (sessionRequest.sqlRequest == nullptr) {
        return false;
    }
    std::string info;
    sessionRequest.sqlRequest->getValue(SQL_GET_SQL_PLAN, info);
    if (info.empty()) {
        return false;
    }
    bool res = false;
    StringUtil::fromString(info, res);
    return res;
}

void QrsSqlHandler::runGraphCallback(NaviUserResultPtr naviUserResult) {
#ifndef AIOS_OPEN_SOURCE
    MallocPoolScope mallocScope;
#endif
    auto &errorResult = _sessionResult->errorResult;
    std::string runGraphDesc;
    if (processResult(naviUserResult, runGraphDesc)) {
        // todo navi multi_calll error
        _sessionResult->multicallEc = multi_call::MULTI_CALL_ERROR_NONE;
        checkTimeOutWithError(errorResult, ERROR_SQL_RUN_GRAPH_TIMEOUT);
        errorResult.setErrorMsg(runGraphDesc);
    } else {
        AUTIL_LOG(ERROR, "process result failed: %s", runGraphDesc.c_str());
        errorResult.resetError(ERROR_SQL_RUN_GRAPH, "run graph failed. " + runGraphDesc);
    }
    afterRun();
}

void QrsSqlHandler::afterRun() {
    auto &errorResult = _sessionResult->errorResult;
    _sessionRequest->metricsCollectorPtr->sqlRunGraphEndTrigger();
    if (errorResult.hasError()) {
        AUTIL_LOG(ERROR,
                  "sql process query [%s], error [%s]",
                  _sessionRequest->sqlRequest->toString().c_str(),
                  errorResult.getErrorMsg().c_str());
    }
    auto callback = std::move(_callback);
    QrsSqlHandlerResult result = {
        _finalOutputNum,
        std::move(_searchInfo),
        std::move(_rpcInfoMap),
    };

    delete this;
    if (callback) {
        callback(std::move(result));
    }
}

} // namespace service
} // namespace isearch
