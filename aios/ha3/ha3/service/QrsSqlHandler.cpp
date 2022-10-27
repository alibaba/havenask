#include <ha3/service/QrsSqlHandler.h>
#include <ha3/common/XMLResultFormatter.h>
#include <ha3/sql/ops/exchange/ExchangeKernel.h>
#include <ha3/sql/data/Table.h>
#include <ha3/sql/data/TableFormatter.h>
#include <navi/engine/Navi.h>
#include <autil/StringUtil.h>
#include <autil/URLUtil.h>
#include <autil/legacy/any_jsonizable.h>
#include <iquan_common/common/Status.h>
#include <ha3/sql/common/common.h>
#include <suez/turing/search/SearchContext.h>

using namespace std;
using namespace multi_call;
using namespace autil;
using namespace autil::legacy;
using namespace kmonitor;

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(monitor);
USE_HA3_NAMESPACE(proto);
USE_HA3_NAMESPACE(turing);
USE_HA3_NAMESPACE(sql);

BEGIN_HA3_NAMESPACE(service);
HA3_LOG_SETUP(service, QrsSqlHandler);

QrsSqlHandler::QrsSqlHandler(const turing::QrsSqlBizPtr &qrsSqlBiz,
                             const multi_call::QuerySessionPtr &querySession,
                             common::TimeoutTerminator *timeoutTerminator)
    : _qrsSqlBiz(qrsSqlBiz)
    , _querySession(querySession)
    , _timeoutTerminator(timeoutTerminator)
    , _finalOutputNum(0)
    , _useGigSrc(false)
{
}

QrsSqlHandler::~QrsSqlHandler() {
}

bool QrsSqlHandler::checkTimeOutWithError(common::ErrorResult &errorResult,
        ErrorCode errorCode)
{
    int64_t leftTime = _timeoutTerminator->getLeftTime();
    if (leftTime <= 0) {
        errorResult.resetError(errorCode);
        return false;
    }
    return true;
}

void QrsSqlHandler::initExecConfig(const QrsSessionSqlRequest &sessionRequest,
                                   const config::SqlConfigPtr &sqlConfig,
                                   int64_t leftTime,
                                   const std::string &traceLevel,
                                   iquan::ExecConfig &execConfig)
{
    execConfig.parallelConfig.parallelNum =
        getParallelNum(sessionRequest.sqlRequest, sqlConfig);
    execConfig.parallelConfig.parallelTables =
        getParallelTalbes(sessionRequest.sqlRequest, sqlConfig);

    size_t subGraphTimeout = leftTime * sqlConfig->subGraphTimeoutFactor;
    execConfig.naviConfig.runtimeConfig.threadLimit = sqlConfig->subGraphThreadLimit;
    execConfig.naviConfig.runtimeConfig.timeout = subGraphTimeout;
    execConfig.naviConfig.runtimeConfig.traceLevel = traceLevel;
    execConfig.lackResultEnable =
        getLackResultEnable(sessionRequest.sqlRequest, sqlConfig);
}

navi::GraphDef* QrsSqlHandler::getGraphDef(iquan::SqlPlan &sqlPlan,
        iquan::SqlQueryRequest &request,
        const QrsSessionSqlRequest &sessionRequest,
        int64_t leftTime, const string &traceLevel,
        common::ErrorResult &errorResult,
        vector<string> &outputPort,
        vector<string> &outputNode)
{
    if (!checkTimeOutWithError(errorResult, ERROR_SQL_PLAN_SERVICE_TIMEOUT)) {
        return nullptr;
    }

    iquan::ExecConfig execConfig;
    config::SqlConfigPtr sqlConfig = _qrsSqlBiz->getSqlConfig();

    initExecConfig(sessionRequest, sqlConfig, leftTime, traceLevel, execConfig);
    if (!execConfig.isValid()) {
        std::string errorMsg = "invalid execConfig";
        HA3_LOG(ERROR, "%s", errorMsg.c_str());
        errorResult.resetError(ERROR_SQL_TRANSFER_GRAPH, errorMsg);
        return nullptr;
    }

    auto sqlClient = _qrsSqlBiz->getSqlClient();
    if (sqlClient == nullptr) {
        string errorMsg = "sqlClient is null";
        HA3_LOG(ERROR, "%s", errorMsg.c_str());
        errorResult.resetError(ERROR_SQL_TRANSFER_GRAPH, errorMsg.c_str());
        return nullptr;
    }

    auto graphDef = new navi::GraphDef();
    std::map<std::string, double> metricCollectors;
    iquan::Status status = sqlClient->transform(sqlPlan, request, execConfig, *graphDef, outputPort, outputNode, metricCollectors);
    if (!status.ok()) {
        string errorMsg = "failed to call iquan transform, error code is " + std::to_string(status.code())
                          + ", error message is " + status.errorMessage();
        HA3_LOG(ERROR, "%s", errorMsg.c_str());
        errorResult.resetError(ERROR_SQL_TRANSFER_GRAPH, errorMsg.c_str());
        delete graphDef;
        graphDef = nullptr;
        return nullptr;
    }

    sessionRequest.metricsCollectorPtr->sqlPlan2GraphEndTrigger();
    return graphDef;
}

void QrsSqlHandler::traceErrorMsg(const string &traceLevel,
                                  iquan::SqlQueryRequest &sqlRequest,
                                  const iquan::SqlQueryResponse &sqlQueryResponse,
                                  const QrsSessionSqlRequest &sessionRequest,
                                  QrsSessionSqlResult &sessionResult)
{
    if (navi::getLevelByString(traceLevel) < navi::LOG_LEVEL_DEBUG) {
        return;
    }

    iquan::SqlQueryResponseWrapper responseWrapper;
    if (!responseWrapper.convert(sqlQueryResponse, sqlRequest.dynamicParams)) {
        return;
    }

    const string &sqlQuery = sessionRequest.sqlRequest->getSqlQuery();
    sessionResult.sqlQuery  = sqlQuery;
    sessionResult.iquanResponseWrapper = responseWrapper;
    if (sessionResult.naviResult && sessionResult.naviResult->_ec != navi::EC_NONE) {
        sessionResult.sqlTrace.push_back(sessionResult.naviResult->_msg);
    }
}

bool QrsSqlHandler::getSqlDynamicParams(
        const QrsSessionSqlRequest &sessionRequest,
        std::vector<std::vector<autil::legacy::Any>> &dynamicParams)
{
    string dynamicParamsStr;
    bool retValue = sessionRequest.sqlRequest->getValue(SQL_DYNAMIC_PARAMS, dynamicParamsStr);
    if (!retValue) {
        return true;
    }
    string urlencodeDataStr;
    if (sessionRequest.sqlRequest->getValue(SQL_URLENCODE_DATA, urlencodeDataStr) &&
        urlencodeDataStr == "true")
    {
        dynamicParamsStr = URLUtil::decode(dynamicParamsStr);
    }

    try {
        FromJsonString(dynamicParams, dynamicParamsStr);
    } catch (const std::exception &e) {
        HA3_LOG(ERROR, "parse json string [%s] failed, exception [%s]",
                dynamicParamsStr.c_str(), e.what());
        return false;
    }
    return true;
}

bool QrsSqlHandler::parseIquanSqlParams(const QrsSessionSqlRequest &sessionRequest,
                                        iquan::SqlQueryRequest &sqlQueryRequest)
{
    const std::unordered_map<std::string, std::string> &kvPair = sessionRequest.sqlRequest->getKvPair();
    auto iter = kvPair.begin();
    auto end = kvPair.end();

    for (; iter != end; iter++) {
        if (StringUtil::startsWith(iter->first, SQL_IQUAN_SQL_PARAMS_PREFIX)) {
            sqlQueryRequest.sqlParams[iter->first] = iter->second;
        } else if (StringUtil::startsWith(iter->first, SQL_IQUAN_EXEC_PARAMS_PREFIX)) {
            sqlQueryRequest.execParams[iter->first] = iter->second;
        }
    }
    auto sqlConfig = _qrsSqlBiz->getSqlConfig();
    if (sqlQueryRequest.sqlParams.find(SQL_IQUAN_PLAN_CACHE_ENABLE) ==
        sqlQueryRequest.sqlParams.end())
    {
        sqlQueryRequest.sqlParams[SQL_IQUAN_PLAN_CACHE_ENABLE] =
            sqlConfig->iquanPlanCacheEnable ? string("true") : string("false");
    }
    if (sqlQueryRequest.sqlParams.find(SQL_IQUAN_PLAN_PREPARE_LEVEL) ==
        sqlQueryRequest.sqlParams.end())
    {
        sqlQueryRequest.sqlParams[SQL_IQUAN_PLAN_PREPARE_LEVEL] = sqlConfig->iquanPlanPrepareLevel;
    }

    if (sqlQueryRequest.execParams.find(SQL_IQUAN_EXEC_PARAMS_SOURCE_ID) ==
        sqlQueryRequest.execParams.end())
    {
        sqlQueryRequest.execParams[SQL_IQUAN_EXEC_PARAMS_SOURCE_ID] =
            StringUtil::toString(TimeUtility::currentTimeInNanoSeconds());
    }
    if (sqlQueryRequest.execParams.find(SQL_IQUAN_EXEC_PARAMS_SOURCE_SPEC) ==
        sqlQueryRequest.execParams.end())
    {
        sqlQueryRequest.execParams[SQL_IQUAN_EXEC_PARAMS_SOURCE_SPEC] = SQL_SOURCE_SPEC_EMPTY;
    }

    return true;
}

bool QrsSqlHandler::parseSqlParams(const QrsSessionSqlRequest &sessionRequest,
                                   iquan::SqlQueryRequest &sqlQueryRequest)
{
    // 1. parse dynamic params
    if (!getSqlDynamicParams(sessionRequest, sqlQueryRequest.dynamicParams)) {
        return false;
    }

    // 2. parse iquan params.
    parseIquanSqlParams(sessionRequest, sqlQueryRequest);

    // 3. add iquan.plan.format.version
    auto iter = sqlQueryRequest.sqlParams.find(std::string(SQL_IQUAN_PLAN_FORMAT_VERSION));
    if (iter == sqlQueryRequest.sqlParams.end()) {
        sqlQueryRequest.sqlParams[SQL_IQUAN_PLAN_FORMAT_VERSION] = string(SQL_DEFAULT_VALUE_IQUAN_PLAN_FORMAT_VERSION);
    }

    // 4. send the suffix of summary table to iquan
    config::SqlConfigPtr sqlConfigPtr = _qrsSqlBiz->getSqlConfig();
    sqlQueryRequest.sqlParams[SQL_IQUAN_OPTIMIZER_TABLE_SUMMARY_SUFFIX] = sqlConfigPtr->jniConfig.tableConfig.summaryTableSuffix;
    return true;
}

void QrsSqlHandler::getSqlPlan(const QrsSessionSqlRequest &sessionRequest,
                               iquan::SqlQueryResponse &sqlQueryResponse,
                               iquan::SqlQueryRequest &request,
                               HA3_NS(common)::ErrorResult &errorResult)
{
    sessionRequest.metricsCollectorPtr->sqlPlanStartTrigger();

    const string &sqlQuery = sessionRequest.sqlRequest->getSqlQuery();
    if (!checkTimeOutWithError(errorResult, ERROR_SQL_PLAN_SERVICE_TIMEOUT)) {
        string errMsg = "timeout before getting sql plan, sql query: " + sqlQuery;
        HA3_LOG(ERROR, "%s", errMsg.c_str());
        return;
    }

    auto sqlClient = _qrsSqlBiz->getSqlClient();
    if (sqlClient == NULL) {
        string errorMsg = "sqlClient is null";
        HA3_LOG(ERROR, "%s", errorMsg.c_str());
        errorResult.resetError(ERROR_SQL_INIT_REQUEST, errorMsg.c_str());
        return;
    }

    request.sqls.push_back(sqlQuery);
    if (!parseSqlParams(sessionRequest, request)) {
        string errorMsg = "get invalid sql params";
        errorResult.resetError(ERROR_SQL_PLAN_SERVICE, errorMsg);
        HA3_LOG(ERROR, "%s", errorMsg.c_str());
        return;
    }
    request.defaultCatalogName = getDefaultCatalogName(sessionRequest.sqlRequest);
    request.defaultDatabaseName = getDefaultDatabaseName(sessionRequest.sqlRequest);

    iquan::Status status = sqlClient->query(request, sqlQueryResponse);
    if (!status.ok()) {
        string errorMsg =
            "failed to get sql plan, error message is " + status.errorMessage();
        errorResult.resetError(ERROR_SQL_PLAN_SERVICE, errorMsg);
        HA3_LOG(ERROR, "%s", errorMsg.c_str());
        return;
    } else if (sqlQueryResponse.errorCode != 0) {
        string errorMsg =
            "generate sql plan failed, iquan error code is " + StringUtil::toString(sqlQueryResponse.errorCode) + "\n"
            + sqlQueryResponse.errorMsg;
        errorResult.resetError(ERROR_SQL_PLAN_GENERATE, errorMsg);
        HA3_LOG(ERROR, "%s", errorMsg.c_str());
        return;
    } else {
        HA3_LOG(DEBUG, "sql result [%s]", ToJsonString(sqlQueryResponse).c_str());
    }

    if (!checkTimeOutWithError(errorResult, ERROR_SQL_PLAN_SERVICE_TIMEOUT)) {
        string errMsg = "timeout after getting sql plan, sql query: " + sqlQuery;
        HA3_LOG(ERROR, "%s", errMsg.c_str());
        return;
    }
    // todo
    // sessionRequest.metricsCollectorPtr->setSqlPlanSize(sqlResult.size());
    sessionRequest.metricsCollectorPtr->sqlPlanEndTrigger();
}

void QrsSqlHandler::process(const QrsSessionSqlRequest &sessionRequest,
                            QrsSessionSqlResult &sessionResult)
{
    const string &traceLevel = getTraceLevel(sessionRequest);
    sessionResult.getSearchInfo = getSearchInfo(sessionRequest);
    common::ErrorResult &errorResult = sessionResult.errorResult;
    ErrorCode lastErrorCode = errorResult.getErrorCode();

    bool outputPlan = getOutputSqlPlan(sessionRequest);
    iquan::SqlQueryResponse sqlQueryResponse;
    iquan::SqlQueryRequest sqlQueryRequest;
    getSqlPlan(sessionRequest, sqlQueryResponse, sqlQueryRequest, errorResult);
    if (errorResult.getErrorCode() == lastErrorCode) {
        vector<string> outputPort, outputNode;
        int64_t leftTimeInMs = _timeoutTerminator->getLeftTime() / 1000;
        navi::GraphDef *graphDef = getGraphDef(sqlQueryResponse.result,
                sqlQueryRequest, sessionRequest,
                leftTimeInMs, traceLevel,
                errorResult, outputPort, outputNode);
        if (graphDef != nullptr) {
            std::string runGraphDesc;
            if (outputPlan) {
                sessionResult.naviGraph = graphDef->DebugString();
            }
            if (runGraph(graphDef, sessionRequest, sessionResult, outputPort,
                         outputNode, leftTimeInMs, traceLevel, runGraphDesc))
            {
                // todo navi multi_calll error
                sessionResult.multicallEc = multi_call::MULTI_CALL_ERROR_NONE;
                checkTimeOutWithError(sessionResult.errorResult,
                        ERROR_SQL_RUN_GRAPH_TIMEOUT);
                errorResult.setErrorMsg(runGraphDesc);
            } else {
                HA3_LOG(ERROR, "%s", runGraphDesc.c_str());
                errorResult.resetError(ERROR_SQL_RUN_GRAPH, "run graph failed." + runGraphDesc);
            }
            sessionRequest.metricsCollectorPtr->sqlRunGraphEndTrigger();
        }
    }

    if (outputPlan) {
        iquan::SqlQueryResponseWrapper responseWrapper;
        if (!responseWrapper.convert(sqlQueryResponse, sqlQueryRequest.dynamicParams)) {
            return;
        }
        sessionResult.sqlQuery = sessionRequest.sqlRequest->getSqlQuery();
        sessionResult.iquanResponseWrapper = responseWrapper;
    }
    traceErrorMsg(traceLevel, sqlQueryRequest, sqlQueryResponse, sessionRequest, sessionResult);

    RunSqlTimeInfo *timeInfo = _searchInfo.mutable_runsqltimeinfo();
    timeInfo->set_sqlplanstarttime(sessionRequest.metricsCollectorPtr->getSqlPlanStart());
    timeInfo->set_sqlplantime(sessionRequest.metricsCollectorPtr->getSqlPlanTime());
    timeInfo->set_sqlplan2graphtime(sessionRequest.metricsCollectorPtr->getSqlPlan2GraphTime());
    timeInfo->set_sqlrungraphtime(sessionRequest.metricsCollectorPtr->getSqlRunGraphTime());
    if (errorResult.hasError()) {
        HA3_LOG(ERROR, "sql process query [%s], error [%s]",
                sessionRequest.sqlRequest->getSqlQuery().c_str(),
                errorResult.getErrorMsg().c_str());
    }
}

std::string QrsSqlHandler::getDefaultCatalogName(const SqlQueryRequest *sqlRequest) {
    std::string catalogName;
    if (sqlRequest->getValue(SQL_CATALOG_NAME, catalogName)) {
        return catalogName;
    }
    if (!_qrsSqlBiz->getSqlConfig()->catalogName.empty()) {
        return _qrsSqlBiz->getSqlConfig()->catalogName;
    }
    return _qrsSqlBiz->getDefaultCatalogName();
}

std::string QrsSqlHandler::getDefaultDatabaseName(const SqlQueryRequest *sqlRequest) {
    std::string databaseName;
    if (sqlRequest->getValue(SQL_DATABASE_NAME, databaseName)) {
        return databaseName;
    }
    if (!_qrsSqlBiz->getSqlConfig()->dbName.empty()) {
        return _qrsSqlBiz->getSqlConfig()->dbName;
    }
    return _qrsSqlBiz->getDefaultDatabaseName();
}

bool QrsSqlHandler::runGraph(navi::GraphDef *graphDef,
                             const QrsSessionSqlRequest &sessionRequest,
                             QrsSessionSqlResult &sessionResult,
                             vector<string> &outputPort,
                             vector<string> &outputNode,
                             int64_t leftTime,
                             const string &traceLevel,
                             string &runGraphDesc)
{
    auto naviPtr = _qrsSqlBiz->getNavi();
    if (naviPtr == nullptr) {
        runGraphDesc = "get navi failed";
        return false;
    }
    HA3_LOG(DEBUG, "run graph [%s]", graphDef->DebugString().c_str());

    navi::GraphInputPortsPtr inputs(new navi::GraphInputPorts());
    navi::GraphOutputPortsPtr outputs(new navi::GraphOutputPorts());
    for (size_t i = 0; i < outputPort.size(); ++i) {
        outputs->addPort(new navi::GraphPort(outputNode[i], outputPort[i]));
    }
    navi::RunGraphParams mainGraphParams;
    mainGraphParams.setThreadLimit(_qrsSqlBiz->getSqlConfig()->mainGraphThreadLimit);
    mainGraphParams.setTimeoutMs(leftTime);
    mainGraphParams.setTraceLevel(traceLevel);

    map<std::string, navi::Resource *> resourceMap;

    map<string, string> tagsMap = {
        {"role_type", "qrs"}
    };

    if (sessionRequest.sqlRequest) {
        tagsMap["src"] = sessionRequest.sqlRequest->getSourceSpec();
    }
    suez::turing::SearchContext::addGigMetricTags(_querySession, tagsMap, _useGigSrc);
    MetricsTags tags(tagsMap);
    auto sqlQueryResource = _qrsSqlBiz->createSqlQueryResource(tags, "kmon", sessionRequest.pool);
    sqlQueryResource->setGigQuerySession(_querySession);
    resourceMap.insert(make_pair(sqlQueryResource->getResourceName(),
                                 sqlQueryResource.get()));
    auto collector = sqlQueryResource->getSqlSearchInfoCollector();
    bool forbitMerge = getForbitMergeSearchInfo(sessionRequest);
    collector->setForbitMerge(forbitMerge);
    naviPtr->runGraph(graphDef, inputs, outputs, mainGraphParams, resourceMap);
    outputs->wait();
    if (outputs->size() != 2) {
        runGraphDesc = "sql engine output ports expect 2, actually " +
                       autil::StringUtil::toString(outputs->size());
        return false;
    }

    if (!fillSessionSqlResult(outputs, sessionResult, runGraphDesc)) {
        return false;
    }
    _searchInfo = collector->getSqlSearchInfo();
    _finalOutputNum = sessionResult.getTableRowCount();
    runGraphDesc = sessionResult.naviResult->_msg;
    return true;
}

bool QrsSqlHandler::fillSessionSqlResult(navi::GraphOutputPortsPtr &outputs,
        QrsSessionSqlResult &sessionResult, string &runGraphDesc)
{
    navi::DataPtr sqlResultData;
    bool eof = false;
    (*outputs)[1]->getData(sqlResultData, eof);
    navi::NaviResultPtr result = dynamic_pointer_cast<navi::NaviResult>(sqlResultData);
    if (!result) {
        runGraphDesc = "sql engine output without navi result";
        return false;
    }
    sessionResult.naviResult = result;
    if (result->_ec != navi::EC_NONE) {
        runGraphDesc = "searcher result has error [" +
                       autil::StringUtil::toString(result->_ec) + "], error msg [" +
                       result->_msg + "]";
        return false;
    }

    navi::DataPtr tableData;
    eof = false;
    (*outputs)[0]->getData(tableData, eof);
    TablePtr table = dynamic_pointer_cast<Table>(tableData);
    if (!table) {
        runGraphDesc = "searcher reuslt output without table";
        return false;
    }
    sessionResult.table = table;
    return true;
}

size_t QrsSqlHandler::getParallelNum(const SqlQueryRequest *sqlQueryRequest,
                                     const config::SqlConfigPtr &sqlConfig)
{
    size_t parallelNum = PARALLEL_NUM_MIN_LIMIT;
    std::string parallelNumStr;
    if (sqlQueryRequest->getValue(SQL_PARALLEL_NUMBER, parallelNumStr)) {
        parallelNum = autil::StringUtil::fromString<size_t>(parallelNumStr);
    } else {
        parallelNum = sqlConfig->parallelNum;
    }
    parallelNum = std::min(PARALLEL_NUM_MAX_LIMIT, parallelNum);
    parallelNum = std::max(PARALLEL_NUM_MIN_LIMIT, parallelNum);
    return parallelNum;
}

vector<string> QrsSqlHandler::getParallelTalbes(
        const SqlQueryRequest *sqlQueryRequest,
        const config::SqlConfigPtr &sqlConfig)
{
    vector<string> parallelTables;
    std::string parallelTablesStr;
    if (sqlQueryRequest->getValue(SQL_PARALLEL_TABLES, parallelTablesStr)) {
        autil::StringUtil::split(parallelTables, parallelTablesStr,
                SQL_PARALLEL_TABLES_SPLIT);
    } else {
        parallelTables = sqlConfig->parallelTables;
    }
    return parallelTables;
}

bool QrsSqlHandler::getLackResultEnable(const SqlQueryRequest *sqlQueryRequest,
                                   const config::SqlConfigPtr &sqlConfig)
{
    bool lackResultEnable = sqlConfig->lackResultEnable;
    string lackResultEnableStr;
    if (sqlQueryRequest->getValue(SQL_LACK_RESULT_ENABLE, lackResultEnableStr)) {
        lackResultEnable = autil::StringUtil::fromString<bool>(lackResultEnableStr);
    }
    return lackResultEnable;
}

string QrsSqlHandler::getTraceLevel(const QrsSessionSqlRequest &sessionRequest) {
    std::string traceLevel;
    if (sessionRequest.sqlRequest == nullptr) {
        return "";
    }
    sessionRequest.sqlRequest->getValue(SQL_TRACE_LEVEL, traceLevel);
    autil::StringUtil::toUpperCase(traceLevel);
    if (traceLevel.empty()) {
        traceLevel = "ERROR";
    }
    return traceLevel;
}

bool QrsSqlHandler::getSearchInfo(const QrsSessionSqlRequest &sessionRequest) {
    std::string info;
    if (sessionRequest.sqlRequest == nullptr) {
        return false;
    }
    sessionRequest.sqlRequest->getValue(SQL_GET_SEARCH_INFO, info);
    if (info.empty()) {
        return false;
    }
    bool res = false;
    StringUtil::fromString(info, res);
    return res;
}

bool QrsSqlHandler::getOutputSqlPlan(const QrsSessionSqlRequest &sessionRequest) {
    std::string info;
    if (sessionRequest.sqlRequest == nullptr) {
        return false;
    }
    sessionRequest.sqlRequest->getValue(SQL_GET_SQL_PLAN, info);
    if (info.empty()) {
        return false;
    }
    bool res = false;
    StringUtil::fromString(info, res);
    return res;
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

END_HA3_NAMESPACE(service);
