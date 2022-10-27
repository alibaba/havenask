#include <ha3/service/QrsArpcSqlSession.h>
#include <ha3/common/ErrorResult.h>
#include <ha3/common/XMLResultFormatter.h>
#include <ha3/service/QrsSqlHandler.h>
#include <ha3/sql/framework/SqlResultFormatter.h>
#include <ha3/common/SqlAccessLog.h>
#include <ha3/sql/framework/SqlQueryRequest.h>
#include <autil/StringUtil.h>
#include <tensorflow/core/graph/node_builder.h>
#include <suez/turing/search/SearchContext.h>

using namespace std;
using namespace autil;
using namespace tensorflow;
using namespace multi_call;
using namespace autil;
using namespace kmonitor;

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(monitor);
USE_HA3_NAMESPACE(sql);
USE_HA3_NAMESPACE(proto);
USE_HA3_NAMESPACE(turing);
BEGIN_HA3_NAMESPACE(service);
HA3_LOG_SETUP(service, QrsArpcSqlSession);


QrsArpcSqlSession::QrsArpcSqlSession(SessionPool *pool)
    : Session(pool)
    , _qrsResponse(nullptr)
    , _done(nullptr)
    , _timeout(-1)
    , _timeoutTerminator(nullptr)
    , _sqlQueryRequest(nullptr)
    , _accessLog(nullptr)
{
}

QrsArpcSqlSession::~QrsArpcSqlSession() {
    reset();
}

void QrsArpcSqlSession::dropRequest() {
    QrsSessionSqlResult result;
    result.errorResult.resetError(ERROR_REQUEST_DROPPED, "Server is too busy, thread pool queue is full\n");
    endQrsSession(result);
}

void QrsArpcSqlSession::reset() {
    _snapshot.reset();
    _qrsResponse = nullptr;
    _done = nullptr;
    _clientAddress.clear();
    _timeout = -1;
    POOL_DELETE_CLASS(_timeoutTerminator);
    _timeoutTerminator = nullptr;
	_queryStr.clear();
    POOL_DELETE_CLASS(_sqlQueryRequest);
    _sqlQueryRequest = nullptr;
	_qrsSqlBiz.reset();
    POOL_DELETE_CLASS(_accessLog);
    _accessLog = nullptr;
    recycleResource();
    Session::reset();
}

void QrsArpcSqlSession::setRequest(const proto::QrsRequest *request,
                                   proto::QrsResponse *response,
                                   RPCClosure *done,
                                   const turing::QrsServiceSnapshotPtr &snapshot)
{
    _qrsResponse = response;
    _done = dynamic_cast<multi_call::GigClosure *>(done);
    assert(_done);
    _snapshot = snapshot;
    if (request->has_timeout()) {
        _timeout = request->timeout() * 1000;
    }
    if (request->has_assemblyquery()) {
        _queryStr = request->assemblyquery();
    }
}

QrsSessionSqlResult::SqlResultFormatType QrsArpcSqlSession::getFormatType(
        const sql::SqlQueryRequest *sqlQueryRequest,
        const turing::QrsSqlBizPtr &qrsSqlBiz)
{
    if (sqlQueryRequest == nullptr) {
        return QrsSessionSqlResult::SQL_RF_STRING;
    }
    string format;
    sqlQueryRequest->getValue(SQL_FORMAT_TYPE, format);
    if (format.empty()) {
        sqlQueryRequest->getValue(SQL_FORMAT_TYPE_NEW, format);
        if (format.empty()) {
            if (qrsSqlBiz == nullptr) {
                return QrsSessionSqlResult::SQL_RF_STRING;
            }
            format = qrsSqlBiz->getSqlConfig()->outputFormat;
        }
    }
    return QrsSessionSqlResult::strToType(format);
}

void QrsArpcSqlSession::formatSqlResult(QrsSessionSqlResult &sqlResult) {
    sqlResult.formatType = getFormatType(_sqlQueryRequest, _qrsSqlBiz);
    if (_sqlQueryRequest) {
        _sqlQueryRequest->getValue(SQL_FORMAT_DESC, sqlResult.formatDesc);
    }
    SqlResultFormatter::format(sqlResult, _accessLog, _pool);
}

bool QrsArpcSqlSession::beginSession() {
    _pool = _poolCache->get();
    _accessLog = POOL_NEW_CLASS(_pool, SqlAccessLog);
    if (!Session::beginSession()) {
        return false;
    }
    _sqlQueryRequest = POOL_NEW_CLASS(_pool, SqlQueryRequest);
    if (!_sqlQueryRequest->init(_queryStr)) {
        _errorResult.resetError(ERROR_SQL_INIT_REQUEST, "sql request init failed, " + _queryStr);
        HA3_LOG(WARN, "parse query failed. [%s]", _queryStr.c_str());
        return false;
    }
    string bizName;
    if (!_sqlQueryRequest->getValue(SQL_BIZ_NAME, bizName) || bizName.empty()) {
        bizName = _snapshot->getBizName(DEFAULT_SQL_BIZ_NAME);
    }
    // bizName not work, if not found, return first biz in map
    _qrsSqlBiz = dynamic_pointer_cast<turing::QrsSqlBiz>(_snapshot->getBiz(bizName));
    if (!_qrsSqlBiz) {
        _errorResult.resetError(ERROR_BIZ_NOT_FOUND, "biz not fount: " + bizName);
        HA3_LOG(WARN, "sql biz [%s] not exist.", bizName.c_str());
        return false;
    }
    if (_qrsSqlBiz->getSqlConfig()) {
        _accessLog->setLogSearchInfoThres(_qrsSqlBiz->getSqlConfig()->logSearchInfoThreshold);
    }
    return true;
}

void QrsArpcSqlSession::processRequest() {
    HA3_LOG(DEBUG, "QrsArpcSqlSession::handleSearchClientRequest");
    if (!beginSession()) {
        HA3_LOG(WARN, "Qrs begin session failed.");
        QrsSessionSqlResult result;
        result.errorResult = _errorResult;
        endQrsSession(result);
        return;
    }

    if (_timeout < 0) {
        std::string sqlTimeoutStr;
        if (_sqlQueryRequest->getValue(SQL_TIMEOUT, sqlTimeoutStr)) {
            _timeout = autil::StringUtil::fromString<size_t>(sqlTimeoutStr) * 1000;
        } else {
            _timeout = _qrsSqlBiz->getSqlConfig()->sqlTimeout * 1000;
        }
    }

    int64_t currentTime = autil::TimeUtility::currentTime();
    if (!initTimeoutTerminator(currentTime)) {
        string errorMsg = "Qrs wait in queue timeout!";
        HA3_LOG(WARN, "%s", errorMsg.c_str());
        QrsSessionSqlResult result;
        result.errorResult.resetError(ERROR_QRS_OUTPOOLQUEUE_TIMEOUT, errorMsg);
        endQrsSession(result);
        return;
    }
    assert(_timeoutTerminator);

    QrsSessionSqlRequest request(_sqlQueryRequest, _pool,
                                 _sessionMetricsCollectorPtr);
    QrsSessionSqlResult result;
    result.readable = getResultReadable(*_sqlQueryRequest);
    {
        QrsSqlHandler sqlHandler(_qrsSqlBiz, _gigQuerySession, _timeoutTerminator);
        sqlHandler.setUseGigSrc(_useGigSrc);
        try {
            sqlHandler.process(request, result);
            if (_accessLog) {
                _accessLog->setRowCount(sqlHandler.getFinalOutputNum());
                _accessLog->setSearchInfo(sqlHandler.getSearchInfo());
            }
        } catch (const std::exception& e) {
            HA3_LOG(ERROR, "EXCEPTION: %s", e.what());
            result.errorResult.resetError(ERROR_UNKNOWN, e.what());
        } catch (...) {
            HA3_LOG(ERROR, "UnknownException");
            result.errorResult.resetError(ERROR_UNKNOWN, "UnknownException");
        }
    }
    endQrsSession(result);
}

// in failSession zoneResource and bizInfoManager is null ptr, should be checked!
void QrsArpcSqlSession::endQrsSession(QrsSessionSqlResult &result) {
    if (_accessLog) {
        _accessLog->setSessionSrcType(_sessionSrcType);
        _accessLog->setProcessTime(getCurrentTime() - getStartTime()); // for return time
    }
    formatSqlResult(result);
    fillResponse(result.resultStr, result.formatType, _qrsResponse);
    int64_t formatUseTime = 0;
    if (_sessionMetricsCollectorPtr != nullptr) {
        _sessionMetricsCollectorPtr->sqlFormatEndTrigger();
        formatUseTime = _sessionMetricsCollectorPtr->getSqlFormatTime();
        if (_accessLog) {
            _sessionMetricsCollectorPtr->setSqlRowCount(_accessLog->getRowCount());
        }
        _sessionMetricsCollectorPtr->setSqlResultSize(result.resultStr.size());
        if (result.errorResult.hasError()) {
            if (isIquanError(result.errorResult.getErrorCode())) {
                _sessionMetricsCollectorPtr->increaseSqlPlanErrorQps();
            }
            if (isRunGraphError(result.errorResult.getErrorCode())) {
                _sessionMetricsCollectorPtr->increaseSqlRunGraphErrorQps();
            }
            _sessionMetricsCollectorPtr->increaseProcessSqlErrorQps();
            if (_pool) {
                _sessionMetricsCollectorPtr->setMemPoolUsedBufferSize(_pool->getTotalBytes());
                _sessionMetricsCollectorPtr->setMemPoolAllocatedBufferSize(_pool->getAllocatedSize());
            }
        } else {
            if (_accessLog && (_accessLog->getRowCount() == 0)) {
                _sessionMetricsCollectorPtr->increaseSqlEmptyQps();
            }
        }
    }
    if (_accessLog) {
        _accessLog->setFormatResultTime(formatUseTime);
        _accessLog->setResultLen(result.resultStr.size());
        _accessLog->setIp(_clientAddress);
        _accessLog->setQueryString(_queryStr);
        _accessLog->setProcessTime(getCurrentTime() - getStartTime());
        _accessLog->setStatusCode(result.errorResult.getErrorCode());
    }
    Session::endSession(); // to collect metrics
    if (_qrsSqlBiz) {
        if (_sqlQueryRequest) {
            map<string, string> tagsMap = {
                {"role_type", "qrs"},
                {"src", _sqlQueryRequest->getSourceSpec()},
                {"session_src", transSessionSrcType(_sessionSrcType)}
            };
            suez::turing::SearchContext::addGigMetricTags(_gigQuerySession, tagsMap, _useGigSrc);
            MetricsTags tags(tagsMap);
            auto reporter = _qrsSqlBiz->getQueryMetricsReporter(tags, "kmon");
            auto oldReporter = _qrsSqlBiz->getQueryMetricsReporter({});
            reportMetrics(reporter.get());
            reportMetrics(oldReporter.get());
        } else {
            _qrsSqlBiz->getBizMetricsReporter()->report(1, "session.error.qps", kmonitor::QPS, nullptr);
        }
    }
    if (_gigQuerySession) {
        auto rpcContext = _gigQuerySession->getRpcContext();
        if (rpcContext) {
            if (_accessLog) {
                _accessLog->setTraceId(rpcContext->getTraceId());
                const map<string, string> &userDataMap = rpcContext->getUserDataMap();
                string userData;
                for (auto iter = userDataMap.begin(); iter != userDataMap.end(); iter++) {
                    if (iter->first == "s") {
                        continue;
                    }
                    userData += iter->first + "=" + iter->second + ";";
                }
                _accessLog->setUserData(userData);
            }
            rpcContext->setRespSize(_qrsResponse->ByteSize());
            if (!result.errorResult.hasError()) {
                rpcContext->serverEnd(sap::RpcContext::SUCCESS, "");
            } else {
                rpcContext->serverEnd(sap::RpcContext::FAILED,
                        result.errorResult.getErrorDescription());
            }
        }
    }
    if (_done) {
        _done->setErrorCode(result.multicallEc);
        _done->Run();
    }
    destroy();
}


void QrsArpcSqlSession::recycleResource() {
    if (_poolCache && _pool) {
        _poolCache->put(_pool);
        _poolCache.reset();
        _pool = nullptr;
    }
}

void QrsArpcSqlSession::reportMetrics(MetricsReporter *metricsReporter) {
    if (_sessionMetricsCollectorPtr && metricsReporter) {
        _sessionMetricsCollectorPtr->setRequestType(SessionMetricsCollector::SqlType);
        metricsReporter->report<QrsBizMetrics, SessionMetricsCollector>(
                nullptr, _sessionMetricsCollectorPtr.get());
    }
}

void QrsArpcSqlSession::fillResponse(const std::string &result,
                                     QrsSessionSqlResult::SqlResultFormatType formatType,
                                     proto::QrsResponse *response)
{
    response->set_compresstype(proto::CT_NO_COMPRESS);
    response->set_assemblyresult(result);
    response->set_formattype(convertFormatType(formatType));
}

void QrsArpcSqlSession::setDropped() {
    if (_sessionMetricsCollectorPtr) {
        _sessionMetricsCollectorPtr->increaseProcessErrorQps();
    }
}

bool QrsArpcSqlSession::initTimeoutTerminator(int64_t currentTime) {
    if (_timeout >= 0 && currentTime - getStartTime() >= _timeout) {
        HA3_LOG(WARN, "wait in pool timeout, start time [%ld], current time [%ld], "
                "rpc timeout [%ld] us", getStartTime(), currentTime, _timeout);
        return false;
    }
    if (_timeoutTerminator) {
        POOL_DELETE_CLASS(_timeoutTerminator);
        _timeoutTerminator = NULL;
    }
    _timeoutTerminator = POOL_NEW_CLASS(_pool, TimeoutTerminator, _timeout, getStartTime());
    _timeoutTerminator->init(1);
    HA3_LOG(DEBUG, "init timeout terminator, start time [%ld], current time [%ld], "
            "rpc timeout [%ld] us", getStartTime(), currentTime, _timeout);
    return true;
}

bool QrsArpcSqlSession::getResultReadable(const sql::SqlQueryRequest &sqlRequest) {
    std::string info;
    sqlRequest.getValue(SQL_RESULT_READABLE, info);
    if (info.empty()) {
        return false;
    }
    bool res = false;
    StringUtil::fromString(info, res);
    return res;
}

END_HA3_NAMESPACE(service);
