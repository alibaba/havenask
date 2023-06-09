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
#include "ha3/service/QrsArpcSqlSession.h"

#ifdef AIOS_OPEN_SOURCE
#include <aios/network/opentelemetry/core/Span.h>
#else
#include <Span.h>
#endif
#include <assert.h>
#include <cstddef>
#include <map>
#include <utility>

#include "alog/Logger.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/TimeoutTerminator.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/PoolBase.h"
#include "ha3/common/ErrorResult.h"
#include "ha3/common/TimeoutTerminator.h"
#include "ha3/config/SqlConfig.h"
#include "ha3/isearch.h"
#include "ha3/monitor/Ha3BizMetrics.h"
#include "ha3/monitor/QrsBizMetrics.h"
#include "ha3/monitor/SessionMetricsCollector.h"
#include "ha3/proto/ProtoClassUtil.h"
#include "ha3/proto/QrsService.pb.h"
#include "ha3/service/QrsSqlHandler.h"
#include "ha3/sql/common/common.h"
#include "ha3/sql/common/SqlAuthManager.h"
#include "ha3/sql/framework/QrsSessionSqlRequest.h"
#include "ha3/sql/framework/SqlAccessLog.h"
#include "ha3/sql/framework/SqlSlowAccessLog.h"
#include "ha3/sql/framework/SqlErrorAccessLog.h"
#include "ha3/sql/framework/SqlAccessLogFormatHelper.h"
#include "ha3/sql/data/SqlQueryRequest.h"
#include "ha3/sql/framework/SqlResultFormatter.h"
#include "ha3/turing/common/Ha3BizMeta.h"
#include "ha3/turing/common/ModelConfig.h"
#include "ha3/util/TypeDefine.h"
#include "kmonitor/client/MetricType.h"
#include "kmonitor/client/MetricsReporter.h"
#include "kmonitor/client/core/MetricsTags.h"
#include "aios/network/gig/multi_call/common/ErrorCode.h"
#include "aios/network/gig/multi_call/interface/QuerySession.h"
#include "aios/network/gig/multi_call/rpc/GigClosure.h"
#include "suez/turing/search/SearchContext.h"
#include "tensorflow/core/framework/resource_handle.pb.h"
#ifndef AIOS_OPEN_SOURCE
#include "lockless_allocator/LocklessApi.h"
#endif
#include "aios/network/opentelemetry/core/eagleeye/EagleeyeUtil.h"

namespace isearch {
namespace service {
class SessionPool;
}  // namespace service
}  // namespace isearch

using namespace std;
using namespace autil;
using namespace tensorflow;
using namespace multi_call;
using namespace autil;
using namespace kmonitor;

using namespace isearch::common;
using namespace isearch::monitor;
using namespace isearch::sql;
using namespace isearch::proto;
using namespace isearch::turing;
using namespace isearch::util;

namespace isearch {
namespace service {
AUTIL_LOG_SETUP(ha3, QrsArpcSqlSession);
alog::Logger* QrsArpcSqlSession::_patternLogger = alog::Logger::getLogger("sql.SqlPatternLog");

QrsArpcSqlSessionEnv::QrsArpcSqlSessionEnv() {
    disableSoftFailure = autil::EnvUtil::getEnv("disableSoftFailure", disableSoftFailure);
}

QrsArpcSqlSessionEnv::~QrsArpcSqlSessionEnv() {
}

const QrsArpcSqlSessionEnv &QrsArpcSqlSessionEnv::get() {
    static QrsArpcSqlSessionEnv instance;
    return instance;
}

QrsArpcSqlSession::QrsArpcSqlSession(SessionPool *pool)
    : Session(pool)
    , _qrsResponse(nullptr)
    , _done(nullptr)
    , _timeout(std::numeric_limits<int64_t>::max())
    , _timeoutTerminator(nullptr)
    , _env(QrsArpcSqlSessionEnv::get())
    , _accessLog(nullptr)
    , _slowAccessLog(nullptr)
    , _errorAccessLog(nullptr)
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
    _timeout = std::numeric_limits<int64_t>::max();
    POOL_DELETE_CLASS(_timeoutTerminator);
    _timeoutTerminator = nullptr;
    _queryStr.clear();
    _bizName.clear();
    _sqlQueryRequest.reset();
    _qrsSqlBiz.reset();
    POOL_DELETE_CLASS(_accessLog);
    POOL_DELETE_CLASS(_slowAccessLog);
    POOL_DELETE_CLASS(_errorAccessLog);
    _accessLog = nullptr;
    _slowAccessLog = nullptr;
    _errorAccessLog = nullptr;
    _request.reset();
    _result.reset();
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
    if (request->has_bizname()) {
        _bizName = request->bizname();
        if (!_bizName.empty()) {
            _useFirstBiz = false;
        }
    }
    if (request->has_traceid() || request->has_rpcid() || request->has_userdata()) {
        _gigQuerySession->setEagleeyeUserData(request->traceid(),
                                              request->rpcid(),
                                              request->userdata());
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
            format = qrsSqlBiz->getSqlConfig()->sqlConfig.outputFormat;
        }
    }
    return QrsSessionSqlResult::strToType(format);
}

void QrsArpcSqlSession::formatSqlResult(QrsSessionSqlResult &sqlResult,
                                        const SqlAccessLogFormatHelper *accessLogHelper) {
    sqlResult.formatType = getFormatType(_sqlQueryRequest.get(), _qrsSqlBiz);
    if (_sqlQueryRequest) {
        _sqlQueryRequest->getValue(SQL_FORMAT_DESC, sqlResult.formatDesc);
    }
    SqlResultFormatter::format(sqlResult, accessLogHelper, getMemPool());
}

bool QrsArpcSqlSession::beginSession() {
    if (!Session::beginSession()) {
        return false;
    }
    _accessLog = POOL_NEW_CLASS(getMemPool(), SqlAccessLog);
    _sqlQueryRequest.reset(new SqlQueryRequest());
    if (!_sqlQueryRequest->init(_queryStr)) {
        _errorResult.resetError(ERROR_SQL_INIT_REQUEST, "sql request init failed, " + _queryStr);
        AUTIL_LOG(WARN, "parse query failed. [%s]", _queryStr.c_str());
        return false;
    }
    if (_bizName.empty()) {
        if (!_sqlQueryRequest->getValue(SQL_BIZ_NAME, _bizName) || _bizName.empty()) {
            _bizName = _snapshot->getBizName(DEFAULT_SQL_BIZ_NAME, {});
        }
    }
    _qrsSqlBiz = dynamic_pointer_cast<turing::QrsSqlBiz>(_snapshot->getBiz(_bizName));
    if (_qrsSqlBiz == nullptr) {
        _qrsSqlBiz = dynamic_pointer_cast<turing::QrsSqlBiz>(_snapshot->getBiz(_bizName + HA3_LOCAL_QRS_BIZ_NAME_SUFFIX));
    }
    if (_qrsSqlBiz == nullptr && _useFirstBiz) {
        _qrsSqlBiz = _snapshot->getFirstQrsSqlBiz();
    }
    if (_qrsSqlBiz == nullptr) {
        _errorResult.resetError(ERROR_BIZ_NOT_FOUND, "biz not fount: " + _bizName);
        AUTIL_LOG(WARN, "sql biz [%s] not exist.", _bizName.c_str());
        return false;
    }
    if (_qrsSqlBiz->getSqlConfig()) {
        _accessLog->setLogSearchInfoThres(_qrsSqlBiz->getSqlConfig()->sqlConfig.logSearchInfoThreshold);
    }
    return true;
}

void QrsArpcSqlSession::processRequest() {
#ifndef AIOS_OPEN_SOURCE
    MallocPoolScope mallocScope;
#endif
    AUTIL_LOG(DEBUG, "QrsArpcSqlSession::handleSearchClientRequest");

    if (!beginSession()) {
        AUTIL_LOG(WARN, "Qrs begin session failed.");
        QrsSessionSqlResult result;
        result.errorResult = _errorResult;
        endQrsSession(result);
        return;
    }
    auto *manager = _qrsSqlBiz->getSqlAuthManager();
    if (manager && !_sqlQueryRequest->checkAuth(*manager)) {
        AUTIL_LOG(WARN, "request unauthorized, query rejected: %s", _queryStr.c_str());
        _errorResult.resetError(ERROR_SQL_UNAUTHORIZED, "sql request unauthorized, " + _queryStr);
        QrsSessionSqlResult result;
        result.errorResult = _errorResult;
        endQrsSession(result);
        return;
    }
    if (_timeout == std::numeric_limits<int64_t>::max()) {
        std::string sqlTimeoutStr;
        if (_sqlQueryRequest->getValue(SQL_TIMEOUT, sqlTimeoutStr)) {
            _timeout = autil::StringUtil::fromString<size_t>(sqlTimeoutStr) * 1000;
        } else {
            _timeout = _qrsSqlBiz->getSqlConfig()->sqlConfig.sqlTimeout * 1000;
        }
    }

    int64_t currentTime = autil::TimeUtility::currentTime();
    if (!initTimeoutTerminator(currentTime)) {
        string errorMsg = "Qrs wait in queue timeout!";
        AUTIL_LOG(WARN, "%s", errorMsg.c_str());
        QrsSessionSqlResult result;
        result.errorResult.resetError(ERROR_QRS_OUTPOOLQUEUE_TIMEOUT, errorMsg);
        endQrsSession(result);
        return;
    }
    assert(_timeoutTerminator);

    _request.reset(new QrsSessionSqlRequest(_sqlQueryRequest,
                                            _sessionMetricsCollectorPtr));
    _result.reset(new QrsSessionSqlResult());
    _result->readable = getResultReadable(*_sqlQueryRequest);
    _result->allowSoftFailure = getAllowSoftFailure(*_sqlQueryRequest);
    _result->sqlQuery = _queryStr;

    auto sqlHandler = new QrsSqlHandler(_qrsSqlBiz, _gigQuerySession, _timeoutTerminator);
    sqlHandler->setUseGigSrc(_useGigSrc);
    sqlHandler->setGDBPtr(this);
    sqlHandler->process(_request.get(), _result.get(), [this](QrsSqlHandlerResult handlerResult) {
        this->afterProcess(std::move(handlerResult));
    });
}

void QrsArpcSqlSession::afterProcess(QrsSqlHandlerResult handlerResult) {
    if (_accessLog) {
        _accessLog->setRowCount(handlerResult.finalOutputNum);
        auto formatType = getFormatType(_sqlQueryRequest.get(), _qrsSqlBiz);
        _accessLog->setSearchInfo(std::move(handlerResult.searchInfo),
                                  formatType != QrsSessionSqlResult::SQL_RF_FLATBUFFERS_TIMELINE);
        _accessLog->setRpcInfoMap(std::move(handlerResult.rpcInfoMap));
    }
    endQrsSession(*_result);
}

// in failSession zoneResource and bizInfoManager is null ptr, should be checked!
void QrsArpcSqlSession::endQrsSession(sql::QrsSessionSqlResult &result) {
    std::unique_ptr<SqlAccessLogFormatHelper> accessLogHelper;
    if (_accessLog) {
        _accessLog->setSessionSrcType(_sessionSrcType);
        _accessLog->setProcessTime(getCurrentTime() - getStartTime()); // for return time
        accessLogHelper = make_unique<SqlAccessLogFormatHelper>(*_accessLog);

        if (!result.allowSoftFailure && accessLogHelper->hasSoftFailure()
            && !result.errorResult.hasError()) {
            result.errorResult.resetError(ERROR_SQL_SOFT_FAILURE_NOT_ALLOWED,
                                          "soft failure is not allowed");
        }
    }

    formatSqlResult(result, accessLogHelper.get());
    fillResponse(result.resultStr, result.resultCompressType, result.formatType, _qrsResponse);
    if (_gigQuerySession) {
        endGigTrace(result);
    }
    if (_done) {
        _done->setErrorCode(result.multicallEc);
        _done->Run();
    }
    logSqlPattern(result);
    int64_t formatUseTime = 0;
    if (_sessionMetricsCollectorPtr != nullptr) {
        _sessionMetricsCollectorPtr->sqlFormatEndTrigger();
        formatUseTime = _sessionMetricsCollectorPtr->getSqlFormatTime();
        if (_accessLog) {
            _sessionMetricsCollectorPtr->setSqlRowCount(_accessLog->getRowCount());
        }
        if (_snapshot != nullptr && _snapshot->getSqlClient() != nullptr) {
            _sessionMetricsCollectorPtr->setSqlCacheKeyCount(_snapshot->getSqlClient()->getPlanCacheKeyCount());
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
            if (accessLogHelper && accessLogHelper->hasSoftFailure()) {
                _sessionMetricsCollectorPtr->increaseSqlSoftFailureQps();
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
        if (_qrsSqlBiz && _qrsSqlBiz->getSqlConfig() != nullptr) {
            const auto &sqlConfig = _qrsSqlBiz->getSqlConfig()->sqlConfig;
            if (sqlConfig.needPrintSlowLog && _accessLog->getStatusCode() == ERROR_NONE &&
                _accessLog->getProcessTime() > sqlConfig.slowQueryFactory * _timeout) {
                _slowAccessLog = POOL_NEW_CLASS(
                        getMemPool(), SqlSlowAccessLog, _accessLog->getSqlAccessInfo());
            }
            if (sqlConfig.needPrintErrorLog && _accessLog->getStatusCode() != ERROR_NONE) {
                _errorAccessLog = POOL_NEW_CLASS(
                        getMemPool(), SqlErrorAccessLog, _accessLog->getSqlAccessInfo());
            }
        }
    }
    Session::endSession(); // to collect metrics
    if (_qrsSqlBiz) {
        if (_sqlQueryRequest) {
            map<string, string> tagsMap = {
                {"role_type", "qrs"},
                {"src", _sqlQueryRequest->getSourceSpec()},
                {"db_name", _sqlQueryRequest->getDatabaseName()},
                {"keyword", _sqlQueryRequest->getKeyword()},
                {"session_src", transSessionSrcType(_sessionSrcType)}
            };
            suez::turing::SearchContext::addGigMetricTags(_gigQuerySession.get(), tagsMap, _useGigSrc);
            MetricsTags tags(tagsMap);
            auto reporter = _qrsSqlBiz->getQueryMetricsReporter(tags, "kmon");
            auto oldReporter = _qrsSqlBiz->getQueryMetricsReporter({});
            reportMetrics(reporter.get());
            reportMetrics(oldReporter.get());
        } else {
            _qrsSqlBiz->getBizMetricsReporter()->report(1, "session.error.qps", kmonitor::QPS, nullptr);
        }
    }
    destroy();
}

void QrsArpcSqlSession::endGigTrace(QrsSessionSqlResult &result) {
    auto span = _gigQuerySession->getTraceServerSpan();
    if (span) {
        if (_accessLog) {
            _accessLog->setTraceId(opentelemetry::EagleeyeUtil::getETraceId(span));
            const map<string, string> &userDataMap = opentelemetry::EagleeyeUtil::getEUserDatas(span);
            string userData;
            for (auto iter = userDataMap.begin(); iter != userDataMap.end(); iter++) {
                if (iter->first == "s") {
                    continue;
                }
                userData += iter->first + "=" + iter->second + ";";
            }
            _accessLog->setUserData(userData);
        }
        span->setAttribute(opentelemetry::kSpanAttrRespContentLength, std::to_string(_qrsResponse->ByteSize()));
        if (result.errorResult.hasError()) {
            span->setStatus(opentelemetry::StatusCode::kError, result.errorResult.getErrorDescription());
        } else {
            span->setStatus(opentelemetry::StatusCode::kOk);
        }
    }
}

void QrsArpcSqlSession::logSqlPattern(const sql::QrsSessionSqlResult &result) {
    static constexpr auto logLevel = alog::LOG_LEVEL_INFO;
    if (!_sqlQueryRequest) {
        return;
    }
    if (!_patternLogger->isLevelEnabled(logLevel)) {
        return;
    }
    auto queryHashKey = result.sqlQueryRequest.queryHashKey;
    if (queryHashKey == 0u) {
        return;
    }
    auto tableCount = result.getTableRowCount();
    if (tableCount == 0u) {
        return;
    }

#ifndef AIOS_OPEN_SOURCE
    DisablePoolScope disableScope;
#endif
    if (!_snapshot->getSqlQueryCache()->getAndPut(queryHashKey)) {
        return;
    }
    const auto &pattern = _sqlQueryRequest->toPatternString(queryHashKey);
    if (!pattern.empty()) {
        _patternLogger->log(logLevel, "%s\n", pattern.c_str());
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
                                     autil::CompressType compressType,
                                     QrsSessionSqlResult::SqlResultFormatType formatType,
                                     proto::QrsResponse *response)
{
    string compressedResult;
    bool compressed = false;
    if (compressType != autil::CompressType::NO_COMPRESS
        && compressType != autil::CompressType::INVALID_COMPRESS_TYPE
        && getMemPool() != nullptr)
    {
        _sessionMetricsCollectorPtr->resultCompressStartTrigger();
        compressed = autil::CompressionUtil::compress(result, compressType,
                compressedResult, getMemPool());
        _sessionMetricsCollectorPtr->resultCompressEndTrigger();

        if (compressed) {
            _sessionMetricsCollectorPtr->setIsCompress(true);
            _sessionMetricsCollectorPtr->resultCompressLengthTrigger(compressedResult.size());
            response->set_compresstype(ProtoClassUtil::convertCompressType(compressType));
            response->set_assemblyresult(compressedResult);
        }
    }

    if (!compressed) {
        response->set_compresstype(proto::CT_NO_COMPRESS);
        response->set_assemblyresult(result);
    }
    response->set_formattype(convertFormatType(formatType));
}

void QrsArpcSqlSession::setDropped() {
    if (_sessionMetricsCollectorPtr) {
        _sessionMetricsCollectorPtr->increaseProcessErrorQps();
    }
}

bool QrsArpcSqlSession::initTimeoutTerminator(int64_t currentTime) {
    if (currentTime - getStartTime() >= _timeout) {
        AUTIL_LOG(WARN, "wait in pool timeout, start time [%ld], current time [%ld], "
                "rpc timeout [%ld] us", getStartTime(), currentTime, _timeout);
        return false;
    }
    if (_timeoutTerminator) {
        POOL_DELETE_CLASS(_timeoutTerminator);
        _timeoutTerminator = NULL;
    }
    _timeoutTerminator = POOL_NEW_CLASS(getMemPool(), TimeoutTerminator, _timeout, getStartTime());
    _timeoutTerminator->init(1);
    AUTIL_LOG(DEBUG, "init timeout terminator, start time [%ld], current time [%ld], "
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

bool QrsArpcSqlSession::getAllowSoftFailure(const sql::SqlQueryRequest &sqlRequest) {
    std::string info;
    sqlRequest.getValue(SQL_RESULT_ALLOW_SOFT_FAILURE, info);
    if (info.empty()) {
        return !_env.disableSoftFailure;
    }
    bool res = false;
    StringUtil::fromString(info, res);
    return res;
}

} // namespace service
} // namespace isearch
