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

#include "autil/CompressionUtil.h"
#include "google/protobuf/stubs/common.h"
#include "tensorflow/core/framework/graph.pb.h"

#include "ha3/monitor/Ha3BizMetrics.h"
#include "ha3/proto/QrsService.pb.h"
#include "ha3/sql/framework/QrsSessionSqlResult.h"
#include "ha3/sql/data/SqlQueryRequest.h"
#include "ha3/service/Session.h"
#include "ha3/sql/common/common.h"
#include "ha3/turing/qrs/QrsServiceSnapshot.h"
#include "ha3/turing/qrs/QrsSqlBiz.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace sql {
class SqlAccessLog;
class SqlSlowAccessLog;
class SqlErrorAccessLog;
class SqlAccessLogFormatHelper;
struct QrsSessionSqlRequest;
} // namespace sql
} // namespace isearch

namespace isearch {
namespace service {

class QrsSqlHandler;
struct QrsSqlHandlerResult;

struct QrsArpcSqlSessionEnv {
public:
    QrsArpcSqlSessionEnv();
    ~QrsArpcSqlSessionEnv();

public:
    static const QrsArpcSqlSessionEnv &get();

public:
    bool disableSoftFailure = false;
};

class QrsArpcSqlSession : public Session
{
public:
    QrsArpcSqlSession(SessionPool *pool = NULL);
    virtual ~QrsArpcSqlSession();
private:
    QrsArpcSqlSession(const QrsArpcSqlSession &);
    QrsArpcSqlSession& operator=(const QrsArpcSqlSession &);
public:
    void processRequest() override;
    void dropRequest() override;
    bool beginSession() override;
    void setRequest(const proto::QrsRequest *request, proto::QrsResponse *response,
                    RPCClosure *done, const turing::QrsServiceSnapshotPtr &snapshot);
    void afterProcess(QrsSqlHandlerResult handlerResult);
    void endQrsSession(sql::QrsSessionSqlResult &result);
    void setClientAddress(const std::string &clientAddress) {
        _clientAddress = clientAddress;
    }
    void reset() override;
    void setDropped();
protected:
    void reportMetrics(kmonitor::MetricsReporter *metricsReporter);

private:
    void fillResponse(const std::string &result,
                      autil::CompressType compressType,
                      sql::QrsSessionSqlResult::SqlResultFormatType formatType,
                      proto::QrsResponse *response);
    void endGigTrace(sql::QrsSessionSqlResult &result);
    void logSqlPattern(const sql::QrsSessionSqlResult &result);

private:
    inline proto::FormatType convertFormatType(
            sql::QrsSessionSqlResult::SqlResultFormatType formatType);
    bool initTimeoutTerminator(int64_t currentTime);
    static sql::QrsSessionSqlResult::SqlResultFormatType getFormatType(
            const sql::SqlQueryRequest *sqlQueryRequest,
            const turing::QrsSqlBizPtr &qrsSqlBiz);
    bool runSampleGraph(turing::QrsSqlBizPtr &qrsSqlBiz);
    void formatSqlResult(sql::QrsSessionSqlResult &sqlResult,
                         const sql::SqlAccessLogFormatHelper *accessLogHelper);
    bool isIquanError(const ErrorCode errorCode) const;
    bool isRunGraphError(const ErrorCode errorCode) const;
    bool getResultReadable(const sql::SqlQueryRequest &sqlRequest);
    bool getAllowSoftFailure(const sql::SqlQueryRequest &sqlRequest);    

protected:
    turing::QrsServiceSnapshotPtr _snapshot;
    proto::QrsResponse *_qrsResponse = nullptr;
    multi_call::GigClosure *_done = nullptr;
    // query input
    std::string _clientAddress;
    int64_t _timeout;
    autil::TimeoutTerminator *_timeoutTerminator = nullptr;
    sql::ErrorResult _errorResult;
    std::string _queryStr;
    std::string _bizName;
private:
    std::shared_ptr<sql::QrsSessionSqlRequest> _request;
    std::shared_ptr<sql::QrsSessionSqlResult> _result;
    QrsArpcSqlSessionEnv _env;    
    sql::SqlQueryRequestPtr _sqlQueryRequest;
    turing::QrsSqlBizPtr _qrsSqlBiz;
    sql::SqlAccessLog *_accessLog = nullptr;
    sql::SqlSlowAccessLog *_slowAccessLog = nullptr;
    sql::SqlErrorAccessLog *_errorAccessLog = nullptr;
    bool _useFirstBiz = true;
private:
    friend class QrsArpcSqlSessionTest;

private:
    AUTIL_LOG_DECLARE();
    static alog::Logger *_patternLogger;
};

typedef std::shared_ptr<QrsArpcSqlSession> QrsArpcSqlSessionPtr;

inline proto::FormatType QrsArpcSqlSession::convertFormatType(
        sql::QrsSessionSqlResult::SqlResultFormatType formatType)
{
    switch (formatType)
    {
    case sql::QrsSessionSqlResult::SQL_RF_JSON:
        return proto::FT_SQL_JSON;
    case sql::QrsSessionSqlResult::SQL_RF_STRING:
        return proto::FT_SQL_STRING;
    case sql::QrsSessionSqlResult::SQL_RF_FULL_JSON:
        return proto::FT_SQL_FULL_JSON;
    case sql::QrsSessionSqlResult::SQL_RF_FLATBUFFERS:
        return proto::FT_SQL_FLATBUFFERS;
    default:
        return proto::FT_SQL_UNKNOWN;
    }
}


inline bool QrsArpcSqlSession::isIquanError(const ErrorCode errorCode) const {
    return ERROR_SQL_PLAN_SERVICE <= errorCode &&
        errorCode <= ERROR_SQL_PLAN_SERVICE_TIMEOUT;
}

inline bool QrsArpcSqlSession::isRunGraphError(const ErrorCode errorCode) const {
    return ERROR_SQL_RUN_GRAPH <= errorCode &&
        errorCode <= ERROR_SQL_RUN_GRAPH_TIMEOUT;
}

} // namespace service
} // namespace isearch
