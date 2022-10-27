#ifndef ISEARCH_QRSARPCSQLSESSION_H
#define ISEARCH_QRSARPCSQLSESSION_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/service/Session.h>
#include <ha3/proto/QrsService.pb.h>
#include <ha3/monitor/Ha3BizMetrics.h>
#include <ha3/turing/qrs/QrsServiceSnapshot.h>
#include <ha3/turing/qrs/QrsSqlBiz.h>
#include <ha3/sql/common/common.h>
#include <ha3/service/QrsSessionSqlResult.h>
#include <tensorflow/core/framework/graph.pb.h>
#include <google/protobuf/stubs/common.h>

BEGIN_HA3_NAMESPACE(common);
class SqlAccessLog;
END_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(sql);
class SqlQueryRequest;
END_HA3_NAMESPACE(sql);

BEGIN_HA3_NAMESPACE(service);

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
    void endQrsSession(QrsSessionSqlResult &result);
    void setClientAddress(const std::string &clientAddress) {
        _clientAddress = clientAddress;
    }
    void reset() override;
    void setDropped();
    void setPoolCache(const suez::turing::PoolCachePtr &poolCache) {
        _poolCache = poolCache;
    }
protected:
    void reportMetrics(kmonitor::MetricsReporter *metricsReporter);

private:
    void recycleResource();
    void fillResponse(const std::string &result,
                      QrsSessionSqlResult::SqlResultFormatType formatType,
                      proto::QrsResponse *response);
private:
    inline proto::FormatType convertFormatType(
            QrsSessionSqlResult::SqlResultFormatType formatType);
    bool initTimeoutTerminator(int64_t currentTime);
    static QrsSessionSqlResult::SqlResultFormatType getFormatType(
            const sql::SqlQueryRequest *sqlQueryRequest,
            const turing::QrsSqlBizPtr &qrsSqlBiz);
    bool runSampleGraph(turing::QrsSqlBizPtr &qrsSqlBiz);
    void formatSqlResult(QrsSessionSqlResult &sqlResult);
    bool isIquanError(const ErrorCode errorCode) const;
    bool isRunGraphError(const ErrorCode errorCode) const;
    bool getResultReadable(const sql::SqlQueryRequest &sqlRequest);

protected:
    turing::QrsServiceSnapshotPtr _snapshot;
    proto::QrsResponse *_qrsResponse;
    multi_call::GigClosure *_done;
    // query input
    std::string _clientAddress;
    int64_t _timeout;
    suez::turing::TimeoutTerminator *_timeoutTerminator;
    common::ErrorResult _errorResult;
    suez::turing::PoolCachePtr _poolCache;
    std::string _queryStr;
private:
    sql::SqlQueryRequest *_sqlQueryRequest;
    turing::QrsSqlBizPtr _qrsSqlBiz;
    common::SqlAccessLog *_accessLog;
private:
    friend class QrsArpcSqlSessionTest;

private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(QrsArpcSqlSession);

inline proto::FormatType QrsArpcSqlSession::convertFormatType(
        QrsSessionSqlResult::SqlResultFormatType formatType)
{
    switch (formatType)
    {
    case QrsSessionSqlResult::SQL_RF_JSON:
        return proto::FT_SQL_JSON;
    case QrsSessionSqlResult::SQL_RF_STRING:
        return proto::FT_SQL_STRING;
    case QrsSessionSqlResult::SQL_RF_FULL_JSON:
        return proto::FT_SQL_FULL_JSON;
    case QrsSessionSqlResult::SQL_RF_FLATBUFFERS:
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

END_HA3_NAMESPACE(service);

#endif //ISEARCH_QRSARPCSQLSESSION_H
