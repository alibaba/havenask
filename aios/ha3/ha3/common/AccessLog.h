#ifndef ISEARCH_ACCESSLOG_H
#define ISEARCH_ACCESSLOG_H

#include <ha3/common.h>
#include <ha3/util/Log.h>
#include <string>
#include <tr1/memory>
#include <ha3/common/ErrorDefine.h>
#include <ha3/common/PhaseOneSearchInfo.h>

BEGIN_HA3_NAMESPACE(common);

class AccessLog
{
public:
    AccessLog();
    AccessLog(const std::string& clientIp);

    ~AccessLog();
public:
    void setIp(const std::string &ip) {
        _ip = ip;
    }
    void setStatusCode(const ErrorCode &statusCode) {
        _statusCode = statusCode;
    }
    // millisecond
    void setProcessTime(int64_t processTime) {
        _processTime = processTime;
    }
    void setQueryString(const std::string &queryString) {
        _queryString = queryString;
    }
    const std::string& getQueryString() const {
        return _queryString;
    }
    void setTotalHits(uint32_t totalHits) {
        _totalHits = totalHits;
    }
    void setPhaseOneSearchInfo(const PhaseOneSearchInfo &searchInfo);
    void setRowKey(uint64_t key) {
        _rowKey = key;
    }
    void setTraceId(const std::string &traceId) {
        _traceId = traceId;
    }
    const std::string &getTraceId() const {
        return _traceId;
    }
    void setUserData(const std::string &userData) {
        _userData = userData;
    }
    const std::string& getUserData() const {
        return _userData;
    }
    void setSessionSrcType(SessionSrcType type) {
        _sessionSrcType = type;
    }
    SessionSrcType getSessionSrcTYPE() const {
        return _sessionSrcType;
    }

private:
    void log();

private:
    std::string _ip;
    std::string _queryString;
    std::string _traceId;
    std::string _userData;
    int64_t _processTime;
    ErrorCode _statusCode;
    uint32_t _totalHits;
    uint64_t _rowKey;
    SessionSrcType _sessionSrcType = SESSION_SRC_UNKNOWN;
    PhaseOneSearchInfo _searchInfo;

private:
    friend class AccessLogTest;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(AccessLog);

END_HA3_NAMESPACE(common);

#endif //ISEARCH_ACCESSLOG_H
