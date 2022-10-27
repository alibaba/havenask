#ifndef ISEARCH_SQLACCESSLOG_H
#define ISEARCH_SQLACCESSLOG_H

#include <ha3/common.h>
#include <ha3/util/Log.h>
#include <string>
#include <tr1/memory>
#include <ha3/common/ErrorDefine.h>
#include <ha3/common/PhaseOneSearchInfo.h>
#include <ha3/sql/proto/SqlSearchInfo.pb.h>

BEGIN_HA3_NAMESPACE(common);

class SqlAccessLog
{
public:
    SqlAccessLog();
    SqlAccessLog(const std::string& clientIp);

    ~SqlAccessLog();
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
    int64_t getProcessTime() {
        return _processTime;
    }
    void setQueryString(const std::string &queryString) {
        _queryString = queryString;
    }
    const std::string& getQueryString() const {
        return _queryString;
    }
    void setRowCount(uint32_t rowCount) {
        _rowCount = rowCount;
    }
    uint32_t getRowCount() {
        return _rowCount;
    }
    void setResultLen(uint32_t resultLen) {
        _resultLen = resultLen;
    }
    uint32_t getResultLen() {
        return _resultLen;
    }
    void setLogSearchInfoThres(size_t thres) {
        _logSearchInfoThres = thres;
    }
    void setFormatResultTime(int64_t formatTime) {
        _formatTime = formatTime;
    }
    void setSearchInfo(const sql::SqlSearchInfo &searchInfo) {
        _searchInfo = searchInfo;
    }
    std::string getSearchInfoStr() {
        return _searchInfo.ShortDebugString();;
    }
    const sql::SqlSearchInfo& getSearchInfo() {
        return _searchInfo;
    }
    void setTraceId(const std::string &traceId) {
        _traceId = traceId;
    }
    const std::string &getTraceId() const {
        return _traceId;
    }
    void setSessionSrcType(SessionSrcType type) {
        _sessionSrcType = type;
    }
    SessionSrcType getSessionSrcTYPE() const {
        return _sessionSrcType;
    }
    void setUserData(const std::string &userData) {
        _userData = userData;
    }
    const std::string& getUserData() const {
        return _userData;
    }

private:
    void log();

private:
    std::string _ip;
    std::string _queryString;
    std::string _traceId;
    std::string _userData;
    int64_t _processTime;
    int64_t _formatTime;
    ErrorCode _statusCode;
    uint32_t _rowCount;
    uint32_t _resultLen;
    size_t _logSearchInfoThres;
    SessionSrcType _sessionSrcType = SESSION_SRC_UNKNOWN;
    sql::SqlSearchInfo _searchInfo;
private:
    friend class SqlAccessLogTest;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(SqlAccessLog);

END_HA3_NAMESPACE(common);

#endif //ISEARCH_SQLACCESSLOG_H
