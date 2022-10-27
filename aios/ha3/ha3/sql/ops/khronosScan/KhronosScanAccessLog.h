#pragma once

#include <ha3/common.h>
#include <ha3/common/SearcherAccessLog.h>
#include <string>

BEGIN_HA3_NAMESPACE(sql);

class KhronosScanAccessLog
{
public:
    enum StatusCode {
        KAL_STATUS_ALL_FINISHED,
        KAL_STATUS_UNKNOWN,
        KAL_STATUS_INIT_FAILED,
        KAL_STATUS_INIT_FINISHED,
        KAL_STATUS_SCAN_FAILED,
    };
public:
    KhronosScanAccessLog();
    ~KhronosScanAccessLog();
public:
    void setStatusCode(StatusCode sc) {
        _statusCode = sc;
    }
    void setQueryString(const std::string &queryString) {
        _queryString = queryString;
    }
    void setTimeoutQuota(int64_t timeoutQuota) {
        // microsecond
        _timeoutQuota = timeoutQuota;
    }
    void setInitTime(int64_t initTime) {
        // microsecond
        _initTime = initTime;
    }
    void setScanTime(int64_t scanTime) {
        // microsecond
        _scanTime = scanTime;
    }
    void setKernelPoolSize(int64_t kernelPoolSize) {
        // byte
        _kernelPoolSize = kernelPoolSize;
    }
    void setQueryPoolSize(int64_t queryPoolSize) {
        // byte
        _queryPoolSize = queryPoolSize;
    }
private:
    common::SearcherAccessLog _searcherAccessLog;
    StatusCode _statusCode;
    std::string _queryString;
    int64_t _timeoutQuota;
    int64_t _initTime;
    int64_t _scanTime;
    int64_t _kernelPoolSize;
    int64_t _queryPoolSize;
};

HA3_TYPEDEF_PTR(KhronosScanAccessLog);

END_HA3_NAMESPACE(sql);
