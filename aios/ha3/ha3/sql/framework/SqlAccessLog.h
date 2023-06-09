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

#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>

#include "autil/Log.h"
#include "ha3/common/ErrorDefine.h"
#include "ha3/isearch.h"
#include "ha3/sql/common/Log.h" // IWYU pragma: keep
#include "ha3/sql/proto/SqlSearchInfo.pb.h"
#include "aios/network/gig/multi_call/stream/GigStreamRpcInfo.h"

namespace isearch {
namespace sql {

struct SqlAccessInfo {
    SqlAccessInfo(const std::string &clientIp)
        : ip(clientIp)
        , processTime(0)
        , formatTime(0)
        , statusCode(0)
        , rowCount(0)
    {
    }
    std::string ip;
    std::string queryString;
    std::string traceId;
    std::string userData;
    int64_t processTime;
    int64_t formatTime;
    isearch::ErrorCode statusCode;
    uint32_t rowCount;
    uint32_t resultLen;
    size_t logSearchInfoThres;
    SessionSrcType sessionSrcType = SESSION_SRC_UNKNOWN;
    sql::SqlSearchInfo searchInfo;
    multi_call::GigStreamRpcInfoMap rpcInfoMap;
};

class SqlAccessLog {
public:
    SqlAccessLog();
    SqlAccessLog(const std::string &clientIp);

    ~SqlAccessLog();

public:
    void setIp(const std::string &ip) {
        _info.ip = ip;
    }
    void setStatusCode(const ErrorCode &statusCode) {
        _info.statusCode = statusCode;
    }
    const ErrorCode &getStatusCode() {
        return _info.statusCode;
    }
    // millisecond
    void setProcessTime(int64_t processTime) {
        _info.processTime = processTime;
    }
    int64_t getProcessTime() const {
        return _info.processTime;
    }
    void setQueryString(const std::string &queryString) {
        _info.queryString = queryString;
    }
    const std::string &getQueryString() const {
        return _info.queryString;
    }
    void setRowCount(uint32_t rowCount) {
        _info.rowCount = rowCount;
    }
    uint32_t getRowCount() const {
        return _info.rowCount;
    }
    void setResultLen(uint32_t resultLen) {
        _info.resultLen = resultLen;
    }
    uint32_t getResultLen() const {
        return _info.resultLen;
    }
    void setLogSearchInfoThres(size_t thres) {
        _info.logSearchInfoThres = thres;
    }
    void setFormatResultTime(int64_t formatTime) {
        _info.formatTime = formatTime;
    }
    void setSearchInfo(sql::SqlSearchInfo searchInfo, bool clearNaviPerf) {
        _info.searchInfo = std::move(searchInfo);
        if (clearNaviPerf) {
            _info.searchInfo.clear_naviperfinfos();
        }
    }
    const sql::SqlSearchInfo &getSearchInfo() const {
        return _info.searchInfo;
    }

    void setTraceId(const std::string &traceId) {
        _info.traceId = traceId;
    }
    const std::string &getTraceId() const {
        return _info.traceId;
    }
    void setSessionSrcType(SessionSrcType type) {
        _info.sessionSrcType = type;
    }
    SessionSrcType getSessionSrcTYPE() const {
        return _info.sessionSrcType;
    }
    void setUserData(const std::string &userData) {
        _info.userData = userData;
    }
    const std::string &getUserData() const {
        return _info.userData;
    }
    void setRpcInfoMap(multi_call::GigStreamRpcInfoMap rpcInfoMap);
    const multi_call::GigStreamRpcInfoMap &getRpcInfoMap() const;
    std::map<std::string, bool> getLeaderInfos() const;
    std::map<std::string, int64_t> getBuildWatermarks() const;

    SqlAccessInfo getSqlAccessInfo() const {
        return _info;
    }

private:
    void log();

private:
    SqlAccessInfo _info;
private:
    friend class SqlAccessLogTest;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SqlAccessLog> SqlAccessLogPtr;
} // namespace sql
} // namespace isearch
