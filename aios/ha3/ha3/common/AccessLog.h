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

#include <stdint.h>
#include <memory>
#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/ErrorDefine.h"
#include "ha3/common/searchinfo/PhaseOneSearchInfo.h"
#include "ha3/isearch.h"

namespace isearch {
namespace common {

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
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<AccessLog> AccessLogPtr;

} // namespace common
} // namespace isearch
