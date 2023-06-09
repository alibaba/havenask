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
#include "ha3/common/AccessLog.h" // IWYU pragma: keep

#include <iosfwd>

#include "alog/Logger.h"

#include "ha3/common/ErrorDefine.h"
#include "ha3/common/searchinfo/PhaseOneSearchInfo.h"
#include "autil/Log.h"

using namespace std;
namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, AccessLog);

AccessLog::AccessLog():_ip("unknownip") {
    _processTime = 0;
    _statusCode = ERROR_NONE;
    _totalHits = 0;
    _rowKey = 0;
}

AccessLog::AccessLog(const std::string& clientIp)
    : _ip(clientIp)
{
    _processTime = 0;
    _statusCode = ERROR_NONE;
    _totalHits = 0;
    _rowKey = 0;
}

AccessLog::~AccessLog() {
    log();
}

void AccessLog::setPhaseOneSearchInfo(const PhaseOneSearchInfo &info) {
    _searchInfo = info;
}

void AccessLog::log() {
    AUTIL_LOG(INFO, "%s %4u %lu %4d %ldms %s eagle=[%d %s %s] query=[%s]",
            _ip.c_str(), _totalHits, _rowKey, _statusCode,
            _processTime, _searchInfo.toString().c_str(),
            _sessionSrcType,
            _traceId.empty() ? "0" : _traceId.c_str(),
            _userData.empty() ? "0" : _userData.c_str(),
            _queryString.c_str());
}


} // namespace common
} // namespace isearch
