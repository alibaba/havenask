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
#include "sql/framework/SqlAccessLog.h"

#include <cstdint>
#include <iosfwd>

#include "autil/StringUtil.h"
#include "multi_call/stream/GigStreamRpcInfo.h"

using namespace std;
namespace sql {
AUTIL_LOG_SETUP(sql, SqlAccessLog);

SqlAccessLog::SqlAccessLog()
    : _info("unknownip") {}

SqlAccessLog::SqlAccessLog(const std::string &clientIp)
    : _info(clientIp) {}

SqlAccessLog::~SqlAccessLog() {
    log();
}

void SqlAccessLog::setRpcInfoMap(multi_call::GigStreamRpcInfoMap rpcInfoMap) {
    _info.rpcInfoMap = std::move(rpcInfoMap);
}

const multi_call::GigStreamRpcInfoMap &SqlAccessLog::getRpcInfoMap() const {
    return _info.rpcInfoMap;
}

map<string, int64_t> SqlAccessLog::getBuildWatermarks() const {
    map<string, int64_t> buildWatermarks;
    for (const auto &info : _info.searchInfo.tablemodifyinfos()) {
        if (info.maxcheckpoint() > 0) {
            buildWatermarks[info.tablename()] = info.maxcheckpoint();
        }
    }
    return buildWatermarks;
}

void SqlAccessLog::log() {
    _info.searchInfo.clear_naviperfinfos();
    if (_info.processTime >= (int64_t)_info.logSearchInfoThres) {
        AUTIL_LOG(INFO,
                  "%s \t %4u \t %4d \t %u \t %ldus \t %ldus \t %s \t %4d \t query=[%s] \t "
                  "run_info=[%s] \t rpc_info=[%s] \t %s",
                  _info.ip.c_str(),
                  _info.rowCount,
                  _info.statusCode,
                  _info.resultLen,
                  _info.processTime,
                  _info.formatTime,
                  _info.traceId.empty() ? "0" : _info.traceId.c_str(),
                  _info.sessionSrcType,
                  _info.queryString.c_str(),
                  _info.searchInfo.ShortDebugString().c_str(),
                  autil::StringUtil::toString(_info.rpcInfoMap).c_str(),
                  _info.loggerId.c_str());
    } else {
        AUTIL_LOG(INFO,
                  "%s \t %4u \t %4d \t %u \t %ldus \t %ldus \t %s \t %4d \t  query=[%s] \t %s",
                  _info.ip.c_str(),
                  _info.rowCount,
                  _info.statusCode,
                  _info.resultLen,
                  _info.processTime,
                  _info.formatTime,
                  _info.traceId.empty() ? "0" : _info.traceId.c_str(),
                  _info.sessionSrcType,
                  _info.queryString.c_str(),
                  _info.loggerId.c_str());
    }
}

} // namespace sql
