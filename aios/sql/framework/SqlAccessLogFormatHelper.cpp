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
#include "sql/framework/SqlAccessLogFormatHelper.h"

#include <cstddef>

#include "multi_call/stream/GigStreamRpcInfo.h"
#include "sql/framework/SqlAccessLog.h"
#include "sql/proto/SqlSearchInfo.pb.h"

using namespace std;

namespace sql {

SqlAccessLogFormatHelper::SqlAccessLogFormatHelper(const SqlAccessLog &accessLog)
    : _accessLog(accessLog) {
    _hasSoftFailure = parseSoftFailure(_accessLog);
    _coveredPercent = parseCoveredPercent(_accessLog.getRpcInfoMap());
    _softFailureCodes = parseSoftFailureCodes(_accessLog);
}

SqlAccessLogFormatHelper::~SqlAccessLogFormatHelper() {}

bool SqlAccessLogFormatHelper::parseSoftFailure(const SqlAccessLog &accessLog) {
    // * lack searcher result
    if (parseCoveredPercent(accessLog.getRpcInfoMap()) < 1.0 - 1e-5) {
        return true;
    }

    const auto &searchInfo = accessLog.getSearchInfo();
    for (auto &degradedInfo : searchInfo.degradedinfos()) {
        if (degradedInfo.degradederrorcodes_size() > 0) {
            return true;
        }
    }
    return false;
}

double
SqlAccessLogFormatHelper::parseCoveredPercent(const multi_call::GigStreamRpcInfoMap &rpcInfoMap) {
    size_t usedRpcCount, lackRpcCount;
    calcStreamRpcConverage(rpcInfoMap, usedRpcCount, lackRpcCount);
    if (usedRpcCount != 0) {
        return 1.0 * (usedRpcCount - lackRpcCount) / usedRpcCount;
    } else {
        return 1.0;
    }
}

vector<int64_t> SqlAccessLogFormatHelper::parseSoftFailureCodes(const SqlAccessLog &accessLog) {
    vector<int64_t> softFailureCodes;
    const auto &searchInfo = accessLog.getSearchInfo();
    for (auto &degradedInfo : searchInfo.degradedinfos()) {
        for (auto &errorCode : degradedInfo.degradederrorcodes()) {
            softFailureCodes.emplace_back(errorCode);
        }
    }
    if (parseCoveredPercent(accessLog.getRpcInfoMap()) < 1.0 - 1e-5) {
        softFailureCodes.emplace_back(LACK_PART_ERROR);
    }
    return softFailureCodes;
}
} // namespace sql
