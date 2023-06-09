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
#include "ha3/sql/framework/SqlAccessLogFormatHelper.h"
#include "ha3/sql/framework/SqlAccessLog.h"
#include "navi/engine/NaviResult.h"


using namespace std;

namespace isearch {
namespace sql {

SqlAccessLogFormatHelper::SqlAccessLogFormatHelper(const SqlAccessLog &accessLog)
    : _accessLog(accessLog) {
    _hasSoftFailure = parseSoftFailure(_accessLog);
    _coveredPercent = parseCoveredPercent(_accessLog.getRpcInfoMap());
}

SqlAccessLogFormatHelper::~SqlAccessLogFormatHelper() {}

bool SqlAccessLogFormatHelper::parseSoftFailure(const SqlAccessLog &accessLog) {
    // * lack searcher result
    if (parseCoveredPercent(accessLog.getRpcInfoMap()) < 1.0 - 1e-5) {
        return true;
    }
    
    const auto &searchInfo = accessLog.getSearchInfo();
    // * scan op has degraded docs
    for (auto &scanInfo: searchInfo.scaninfos()) {
        if (scanInfo.degradeddocscount() > 0) {
            return true;
        }
    }
    return false;
}

double SqlAccessLogFormatHelper::parseCoveredPercent(const multi_call::GigStreamRpcInfoMap &rpcInfoMap) {
    size_t usedRpcCount, lackRpcCount;
    calcStreamRpcConverage(rpcInfoMap, usedRpcCount, lackRpcCount);
    if (usedRpcCount != 0) {
        return 1.0 * (usedRpcCount - lackRpcCount) / usedRpcCount;
    } else {
        return 1.0;
    }
}

} // namespace sql
} // namespace isearch
