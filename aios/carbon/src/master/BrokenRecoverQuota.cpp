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
#include "master/BrokenRecoverQuota.h"
#include "autil/TimeUtility.h"

using namespace std;
using namespace autil;
BEGIN_CARBON_NAMESPACE(master);

CARBON_LOG_SETUP(master, BrokenRecoverQuota);

BrokenRecoverQuota::BrokenRecoverQuota() {
}

BrokenRecoverQuota::~BrokenRecoverQuota() {
}

void BrokenRecoverQuota::updateConfig(const BrokenRecoverQuotaConfig &config) {
    CARBON_LOG(DEBUG, "update broken recover quota config:[%s].",
               autil::legacy::ToJsonString(config, true).c_str());
    _config = config;
}

bool BrokenRecoverQuota::require(int64_t curTime) {
    CARBON_LOG(DEBUG, "require quota to recover broken node. "
               "cur broken count:%zd, maxFailedCount:%d, timeWindow:%ld.",
               _brokenCounterQueue.size(), _config.maxFailedCount,
               _config.timeWindow);
    int64_t currentTime = curTime;
    if (currentTime == -1) {
        currentTime = TimeUtility::currentTime();
    }

    while (!_brokenCounterQueue.empty()) {
        int64_t latest = _brokenCounterQueue.front();
        if (latest <= currentTime - _config.timeWindow * 1000000) {
            _brokenCounterQueue.pop_front();
        } else {
            break;
        }
    }

    if (_config.maxFailedCount < 0) {
        return true;
    }
    
    if (_brokenCounterQueue.size() >= (size_t)_config.maxFailedCount) {
        CARBON_LOG(WARN, "broken node count exceed max failed "
                   "count [%d] in time window [%ld]",
                   _config.maxFailedCount, _config.timeWindow);
        return false;
    }

    _brokenCounterQueue.push_back(currentTime);
    return true;
}

END_CARBON_NAMESPACE(master);

