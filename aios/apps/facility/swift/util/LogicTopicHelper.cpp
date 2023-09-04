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
#include "swift/util/LogicTopicHelper.h"

#include <cstddef>

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"

namespace swift {
namespace util {
AUTIL_LOG_SETUP(swift, LogicTopicHelper);
using namespace autil;
using namespace std;

string LogicTopicHelper::genPhysicTopicName(const string &logicName, int64_t timestamp, uint32_t partCnt) {
    return StringUtil::formatString("%s-%ld-%d", logicName.c_str(), timestamp, partCnt);
}

bool LogicTopicHelper::parsePhysicTopicName(
    const string &physicName, string &logicName, int64_t &timestamp, uint32_t &partCnt, bool needLog) {
    size_t partSep = physicName.find_last_of('-');
    if (partSep == string::npos) {
        if (needLog) {
            AUTIL_LOG(ERROR, "[%s] miss sep[-]", physicName.c_str());
        }
        return false;
    }
    const string &partCntStr = physicName.substr(partSep + 1, physicName.size());
    if (!StringUtil::strToUInt32(partCntStr.c_str(), partCnt) || 0 == partCnt) {
        if (needLog) {
            AUTIL_LOG(ERROR, "[%s] parse part count fail", physicName.c_str());
        }
        return false;
    }

    size_t timeSep = physicName.find_last_of('-', partSep - 1);
    if (timeSep == string::npos || timeSep <= 0) {
        if (needLog) {
            AUTIL_LOG(ERROR, "[%s] miss one sep[-]", physicName.c_str());
        }
        return false;
    }
    const string &timestampStr = physicName.substr(timeSep + 1, partSep - timeSep - 1);
    if (!StringUtil::strToInt64(timestampStr.c_str(), timestamp)) {
        if (needLog) {
            AUTIL_LOG(ERROR, "[%s] parse timestamp fail", physicName.c_str());
        }
        return false;
    }
    logicName = physicName.substr(0, timeSep);
    return true;
}

bool LogicTopicHelper::getLogicTopicName(const string &physicName, string &logicName, bool needLog) {
    // logicName-timeStamp-partCnt
    int64_t timestamp;
    uint32_t partCnt;
    return parsePhysicTopicName(physicName, logicName, timestamp, partCnt, needLog);
}

bool LogicTopicHelper::isValidPhysicTopicName(const string &physicTopicName) {
    string logicName;
    int64_t timestamp;
    uint32_t partCnt;
    return parsePhysicTopicName(physicTopicName, logicName, timestamp, partCnt);
}

bool LogicTopicHelper::getPhysicPartCnt(const string &physicName, uint32_t &partCnt, bool needLog) {
    // logicName-timeStamp-partCnt
    string logicName;
    int64_t timestamp;

    return parsePhysicTopicName(physicName, logicName, timestamp, partCnt, needLog);
}

int64_t LogicTopicHelper::getPhysicTopicExpiredTime(int64_t physicNameTsUs,
                                                    int64_t obsoleteFileTimeIntervalHour,
                                                    int64_t brokerCfgTTlSec) {
    int64_t ttlExpireTimeSec = obsoleteFileTimeIntervalHour > 0 ? obsoleteFileTimeIntervalHour * 3600 : brokerCfgTTlSec;
    ttlExpireTimeSec += physicNameTsUs / 1000000;
    return (ttlExpireTimeSec < 0) ? -1 : ttlExpireTimeSec;
}

} // namespace util
} // namespace swift
