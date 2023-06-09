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
#include "fslib/fs/SpeedController.h"

#include <autil/StringUtil.h>
#include <autil/Regex.h>
#include "fslib/util/PathUtil.h"

using namespace std;
using namespace autil;

FSLIB_BEGIN_NAMESPACE(fs);
AUTIL_DECLARE_AND_SETUP_LOGGER(fs, SpeedController);

SpeedController::SpeedController() 
    : _inited(false)
    , _quotaSizeInByte(std::numeric_limits<uint32_t>::max())
    , _quotaUsageRatio(1.0)
    , _interval(1)
    , _baseLineTs(0)
    , _remainQuota(std::numeric_limits<uint32_t>::max())
    , _totalWaitCount(0)
{
}

SpeedController::~SpeedController() { 
}

void SpeedController::initFromString(const string& paramStr)
{
    ScopedLock lock(_lock);
    if (_inited) {
        AUTIL_LOG(INFO, "SpeedController has already been inited!");
        return;
    }

    AUTIL_LOG(INFO, "Init SpeedController with param [%s]!", paramStr.c_str());
    vector<vector<string>> patternStrVec;
    StringUtil::fromString(paramStr, patternStrVec, "=", ",");
    if (patternStrVec.size() == 0) {
        return;
    }
    for (size_t j = 0; j < patternStrVec.size(); j++) {
        if (patternStrVec[j].size() != 2) {
            AUTIL_LOG(ERROR,
                      "fslib speed controller [%s] has format error:"
                      "inner should be kv pair format!",
                      paramStr.c_str());
            return;
        }

        if (patternStrVec[j][0] == "quota_size_in_byte") // Byte
        {
            uint32_t quotaSize;
            if (StringUtil::strToUInt32(patternStrVec[j][1].c_str(), quotaSize) && quotaSize > 0) {
                _quotaSizeInByte = quotaSize;
                continue;
            }
            AUTIL_LOG(ERROR, "quota_size_in_bytes [%s] must be valid number!", patternStrVec[j][1].c_str());
            return;
        }

        if (patternStrVec[j][0] == "quota_interval_in_ms") // ms
        {
            uint32_t quotaInterval;
            if (StringUtil::strToUInt32(patternStrVec[j][1].c_str(), quotaInterval)) {
                _interval = quotaInterval * 1000;
                continue;
            }
            AUTIL_LOG(ERROR, "quota_interval [%s] must be valid [ms]!", patternStrVec[j][1].c_str());
            return;
        }

        if (patternStrVec[j][0] == "path_pattern") {
            _pathPattern = util::PathUtil::normalizePath(patternStrVec[j][1]);
            continue;
        }
        AUTIL_LOG(ERROR, "unknown key [%s] in fslib speed limit args [%s]!", patternStrVec[j][0].c_str(),
                  paramStr.c_str());
        return;
    }
    _inited = true;
}

bool SpeedController::matchPathPattern(const std::string& path) const
{
    if (_pathPattern.empty()) {
        return true;
    }
    string noramlizedPath = util::PathUtil::normalizePath(path);
    return autil::Regex::match(path, _pathPattern);
}

int64_t SpeedController::reserveQuota(int64_t quota)
{
    assert(quota > 0);
    if (!_inited) {
        return quota;
    }

    ScopedLock lock(_lock);
    int64_t currentTs = TimeUtility::currentTime();
    if (currentTs >= _baseLineTs + _interval) {
        // new window
        _baseLineTs = currentTs;
        int64_t leftQuota = _remainQuota;
        _remainQuota = _quotaSizeInByte;

        if (leftQuota < _remainQuota) {
            _quotaUsageRatio = (double)(_remainQuota - leftQuota) / _remainQuota;
        } else {
            _quotaUsageRatio = 1.0;
        }
    }

    if (_remainQuota <= 0) {
        // no quota in current window, wait to next
        int64_t sleepTs = _baseLineTs + _interval - currentTs;
        usleep(sleepTs);
        currentTs = TimeUtility::currentTime();
        _baseLineTs = currentTs;
        _remainQuota = _quotaSizeInByte;
        _quotaUsageRatio = 1.0;

        ++_totalWaitCount;
        if (_totalWaitCount % 100 == 0) {
            AUTIL_LOG(INFO, "wait [%lu] times for speed limit!", _totalWaitCount);
        }
    }

    int64_t availableQuota = min(_remainQuota, quota);
    _remainQuota -= availableQuota;
    return availableQuota;
}

int64_t SpeedController::TEST_getInterval() const {
    return _interval;
}

string SpeedController::TEST_getPathPattern() const {
    return _pathPattern;
}

bool SpeedController::TEST_isInit() const {
    return _inited;
}

uint32_t SpeedController::TEST_getQuotaSize() const {
    return _quotaSizeInByte;
}

size_t SpeedController::TEST_getTotalWaitCount() const {
    return _totalWaitCount;
}

int64_t SpeedController::getQuotaPerSecond() const {
    return (int64_t)(_quotaSizeInByte * (double)1000000 / _interval);
}

void SpeedController::resetQuotaPerSecond(int64_t quota) {
    _quotaSizeInByte = (int64_t)(quota * (double)_interval / 1000000);
}

FSLIB_END_NAMESPACE(fs);

