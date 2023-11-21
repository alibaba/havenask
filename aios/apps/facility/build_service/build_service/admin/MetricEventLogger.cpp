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
#include "build_service/admin/MetricEventLogger.h"

#include <algorithm>
#include <cstddef>
#include <ext/alloc_traits.h>
#include <memory>

#include "autil/TimeUtility.h"

using namespace std;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, MetricEventLogger);

MetricEventLogger::MetricEventLogger(int64_t obsoletTime) : _obsoleteTs(obsoletTime)
{
    _lastCleanTs = autil::TimeUtility::currentTimeInSeconds();
}

void MetricEventLogger::update(int64_t value, const kmonitor::MetricsTags& tags)
{
    int64_t ts = autil::TimeUtility::currentTimeInSeconds();
    cleanObsoleteLog(ts);

    autil::ScopedLock lock(_lock);
    auto key = tags.ToString();
    auto iter = _idxMap.find(key);
    if (iter != _idxMap.end()) {
        _dataVec[iter->second] = make_pair(ts, value);
    } else {
        _idxMap[key] = _dataVec.size();
        _dataVec.emplace_back(make_pair(ts, value));
    }
}

void MetricEventLogger::clear()
{
    autil::ScopedLock lock(_lock);
    std::unordered_map<std::string, size_t> newMap;
    std::vector<std::pair<int64_t, int64_t>> newData;
    _idxMap.swap(newMap);
    _dataVec.swap(newData);
}

void MetricEventLogger::fillLogInfo(std::vector<std::pair<std::string, int64_t>>& values) const
{
    auto currentTs = autil::TimeUtility::currentTimeInSeconds();
    autil::ScopedLock lock(_lock);
    for (auto iter = _idxMap.begin(); iter != _idxMap.end(); iter++) {
        size_t idx = iter->second;
        if (_dataVec[idx].first + _obsoleteTs < currentTs) {
            continue;
        }
        values.emplace_back(make_pair(iter->first, _dataVec[idx].second));
    }
}

void MetricEventLogger::cleanObsoleteLog(int64_t currentTs)
{
    if (currentTs < _lastCleanTs + _obsoleteTs) {
        return;
    }
    autil::ScopedLock lock(_lock);
    std::unordered_map<std::string, size_t> newMap;
    std::vector<std::pair<int64_t, int64_t>> newData;
    for (auto iter = _idxMap.begin(); iter != _idxMap.end(); iter++) {
        size_t idx = iter->second;
        if (_dataVec[idx].first + _obsoleteTs < currentTs) {
            continue;
        }
        newMap[iter->first] = newData.size();
        newData.push_back(_dataVec[idx]);
    }
    _idxMap.swap(newMap);
    _dataVec.swap(newData);
    _lastCleanTs = currentTs;
}

}} // namespace build_service::admin
