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
#include "aios/network/gig/multi_call/agent/GigStatistic.h"

#include <functional>

#include "aios/network/gig/multi_call/agent/BizStat.h"
#include "aios/network/gig/multi_call/util/FileRecorder.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"

using namespace std;

namespace multi_call {

AUTIL_DECLARE_AND_SETUP_LOGGER(multi_call, GigStatistic);

GigStatistic::GigStatistic(const std::string &logPrefix)
    : _logPrefix(logPrefix)
    , _otherBizStarted(true) {
}

GigStatistic::~GigStatistic() {
    _logThreadPtr.reset();
    {
        autil::ScopedWriteLock lock(_lock);
        _statMap.clear();
    }
    {
        autil::ScopedWriteLock lock(_warmUpStrategyMapLock);
        for (const auto &pair : _warmUpStrategyMap) {
            delete pair.second;
        }
        _warmUpStrategyMap.clear();
    }
}

const std::string &GigStatistic::getLogPrefix() const {
    return _logPrefix;
}

BizStatPtr GigStatistic::newBizStat() {
    return BizStatPtr(new BizStat(FILTER_INIT_LIMIT, LOAD_BALANCE_FILTER_INIT_LIMIT));
}

BizStatPtr GigStatistic::getBizStat(const std::string &bizName, PartIdTy partId) {
    {
        autil::ScopedReadLock lock(_lock);
        auto it = _statMap.find(bizName);
        if (_statMap.end() != it) {
            const auto &partMap = it->second;
            auto partIt = partMap.find(partId);
            if (partMap.end() != partIt) {
                return partIt->second;
            }
        }
    }
    {
        autil::ScopedWriteLock lock(_lock);
        {
            auto it = _statMap.find(bizName);
            if (_statMap.end() != it) {
                const auto &partMap = it->second;
                auto partIt = partMap.find(partId);
                if (partMap.end() != partIt) {
                    return partIt->second;
                }
            }
        }
        auto stat = newBizStat();
        stat->bizName = bizName;
        stat->started = _otherBizStarted;
        _statMap[bizName].insert(make_pair(partId, stat));
        return stat;
    }
}

std::vector<BizStatPtr> GigStatistic::getBizStatVec(const std::string &bizName) const {
    std::vector<BizStatPtr> statVec;
    {
        autil::ScopedReadLock lock(_lock);
        auto it = _statMap.find(bizName);
        if (_statMap.end() != it) {
            const auto &partMap = it->second;
            for (const auto &partPair : partMap) {
                statVec.push_back(partPair.second);
            }
        }
    }
    return statVec;
}

WarmUpStrategy *GigStatistic::getWarmUpStrategy(const std::string &warmUpStrategy) {
    autil::ScopedReadLock lock(_warmUpStrategyMapLock);
    auto it = _warmUpStrategyMap.find(warmUpStrategy);
    if (_warmUpStrategyMap.end() != it) {
        return it->second;
    } else {
        return NULL;
    }
}

void GigStatistic::updateWarmUpStrategy(const std::string &warmUpStrategy, int64_t timeoutInSecond,
                                        int64_t queryCountLimit) {
    autil::ScopedWriteLock lock(_warmUpStrategyMapLock);
    auto it = _warmUpStrategyMap.find(warmUpStrategy);
    if (_warmUpStrategyMap.end() != it) {
        assert(it->second);
        it->second->update(timeoutInSecond, queryCountLimit);
    } else {
        _warmUpStrategyMap[warmUpStrategy] = new WarmUpStrategy(timeoutInSecond, queryCountLimit);
    }
}

void GigStatistic::init() {
    _logThreadPtr = autil::LoopThread::createLoopThread(std::bind(&GigStatistic::logThread, this),
                                                        UPDATE_TIME_INTERVAL, "GigStatLog");
    if (!_logThreadPtr) {
        AUTIL_LOG(WARN, "start agent log thread failed");
    }
}

void GigStatistic::start() {
    updateStartStatAll(true);
}

void GigStatistic::stop() {
    updateStartStatAll(false);
}

bool GigStatistic::stopped() {
    autil::ScopedReadLock lock(_lock);
    for (const auto &stat : _statMap) {
        const auto &partMap = stat.second;
        for (const auto &partPair : partMap) {
            if (partPair.second->started) {
                return false;
            }
        }
    }
    return !_otherBizStarted;
}

void GigStatistic::start(const std::string &bizName, PartIdTy partId) {
    updateStartStat(bizName, partId, true);
}

void GigStatistic::stop(const std::string &bizName, PartIdTy partId) {
    updateStartStat(bizName, partId, false);
}

bool GigStatistic::stopped(const std::string &bizName, PartIdTy partId) {
    autil::ScopedReadLock lock(_lock);
    auto it = _statMap.find(bizName);
    if (_statMap.end() != it) {
        const auto &bizMap = it->second;
        auto partIt = bizMap.find(partId);
        if (bizMap.end() != partIt) {
            return !partIt->second->started;
        }
    }
    return !_otherBizStarted;
}

bool GigStatistic::longTimeNoQuery(int64_t second) {
    std::vector<BizStatPtr> statVec;
    {
        autil::ScopedReadLock lock(_lock);
        for (const auto &p : _statMap) {
            if (GIG_NEW_HEARTBEAT_METHOD_NAME == p.first) {
                continue;
            }
            const auto &partMap = p.second;
            for (const auto &partPair : partMap) {
                statVec.push_back(partPair.second);
            }
        }
    }
    return longTimeNoQuery(statVec, second);
}

bool GigStatistic::longTimeNoQuery(const std::string &bizName, int64_t second) {
    auto statVec = getBizStatVec(bizName);
    return longTimeNoQuery(statVec, second);
}

void GigStatistic::updateStartStatAll(bool started) {
    autil::ScopedWriteLock lock(_lock);
    for (const auto &bizPair : _statMap) {
        const auto &partMap = bizPair.second;
        for (const auto &partPair : partMap) {
            partPair.second->started = started;
            partPair.second->resetFilters();
        }
    }
    _otherBizStarted = started;
}

void GigStatistic::updateStartStat(const std::string &bizName, PartIdTy partId, bool started) {
    auto stat = getBizStat(bizName, partId);
    stat->started = started;
    stat->resetFilters();
}

bool GigStatistic::longTimeNoQuery(const std::vector<BizStatPtr> &statVec, int64_t second) {
    int64_t limit = autil::TimeUtility::currentTimeInMicroSeconds() - second * FACTOR_S_TO_US;
    for (const auto &stat : statVec) {
        if (stat->lastQueryTimestamp >= limit) {
            AUTIL_INTERVAL_LOG(3, INFO, "biz[%s] still has query, time interval[%ld]",
                               stat->bizName.c_str(), second);
            return false;
        }
    }
    return true;
}

void GigStatistic::logThread() {
    BizStatMap statMap;
    {
        autil::ScopedReadLock lock(_lock);
        statMap = _statMap;
    }
    std::unordered_map<std::string, WarmUpStrategy *> warmUpStrategyMap;
    {
        autil::ScopedReadLock lock(_warmUpStrategyMapLock);
        warmUpStrategyMap = _warmUpStrategyMap;
    }
    string newLogStr;
    newLogStr += "bizs:\n";
    for (const auto &pair : statMap) {
        const auto &bizName = pair.first;
        const auto &partMap = pair.second;
        for (const auto &partPair : partMap) {
            newLogStr += "  " + bizName + ", partId " +
                         autil::StringUtil::toString(partPair.first) + ": " +
                         partPair.second->toString();
        }
    }
    newLogStr += "WarmUpStrategys:\n";
    for (const auto &pair : warmUpStrategyMap) {
        newLogStr += "  " + pair.first + ": " +
                     autil::StringUtil::toString(pair.second->_timeout / FACTOR_S_TO_US) + " T " +
                     autil::StringUtil::toString(pair.second->_queryCountLimit) + " C\n";
    }
    newLogStr += "AgentStarted: " + autil::StringUtil::toString(_otherBizStarted) + "\n";
    if (newLogStr != _logStr) {
        FileRecorder::recordSnapshot(newLogStr, LOG_COUNT, "gig_snapshot/agent/" + _logPrefix);
        _logStr = newLogStr;
    }
}

} // namespace multi_call
