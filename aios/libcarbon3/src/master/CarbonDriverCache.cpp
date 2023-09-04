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
#include "master/CarbonDriverCache.h"
#include "master/ProxyClient.h"
#include "monitor/MonitorUtil.h"

BEGIN_CARBON_NAMESPACE(master);
USE_CARBON_NAMESPACE(monitor);
using namespace autil;
using namespace autil::legacy;

CARBON_LOG_SETUP(master, CarbonDriverCache);

CarbonDriverCache::CarbonDriverCache(ProxyClientPtr proxy) : _proxy(proxy), _lastUpdateTm(0) {
    assert(_proxy);
}

CarbonDriverCache::~CarbonDriverCache() {
    stop();
}

bool CarbonDriverCache::start() {
    _threadPtr = LoopThread::createLoopThread(std::bind(&CarbonDriverCache::runOnce, this), 3 * 1000 * 1000); // 3s
    if (!_threadPtr) {
        CARBON_LOG(ERROR, "create driver cache thread error");
        return false;
    }
    CARBON_LOG(INFO, "start carbon driver cache");
    return true;
}

void CarbonDriverCache::stop() {
    _threadPtr.reset();
}

void CarbonDriverCache::updateGroupPlans(const GroupPlanMap& plans, ErrorInfo *errorInfo) {
    ScopedLock lock(_targetLock);
    _plans = plans;
    _lastUpdateTm = autil::TimeUtility::currentTime();
    CARBON_LOG(DEBUG, "update GroupPlans (%lu groups)", _plans.size());
}

// NOTE: same behaviour as carbon2. The status here maybe not ready yet, and app will get an empty status.
void CarbonDriverCache::getGroupStatus(const std::vector<std::string> &groupids, std::vector<GroupStatus> *allGroupStatus) const {
    ScopedReadLock lock(_statusLock);
    if (groupids.empty()) {
        for (const auto& kv : _statusCache) {
            allGroupStatus->push_back(kv.second);
        }
    } else {
        for (const auto& id : groupids) {
            const auto& it = _statusCache.find(id);
            if (it != _statusCache.end()) {
                allGroupStatus->push_back(it->second);
            } else {
                CARBON_LOG(WARN, "not found group status for [%s]", id.c_str());
            }
        }
    }
}

void CarbonDriverCache::runOnce() {
    REPORT_USED_TIME(METRIC_DRIVER_CACHE_LATENCY, TagMap());
    loadStatus();
    commitPlans();
}

void CarbonDriverCache::commitPlans() {
    ErrorInfo errorInfo;
    GroupPlanMap plans;
    int64_t lastUpdateTm = 0;
    {
        ScopedLock lock(_targetLock);
        plans = _plans;
        lastUpdateTm = _lastUpdateTm;
    }
    if (lastUpdateTm <= 0) { // no plans set until now.
        return;
    }
    std::string planStr = FastToJsonString(plans, true);
    if (_lastCommitStr == planStr) {
        CARBON_LOG(DEBUG, "skip to commit plans because commit hash not changed, hash len(%lu)", _lastCommitStr.size());
        return;
    }
    CARBON_LOG(DEBUG, "call proxy to setGroups (%lu groups)", plans.size());
    if (!_proxy->setGroups(plans, &errorInfo)) {
        CARBON_LOG(WARN, "request proxy setGroups error: %d, %s", errorInfo.errorCode, errorInfo.errorMsg.c_str());
        // better to clear the hash. Otherwise there maybe a bug:
        // setPlan v1 -> success (hash=v1); 
        // setPlan v2 -> failed (but some resources are taken effect on k8s), and hash=v1);
        // setPlan v1 -> ignore because hash==v1, which is not expected!
        _lastCommitStr.clear();
        return;
    }
    _lastCommitStr.swap(planStr);
}

bool CarbonDriverCache::loadStatus() {
    std::vector<GroupStatus> statusList;
    if (!_proxy->getGroupStatusList(std::vector<groupid_t>(), statusList)) {
        CARBON_LOG(WARN, "request proxy getGroupStatus error");
        return false;
    }
    std::map<groupid_t, GroupStatus> statusMap;
    for (const auto& status : statusList) {
        statusMap[status.groupId] = status;
    }
    ScopedWriteLock lock(_statusLock);
    _statusCache.swap(statusMap);
    return true;
}

END_CARBON_NAMESPACE(master);
