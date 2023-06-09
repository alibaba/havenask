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
#include "suez/table/ZkLeaderElectionManager.h"

#include "aios/apps/facility/cm2/cm_basic/util/zk_wrapper.h"
#include "aios/network/gig/multi_call/rpc/GigRpcServer.h"
#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "fslib/fs/FileSystem.h"
#include "suez/common/GeneratorDef.h"
#include "suez/sdk/PathDefine.h"
#include "suez/table/LeaderElectionStrategy.h"
#include "worker_framework/LeaderElector.h"
#include "worker_framework/PathUtil.h"

using namespace std;
namespace suez {

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, ZkLeaderElectionManager);

LeaderElectorWrapper::LeaderElectorWrapper(std::unique_ptr<cm_basic::ZkWrapper> zk,
                                           std::unique_ptr<LeaderElector> leaderElector)
    : zkWrapper(std::move(zk))
    , impl(std::move(leaderElector))
    , forceFollower(false)
    , forceFollowerExpireTime(-1)
    , forceFollowerTimeInMs(0)
    , becomeLeaderTimestamp(0)
    , hasStarted(false) {}

LeaderElectorWrapper::~LeaderElectorWrapper() {}

bool LeaderElectorWrapper::isLeader() const {
    {
        autil::ScopedLock lock(mutex);
        if (forceFollower) {
            assert(forceFollowerExpireTime != -1);
            int64_t currentTime = autil::TimeUtility::currentTimeInMicroSeconds();
            if (currentTime >= forceFollowerExpireTime) {
                if (impl->start()) {
                    forceFollower = false;
                    forceFollowerExpireTime = -1;
                    AUTIL_LOG(INFO, "start leader elector again");
                }
            }
            return false;
        }
    }
    return impl->isLeader();
}

void LeaderElectorWrapper::releaseLeader() {
    {
        autil::ScopedLock lock(mutex);
        forceFollower = true;
        auto leaseExpireTime =
            std::max(impl->getLeaseExpirationTime(), autil::TimeUtility::currentTimeInMicroSeconds());
        forceFollowerExpireTime = leaseExpireTime + forceFollowerTimeInMs * 1000;
    }
    impl->stop();
}

void LeaderElectorWrapper::registerPartition(const PartitionId &pid, LeaderChangeHandler handler) {
    autil::ScopedLock lock(mutex);
    subscribers[pid] = std::move(handler);
}

void LeaderElectorWrapper::removePartition(const PartitionId &pid) {
    autil::ScopedLock lock(mutex);
    subscribers.erase(pid);
}

size_t LeaderElectorWrapper::getRefCount() const {
    autil::ScopedLock lock(mutex);
    return subscribers.size();
}

bool LeaderElectorWrapper::start() {
    if (!impl) {
        return false;
    }
    impl->setBecomeLeaderHandler(std::bind(&LeaderElectorWrapper::becomeLeader, this));
    impl->setNoLongerLeaderHandler(std::bind(&LeaderElectorWrapper::nolongerLeader, this));
    if (impl->start()) {
        hasStarted = true;
        return true;
    } else {
        return false;
    }
}

void LeaderElectorWrapper::stop() {
    if (impl) {
        impl->stop();
        impl.reset();
    }
}

void LeaderElectorWrapper::becomeLeader() {
    {
        autil::ScopedLock lock(mutex);
        becomeLeaderTimestamp = autil::TimeUtility::currentTime();
    }
    leaderChange(true);
}

void LeaderElectorWrapper::nolongerLeader() { leaderChange(false); }

void LeaderElectorWrapper::leaderChange(bool becomeLeader) {
    std::map<PartitionId, LeaderChangeHandler> snapshot;
    {
        autil::ScopedLock lock(mutex);
        snapshot = subscribers;
    }
    for (const auto &it : snapshot) {
        if (it.second) {
            // TODO: maybe update leader info immediately
            (it.second)(it.first, becomeLeader);
        }
    }
}

int64_t LeaderElectorWrapper::getLeaderTimestamp() const {
    autil::ScopedLock lock(mutex);
    return becomeLeaderTimestamp;
}

//////////////////////////////////////////////////////////////////////////////////////////////////////
ZkLeaderElectionManager::ZkLeaderElectionManager(const LeaderElectionConfig &config)
    : LeaderElectionManager(config)
    , _leaderElectionRoot(PathDefine::getLeaderElectionRoot(config.zkRoot))
    , _leaderInfoRoot(PathDefine::getLeaderInfoRoot(config.zkRoot)) {}

ZkLeaderElectionManager::~ZkLeaderElectionManager() { stop(); }

bool ZkLeaderElectionManager::init() {
    _strategy.reset(LeaderElectionStrategy::create(_config.strategyType));
    return _strategy != nullptr;
}

void ZkLeaderElectionManager::stop() {
    std::map<uint64_t, std::unique_ptr<LeaderElectorWrapper>> leaderElectors;

    {
        autil::ScopedWriteLock lock(_lock);
        leaderElectors = std::move(_leaderElectors);
    }
    for (const auto &it : leaderElectors) {
        it.second->stop();
    }
    leaderElectors.clear();
}

bool ZkLeaderElectionManager::registerPartition(const PartitionId &pid, LeaderChangeHandler handler) {
    autil::ScopedWriteLock lock(_lock);
    _allowLeaderInfos[pid] = AllowLeaderInfo();
    _allowLeaderInfos[pid].campaginStatusReady = _config.campaignNow;
    LeaderElectorWrapper *leaderElector = findLeaderElectorLocked(pid);
    if (leaderElector == nullptr) {
        auto newLeaderElector = createLeaderElectorWrapper(pid);
        if (!newLeaderElector) {
            return false;
        }
        leaderElector = newLeaderElector.get();
        auto key = _strategy->getKey(pid);
        _leaderElectors.emplace(key, std::move(newLeaderElector));
        AUTIL_LOG(INFO, "create leader elector for %s, key is %lu", ToJsonString(pid, true).c_str(), key);
    }
    leaderElector->registerPartition(pid, std::move(handler));
    return true;
}

void ZkLeaderElectionManager::removePartition(const PartitionId &pid) { seal(pid); }

void ZkLeaderElectionManager::seal(const PartitionId &pid) {
    AUTIL_LOG(INFO, "zk leader election manager seal partition %s", ToJsonString(pid, true).c_str());
    autil::ScopedWriteLock lock(_lock);
    _allowLeaderInfos.erase(pid);
    auto it = _leaderElectors.find(_strategy->getKey(pid));
    if (it != _leaderElectors.end()) {
        LeaderElectorWrapper *leaderElector = it->second.get();
        leaderElector->removePartition(pid);
        if (leaderElector->getRefCount() == 0) {
            AUTIL_LOG(INFO, "stop leader elector for %s", ToJsonString(pid, true).c_str());
            leaderElector->stop();
            _leaderElectors.erase(it);
        }
    }
}

int64_t ZkLeaderElectionManager::getLeaderTimestamp(const PartitionId &pid) {
    autil::ScopedWriteLock lock(_lock);
    auto it = _leaderElectors.find(_strategy->getKey(pid));
    if (it != _leaderElectors.end()) {
        return it->second->getLeaderTimestamp();
    } else {
        return 0;
    }
}

void ZkLeaderElectionManager::releaseLeader(const PartitionId &pid) {
    autil::ScopedReadLock lock(_lock);
    LeaderElectorWrapper *leaderElector = findLeaderElectorLocked(pid);
    if (leaderElector != nullptr) {
        leaderElector->releaseLeader();
    }
}

RoleType ZkLeaderElectionManager::getRoleType(const PartitionId &pid) const {
    autil::ScopedReadLock lock(_lock);
    const LeaderElectorWrapper *leaderElector = findLeaderElectorLocked(pid);
    if (leaderElector == nullptr) {
        return RT_FOLLOWER;
    }
    return leaderElector->isLeader() ? RT_LEADER : RT_FOLLOWER;
}

std::unique_ptr<cm_basic::ZkWrapper> ZkLeaderElectionManager::createZkWrapper() const {
    std::string zkHost = worker_framework::PathUtil::getHostFromZkPath(_config.zkRoot);
    if (zkHost.empty()) {
        AUTIL_LOG(ERROR, "invalid zkRoot: [%s]", _config.zkRoot.c_str());
        return nullptr;
    }
    auto zk = std::make_unique<cm_basic::ZkWrapper>(zkHost, _config.zkTimeoutInMs, true);
    if (!zk->open()) {
        return nullptr;
    }
    return zk;
}

std::unique_ptr<LeaderElectorWrapper> ZkLeaderElectionManager::createLeaderElectorWrapper(const PartitionId &pid) {
    auto zk = createZkWrapper();
    if (!zk) {
        AUTIL_LOG(ERROR, "create zk wrapper for %s failed", ToJsonString(pid, true).c_str());
        return nullptr;
    }
    auto rootPath = worker_framework::PathUtil::getPathFromZkPath(_leaderElectionRoot);
    auto leaderPath = PathDefine::join(rootPath, _strategy->getPath(pid));
    auto leaderElector =
        std::make_unique<LeaderElector>(zk.get(), leaderPath, _config.leaseInMs, _config.loopIntervalInMs, true);
    leaderElector->setLeaseInfo(_leaderInfo);
    leaderElector->setForbidCampaignLeaderTimeMs(_config.forceFollowerTimeInMs);
    auto wrapper = std::make_unique<LeaderElectorWrapper>(std::move(zk), std::move(leaderElector));
    wrapper->forceFollowerTimeInMs = _config.forceFollowerTimeInMs;
    if (_config.campaignNow && !wrapper->start()) {
        AUTIL_LOG(WARN, "start leader election for %s failed", ToJsonString(pid, true).c_str());
        return nullptr;
    }
    return wrapper;
}

void ZkLeaderElectionManager::allowCampaginLeader(const PartitionId &pid) {
    autil::ScopedWriteLock lock(_lock);
    if (CLT_BY_INDICATION == _config.campaginLeaderType && !_allowLeaderInfos[pid].allowCampaginLeader) {
        AUTIL_LOG(INFO, "not allow campagin leader pid: [%s]", ToJsonString(pid, true).c_str());
        return;
    }
    _allowLeaderInfos[pid].campaginStatusReady = true;
    LeaderElectorWrapper *wrapper = findLeaderElectorLocked(pid);
    if (wrapper != nullptr && !wrapper->hasStarted) {
        bool canStart = true;
        auto key = _strategy->getKey(pid);
        for (const auto &iter : _allowLeaderInfos) {
            if (key == _strategy->getKey(iter.first)) {
                canStart &= iter.second.campaginStatusReady;
            }
        }
        if (canStart) {
            wrapper->start();
        }
    }
}

pair<bool, int64_t> ZkLeaderElectionManager::getLeaderElectionInfo(const PartitionId &pid) {
    autil::ScopedReadLock lock(_lock);
    const LeaderElectorWrapper *leaderElector = findLeaderElectorLocked(pid);
    bool isLeader = (leaderElector && leaderElector->isLeader());
    if (isLeader) {
        return make_pair(true, leaderElector->getLeaderTimestamp());
    }
    return make_pair(false, 0);
}

LeaderElectorWrapper *ZkLeaderElectionManager::getLeaderElector(const PartitionId &pid) {
    autil::ScopedReadLock lock(_lock);
    return findLeaderElectorLocked(pid);
}

LeaderElectorWrapper *ZkLeaderElectionManager::findLeaderElectorLocked(const PartitionId &pid) {
    auto key = _strategy->getKey(pid);
    auto it = _leaderElectors.find(key);
    if (it == _leaderElectors.end()) {
        return nullptr;
    }
    return it->second.get();
}

const LeaderElectorWrapper *ZkLeaderElectionManager::findLeaderElectorLocked(const PartitionId &pid) const {
    return const_cast<ZkLeaderElectionManager *>(this)->findLeaderElectorLocked(pid);
}

void ZkLeaderElectionManager::setCampaginLeaderIndication(const PartitionId &pid, bool allowCampaginLeader) {
    autil::ScopedReadLock lock(_lock);
    if (_allowLeaderInfos.end() == _allowLeaderInfos.find(pid)) {
        return;
    }
    _allowLeaderInfos[pid].allowCampaginLeader = allowCampaginLeader;
}

} // namespace suez
