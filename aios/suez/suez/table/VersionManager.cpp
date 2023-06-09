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
#include "suez/table/VersionManager.h"

#include "autil/Log.h"

namespace suez {
AUTIL_DECLARE_AND_SETUP_LOGGER(suez, VersionManager);

void VersionManager::remove(const PartitionId &pid) {
    autil::ScopedWriteLock lock(_lock);
    _versionMap.erase(pid);
}

bool VersionManager::getVersion(const PartitionId &pid, PartitionVersion &version) const {
    autil::ScopedReadLock lock(_lock);
    auto it = _versionMap.find(pid);
    if (it == _versionMap.end()) {
        return false;
    }
    version = it->second;
    return true;
}

bool VersionManager::getLeaderVersion(const PartitionId &pid, TableVersion &version) const {
    autil::ScopedReadLock lock(_lock);
    const PartitionVersion *pversion = doGetVersion(pid);
    if (pversion == nullptr) {
        return false;
    }
    version = pversion->leaderVersion;
    return true;
}

bool VersionManager::getLocalVersion(const PartitionId &pid, TableVersion &version) const {
    autil::ScopedReadLock lock(_lock);
    const PartitionVersion *pversion = doGetVersion(pid);
    if (pversion == nullptr) {
        return false;
    }
    version = pversion->localVersion;
    return true;
}

bool VersionManager::getPublishedVersion(const PartitionId &pid, TableVersion &version) const {
    autil::ScopedReadLock lock(_lock);
    const PartitionVersion *pversion = doGetVersion(pid);
    if (pversion == nullptr) {
        return false;
    }
    version = pversion->publishedVersion;
    return true;
}

bool VersionManager::updateLeaderVersion(const PartitionId &pid, const TableVersion &version) {
    autil::ScopedWriteLock lock(_lock);
    return _versionMap[pid].updateLeaderVersion(version);
}

bool VersionManager::updateLocalVersion(const PartitionId &pid, const TableVersion &version) {
    autil::ScopedWriteLock lock(_lock);
    return _versionMap[pid].updateLocalVersion(version);
}

bool VersionManager::updatePublishedVersion(const PartitionId &pid, const TableVersion &version) {
    autil::ScopedWriteLock lock(_lock);
    return _versionMap[pid].updatePublishedVersion(version);
}

const PartitionVersion *VersionManager::doGetVersion(const PartitionId &pid) const {
    auto it = _versionMap.find(pid);
    if (it == _versionMap.end()) {
        return nullptr;
    }
    return &it->second;
}

bool VersionManager::updateHeadVersion(const PartitionId &pid, const TableVersion &version) {
    autil::ScopedWriteLock lock(_lock);
    return _versionMap[pid].updateHeadVersion(version);
}

bool VersionManager::getHeadVersion(const PartitionId &pid, TableVersion &version) const {
    autil::ScopedReadLock lock(_lock);
    const PartitionVersion *pversion = doGetVersion(pid);
    if (pversion == nullptr) {
        return false;
    }
    version = pversion->headVersion;
    return true;
}

} // namespace suez
