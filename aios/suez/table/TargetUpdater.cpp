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
#include "suez/table/TargetUpdater.h"

#include "autil/Log.h"
#include "suez/table/LeaderElectionManager.h"
#include "suez/table/RollbackVersionSelector.h"
#include "suez/table/VersionManager.h"
#include "suez/table/VersionSynchronizer.h"

namespace suez {
AUTIL_DECLARE_AND_SETUP_LOGGER(suez, TargetUpdater);

TargetUpdater::TargetUpdater(LeaderElectionManager *leaderMgr,
                             VersionManager *versionMgr,
                             VersionSynchronizer *versionSync)
    : _leaderMgr(leaderMgr), _versionMgr(versionMgr), _versionSync(versionSync) {}

UPDATE_RESULT TargetUpdater::maybeUpdate(HeartbeatTarget &target) {
    for (auto &it : target.getTableMetas()) {
        const auto &pid = it.first;
        auto &meta = it.second;
        if (meta.getTableMode() != TM_READ_WRITE) {
            continue;
        }
        maybeUpdateRoleType(pid, meta);
        auto versionUpdateResult = maybeUpdateIncVersion(pid, meta);
        if (versionUpdateResult != UR_REACH_TARGET) {
            return versionUpdateResult;
        }
    }
    return UR_REACH_TARGET;
}

void TargetUpdater::maybeUpdateRoleType(const PartitionId &pid, PartitionMeta &meta) {
    auto roleTypeBefore = meta.getRoleType();
    meta.setRoleType(_leaderMgr->getRoleType(pid));
    if (roleTypeBefore != meta.getRoleType()) {
        AUTIL_LOG(DEBUG,
                  "partition[%s]: role type changed from %s to %s",
                  FastToJsonString(pid, true).c_str(),
                  RoleType_Name(roleTypeBefore),
                  RoleType_Name(meta.getRoleType()));
    }
}

UPDATE_RESULT TargetUpdater::maybeUpdateIncVersion(const PartitionId &pid, PartitionMeta &meta) {
    TableVersion version;
    if (!_versionSync->supportSyncFromPersist(meta)) {
        return UR_REACH_TARGET;
    }
    if (!_versionMgr->getLeaderVersion(pid, version)) {
        bool ret = _versionSync->syncFromPersist(pid, meta.getConfigPath(), version);
        if (ret) {
            _versionMgr->updateLeaderVersion(pid, version);
            if (version.getVersionId() >= 0) {
                AUTIL_LOG(INFO,
                          "[%s]: force sync leader version: %s",
                          FastToJsonString(pid, true).c_str(),
                          FastToJsonString(version, true).c_str());
            }
        } else {
            AUTIL_LOG(WARN, "[%s]: sync leader version failed", FastToJsonString(pid, true).c_str());
            return UR_NEED_RETRY;
        }
    }
    if (version.getBranchId() != meta.getBranchId()) {
        // on rollback, update head version
        std::vector<TableVersion> versionList;
        if (!_versionSync->getVersionList(pid, versionList)) {
            AUTIL_LOG(ERROR, "[%s]: recover version list  failed", FastToJsonString(pid, true).c_str());
            return UR_NEED_RETRY;
        } else {
            if (!versionList.empty()) {
                _versionMgr->updateHeadVersion(pid, versionList.back());
            }
        }
        RollbackVersionSelector versionSelector(_versionSync, meta.getRawIndexRoot(), pid);
        auto incVersion = versionSelector.select(meta.getRollbackTimestamp(), meta.getIncVersion());
        if (incVersion != indexlibv2::INVALID_VERSIONID) {
            meta.setIncVersion(incVersion);
            return UR_REACH_TARGET;
        } else {
            return UR_NEED_RETRY;
        }
    }
    if (version.getVersionId() > meta.getIncVersion()) {
        meta.setIncVersion(version.getVersionId());
        AUTIL_LOG(DEBUG,
                  "partition[%s]: version updated to %s by state",
                  FastToJsonString(pid, true).c_str(),
                  version.toString().c_str());
    }
    return UR_REACH_TARGET;
}

} // namespace suez
