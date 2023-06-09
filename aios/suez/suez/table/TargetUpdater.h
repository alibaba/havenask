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
#pragma once

#include "suez/heartbeat/HeartbeatTarget.h"
#include "suez/sdk/PartitionId.h"
#include "suez/table/TableMeta.h"

namespace suez {

class LeaderElectionManager;
class VersionManager;
class VersionSynchronizer;

class TargetUpdater {
public:
    TargetUpdater(LeaderElectionManager *leaderMgr, VersionManager *versionMgr, VersionSynchronizer *versionSync);

public:
    UPDATE_RESULT maybeUpdate(HeartbeatTarget &target);

private:
    void maybeUpdateRoleType(const PartitionId &pid, PartitionMeta &meta);
    UPDATE_RESULT maybeUpdateIncVersion(const PartitionId &pid, PartitionMeta &meta);

private:
    LeaderElectionManager *_leaderMgr;
    VersionManager *_versionMgr;
    VersionSynchronizer *_versionSync;
};

} // namespace suez
