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

#include "suez/table/TableVersion.h"
#include "suez/table/Todo.h"

namespace suez {

class LeaderElectionManager;
struct PartitionVersion;
class VersionManager;
class VersionPublisher;
class VersionSynchronizer;

class SyncVersion final : public TodoWithTarget {
public:
    enum SyncEvent
    {
        SE_NONE = 0x0,
        SE_ROLLBACK = 0x1,
        SE_COMMIT = 0x2,
        SE_ALL = SE_ROLLBACK | SE_COMMIT
    };
    SyncVersion(const TablePtr &table,
                const TargetPartitionMeta &target,
                LeaderElectionManager *leaderElectionMgr,
                VersionManager *versionMgr,
                VersionPublisher *versionPublisher,
                VersionSynchronizer *versionSync,
                int event);

public:
    void run() override;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;

public:
    static bool recoverVersion(const PartitionId &pid,
                               const TargetPartitionMeta &target,
                               VersionSynchronizer *versionSync,
                               VersionPublisher *versionPublisher,
                               TableVersion &leaderVersion,
                               TableVersion &publishedVersion);

private:
    void syncCommitVersion();
    bool syncRollbackVersion();
    bool isLeader() const;
    void syncForLeader(PartitionVersion &version);
    void syncForFollower(PartitionVersion &version);
    bool updateVersionList(const TableVersion &newVersion);

private:
    LeaderElectionManager *_leaderElectionMgr;
    VersionManager *_versionMgr;
    VersionPublisher *_versionPublisher;
    VersionSynchronizer *_versionSync;
    int _event;
};

} // namespace suez
