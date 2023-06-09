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

#include <map>

#include "autil/Lock.h"
#include "suez/sdk/PartitionId.h"
#include "suez/table/TableVersion.h"

namespace suez {

struct PartitionVersion {
    TableVersion localVersion;     // version produced by myself
    TableVersion leaderVersion;    // version produced by leader
    TableVersion publishedVersion; // version published by leader
    TableVersion headVersion;      // last version in current version list

    bool updateLeaderVersion(const TableVersion &version) {
        if (version.getVersionId() > leaderVersion.getVersionId()) {
            leaderVersion = version;
            return true;
        }
        return false;
    }

    bool updateLocalVersion(const TableVersion &version) {
        if (version.getVersionId() > localVersion.getVersionId()) {
            localVersion = version;
            return true;
        }
        return false;
    }

    bool updatePublishedVersion(const TableVersion &version) {
        if (version.getVersionId() > publishedVersion.getVersionId()) {
            publishedVersion = version;
            return true;
        }
        return false;
    }

    bool updateHeadVersion(const TableVersion &version) {
        headVersion = version;
        return true;
    }
};

class VersionManager {
private:
    using VersionMap = std::map<PartitionId, PartitionVersion>;

public:
    void remove(const PartitionId &pid);
    bool getVersion(const PartitionId &pid, PartitionVersion &version) const;
    bool getLeaderVersion(const PartitionId &pid, TableVersion &version) const;
    bool getLocalVersion(const PartitionId &pid, TableVersion &version) const;
    bool getPublishedVersion(const PartitionId &pid, TableVersion &version) const;
    bool getHeadVersion(const PartitionId &pid, TableVersion &version) const;
    bool updateLeaderVersion(const PartitionId &pid, const TableVersion &version);
    bool updateLocalVersion(const PartitionId &pid, const TableVersion &version);
    bool updatePublishedVersion(const PartitionId &pid, const TableVersion &version);
    bool updateHeadVersion(const PartitionId &pid, const TableVersion &version);

private:
    const PartitionVersion *doGetVersion(const PartitionId &pid) const;

private:
    VersionMap _versionMap;
    mutable autil::ReadWriteLock _lock;
};

} // namespace suez
