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

#include "autil/legacy/jsonizable.h"
#include "suez/common/TableMeta.h"
#include "suez/sdk/PartitionId.h"
#include "suez/table/TableVersion.h"

namespace suez {

struct VersionSynchronizerConfig : autil::legacy::Jsonizable {
    bool initFromEnv();
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;
    bool check() const;

private:
    void constructVersionRoot(const std::string &zkRoot);

public:
    std::string zkRoot;
    int64_t zkTimeoutInMs = DEFAULT_ZK_TIMEOUT_IN_MS;
    int64_t syncIntervalInSec = DEFAULT_SYNC_INTERVAL_IN_SEC;
    std::string syncFromBsAdmin;

public:
    static constexpr int64_t DEFAULT_ZK_TIMEOUT_IN_MS = 700;
    static constexpr int64_t DEFAULT_SYNC_INTERVAL_IN_SEC = 2;
};

class VersionSynchronizer {
public:
    virtual ~VersionSynchronizer();

public:
    virtual bool init() = 0;
    virtual void remove(const PartitionId &pid) = 0;
    // if not support sync from psersist, inc version target will be controller by suez admin
    virtual bool supportSyncFromPersist(const PartitionMeta &target) const = 0;
    // called by follower: sync version from leader
    virtual bool syncFromPersist(const PartitionId &pid,
                                 const std::string &appName,
                                 const std::string &dataTable,
                                 const std::string &remoteConfigPath,
                                 TableVersion &version) = 0;
    // called by leader: persist version to zookeeper or something else
    virtual bool persistVersion(const PartitionId &pid,
                                const std::string &appName,
                                const std::string &dataTable,
                                const std::string &remoteConfigPath,
                                const TableVersion &version) = 0;
    virtual bool getVersionList(const PartitionId &pid,
                                const std::string &appName,
                                const std::string &dataTable,
                                std::vector<TableVersion> &versions) = 0;
    virtual bool updateVersionList(const PartitionId &pid,
                                   const std::string &appName,
                                   const std::string &dataTable,
                                   const std::vector<TableVersion> &versions) = 0;

public:
    static VersionSynchronizer *create();
};

} // namespace suez
