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

#include <memory>

#include "autil/Lock.h"
#include "suez/table/VersionSynchronizer.h"
namespace build_service::common {
class RemoteVersionCommitter;
}

namespace suez {

enum BsSyncType
{
    BST_NONE,
    BST_LEADER_ONLY,
    BST_ALL
};

class BsVersionSynchronizer : public VersionSynchronizer {
public:
    BsVersionSynchronizer(const VersionSynchronizerConfig &config);
    virtual ~BsVersionSynchronizer();

public:
    bool init() override;
    void remove(const PartitionId &pid) override;
    bool supportSyncFromPersist(const PartitionMeta &target) const override;
    bool syncFromPersist(const PartitionId &pid, const std::string &configPath, TableVersion &version) override;
    bool
    persistVersion(const PartitionId &pid, const std::string &localConfigPath, const TableVersion &version) override;
    bool getVersionList(const PartitionId &pid, std::vector<TableVersion> &versions) override;
    bool updateVersionList(const PartitionId &pid, const std::vector<TableVersion> &versions) override;

private:
    std::shared_ptr<build_service::common::RemoteVersionCommitter> createCommitter(const PartitionId &pid,
                                                                                   const std::string &configPath) const;
    virtual std::shared_ptr<build_service::common::RemoteVersionCommitter> createCommitter() const;

private:
    BsSyncType _syncFromPersistType;
    mutable autil::ThreadMutex _mutex;
};

} // namespace suez
