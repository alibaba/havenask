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

#include "aios/apps/facility/cm2/cm_basic/util/zk_wrapper.h"
#include "autil/Lock.h"
#include "suez/table/VersionSynchronizer.h"
#include "worker_framework/WorkerState.h"

namespace suez {

using VersionState = worker_framework::WorkerState;

class ZkVersionSynchronizer : public VersionSynchronizer {
public:
    ZkVersionSynchronizer(const VersionSynchronizerConfig &config);

public:
    bool init() override;
    void remove(const PartitionId &pid) override;
    bool supportSyncFromPersist(const PartitionMeta &target) const override;
    bool syncFromPersist(const PartitionId &pid,
                         const std::string &appName,
                         const std::string &dataTable,
                         const std::string &remoteConfigPath,
                         TableVersion &version) override;
    bool persistVersion(const PartitionId &pid,
                        const std::string &appName,
                        const std::string &dataTable,
                        const std::string &remoteConfigPath,
                        const TableVersion &version) override;
    bool getVersionList(const PartitionId &pid,
                        const std::string &appName,
                        const std::string &dataTable,
                        std::vector<TableVersion> &versions) override;
    bool updateVersionList(const PartitionId &pid,
                           const std::string &appName,
                           const std::string &dataTable,
                           const std::vector<TableVersion> &versions) override;

private:
    enum VersionStateType {
        VST_VERSION = 0,
        VST_VERSION_LIST = 1,
        VST_MAX = 2
    };
    std::shared_ptr<VersionState>
    getOrCreateVersionState(const PartitionId &pid, const std::string &fileNameSuffix, const VersionStateType &type);
    VersionState *createVersionState(const PartitionId &pid, const std::string &fileNameSuffix);
    std::shared_ptr<cm_basic::ZkWrapper> connectToZk() const;

    template <typename T, bool getDataOnNotUpdated>
    bool doSyncFromPersist(const PartitionId &pid, const VersionStateType &type, const std::string &suffix, T &version);
    template <typename T>
    bool persistToZk(const PartitionId &pid, const VersionStateType &type, const std::string &suffix, const T &ctx);

private:
    VersionSynchronizerConfig _config;
    std::shared_ptr<cm_basic::ZkWrapper> _zkWrapper; // created by init
    std::map<std::pair<PartitionId, VersionStateType>, std::shared_ptr<VersionState>> _states;
    mutable autil::ThreadMutex _mutex;
};

} // namespace suez
