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
#include "suez/table/VersionSynchronizer.h"

namespace suez {

class LocalVersionSynchronizer : public VersionSynchronizer {
public:
    LocalVersionSynchronizer();
    ~LocalVersionSynchronizer();

private:
    LocalVersionSynchronizer(const LocalVersionSynchronizer &);
    LocalVersionSynchronizer &operator=(const LocalVersionSynchronizer &);

public:
    bool init() override { return true; }
    void remove(const PartitionId &pid) override { _versionList.erase(pid); }
    bool syncFromPersist(const PartitionId &pid, const std::string &configPath, TableVersion &version) override {
        return true;
    }
    bool supportSyncFromPersist(const PartitionMeta &target) const override { return true; }
    bool
    persistVersion(const PartitionId &pid, const std::string &localConfigPath, const TableVersion &version) override {
        return true;
    }
    bool getVersionList(const PartitionId &pid, std::vector<TableVersion> &versions) override {
        versions = _versionList[pid];
        return true;
    }
    bool updateVersionList(const PartitionId &pid, const std::vector<TableVersion> &versions) override {
        if (_versionList.find(pid) != _versionList.end()) {
            _versionList[pid] = versions;
        }
        return true;
    }

private:
    std::map<PartitionId, std::vector<TableVersion>> _versionList;
};

} // namespace suez
