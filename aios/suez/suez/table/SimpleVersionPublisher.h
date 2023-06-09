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
#include "suez/table/VersionPublisher.h"

namespace suez {

class SimpleVersionPublisher final : public VersionPublisher {
public:
    void remove(const PartitionId &pid) override;
    bool publish(const std::string &path, const PartitionId &pid, const TableVersion &version) override;
    bool getPublishedVersion(const std::string &path,
                             const PartitionId &pid,
                             IncVersion versionId,
                             TableVersion &version) override;

private:
    bool needPublish(const PartitionId &pid, const TableVersion &version) const;
    bool doPublish(const std::string &path, const PartitionId &pid, const TableVersion &version);
    void updatePublishedVersion(const PartitionId &pid, const TableVersion &version);
    bool getPublishedVersion(const PartitionId &pid, TableVersion &version) const;

private:
    std::map<PartitionId, TableVersion> _publishedVersionMap;
    mutable autil::ReadWriteLock _lock;
};

} // namespace suez
