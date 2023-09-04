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
#include <utility>

#include "indexlib/base/Types.h"
#include "suez/sdk/PartitionId.h"
#include "suez/table/TableVersion.h"

namespace suez {
class VersionSynchronizer;

class RollbackVersionSelector {
public:
    RollbackVersionSelector(VersionSynchronizer *versionSynchronizer,
                            const std::string &rawIndexRoot,
                            const PartitionId &pid);
    ~RollbackVersionSelector();

private:
    RollbackVersionSelector(const RollbackVersionSelector &);
    RollbackVersionSelector &operator=(const RollbackVersionSelector &);

public:
    //选择一个进度<=timestamp的version，要求在索引路径上存在且被发布过，如果没有满足条件的则返回versionId
    //注意如果索引路径上也没有incVersion会返回INVALID_VERSION
    IncVersion select(int64_t timestamp, IncVersion incVersion) const;

private:
    bool loadSyncedVersion(VersionSynchronizer *versionSynchronizer, std::vector<TableVersion> &versionSynced) const;

private:
    VersionSynchronizer *_versionSynchronizer;
    std::string _partitionRoot;
    PartitionId _pid;
};
} // namespace suez
