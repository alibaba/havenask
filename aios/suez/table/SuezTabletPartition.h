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

#include <mutex>

#include "autil/Lock.h"
#include "suez/table/SuezIndexPartition.h"

namespace build_service {
namespace workflow {
class RealtimeInfoWrapper;
}
} // namespace build_service

namespace indexlibv2::config {
class TabletSchema;
}

namespace indexlibv2::framework {
class ITabletMergeController;
}

namespace suez {
class LogReplicator;

class SuezTabletPartition : public SuezIndexPartition {
public:
    SuezTabletPartition(const TableResource &tableResource, const CurrentPartitionMetaPtr &partitionMeta);
    ~SuezTabletPartition();

public:
    bool needCommit() const override;
    std::pair<bool, TableVersion> commit() override;

private:
    DeployStatus doDeployIndex(const TargetPartitionMeta &target, bool distDeploy) override;
    std::unique_ptr<IIndexlibAdapter> createIndexlibAdapter(const PartitionProperties &properties,
                                                            uint64_t branchId) const override;
    TableBuilder *createTableBuilder(IIndexlibAdapter *indexlibAdapter,
                                     const PartitionProperties &properties) const override;

    std::shared_ptr<TableWriter> createTableWriter(const PartitionProperties &properties) const override;

    std::shared_ptr<indexlibv2::config::TabletSchema> loadSchema(const PartitionProperties &properties) const;
    IncVersion getUnloadedIncVersion() override { return indexlib::INVALID_VERSIONID - 1; };
    std::shared_ptr<indexlibv2::framework::ITabletMergeController>
    createMergeController(const PartitionProperties &properties,
                          const std::shared_ptr<indexlibv2::config::TabletSchema> &schema,
                          uint64_t branchId) const;

    bool getBsPartitionId(const PartitionProperties &properties, build_service::proto::PartitionId &pid) const override;

    bool initLogReplicator(IIndexlibAdapter *indexlibAdapter, const PartitionProperties &properties) override;
    void closeLogReplicator() override;
    bool needReopenRt(const TableVersion &version) const;
    bool reopenRt(const TableVersion &version);

private:
    future_lite::Executor *_executor;
    future_lite::Executor *_dumpExecutor;
    future_lite::TaskScheduler *_taskScheduler;
    std::unique_ptr<LogReplicator> _logReplicator;
    mutable autil::ThreadMutex _mutex;
};

} // namespace suez
