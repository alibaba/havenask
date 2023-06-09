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
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Lock.h"
#include "build_service/workflow/RealtimeBuilderDefine.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/partition/index_partition.h"
#include "suez/deploy/IndexDeployer.h"
#include "suez/sdk/SuezIndexPartitionData.h"
#include "suez/table/InnerDef.h"
#include "suez/table/PartitionProperties.h"
#include "suez/table/SuezPartition.h"
#include "suez/table/TableBuilder.h"
#include "suez/table/TableMeta.h"
#include "suez/table/TableVersion.h"

namespace indexlibv2::config {
class ITabletSchema;
}

namespace build_service::workflow {
class RealtimeInfoWrapper;
} // namespace build_service::workflow

namespace suez {

class IIndexlibAdapter;
class TableWriter;

class SuezIndexPartition : public SuezPartition {
public:
    SuezIndexPartition(const TableResource &tableResource, const CurrentPartitionMetaPtr &partitionMeta);
    virtual ~SuezIndexPartition();

private:
    SuezIndexPartition(const SuezIndexPartition &);
    SuezIndexPartition &operator=(const SuezIndexPartition &);

public:
    StatusAndError<DeployStatus> deploy(const TargetPartitionMeta &target, bool distDeploy) override;
    void cancelDeploy() override;
    void cancelLoad() override;
    StatusAndError<TableStatus> load(const TargetPartitionMeta &target, bool force) override;
    void unload() override;
    void stopRt() override;
    void suspendRt() override;
    void resumeRt() override;
    bool hasRt() const override;
    bool isRecovered() const override;
    void forceOnline() override;
    void setTimestampToSkip(int64_t timestamp) override;
    void cleanIndexFiles(const std::set<IncVersion> &inUseIncVersions) override;
    bool isInUse() const override;
    SuezPartitionDataPtr getPartitionData() const override;
    void stopWrite(StopWriteOption option = SWO_ALL) override;
    std::shared_ptr<TableWriter> getTableWriter() const override;
    void setTableWriter(const std::shared_ptr<TableWriter> &writer) override;
    bool initPartitionSourceReaderConfig(PartitionSourceReaderConfig &config) const override;

private:
    TableStatus loadFull(const TargetPartitionMeta &target, const PartitionProperties &properties);
    TableStatus loadInc(const PartitionProperties &target, uint64_t branchId, bool force);
    void updateIncVersion(const std::pair<TableVersion, SchemaVersion> &loadedVersion,
                          const std::shared_ptr<indexlibv2::config::ITabletSchema> &schema);
    bool loadRt(IIndexlibAdapter *partition, const PartitionProperties &properties);
    StatusAndError<DeployStatus> deployConfig(const TargetPartitionMeta &target, const std::string &localConfigPath);
    StatusAndError<TableStatus> constructLoadStatus(TableStatus ts, bool force);

    int32_t getUseCount() const override;

private:
    static EffectiveFieldInfo
    constructEffectiveFieldInfo(const std::shared_ptr<indexlibv2::config::ITabletSchema> &schema);

protected:
    bool initPartitionProperties(const TargetPartitionMeta &target, PartitionProperties &properties) const;
    virtual DeployStatus doDeployIndex(const TargetPartitionMeta &target, bool distDeploy);
    // bs v1不支持raw doc模式有processor，v2支持，因此pid需要填充data_table
    virtual bool getBsPartitionId(const PartitionProperties &properties, build_service::proto::PartitionId &pid) const;

private:
    // virtual for test
    virtual bool loadTableConfig(const TargetPartitionMeta &target, TableConfig &tableConfig) const;
    virtual std::unique_ptr<IIndexlibAdapter> createIndexlibAdapter(const PartitionProperties &properties,
                                                                    uint64_t branchId) const;
    virtual DeployStatus doDeployConfig(const std::string &src, const std::string &dst);
    virtual TableBuilder *createTableBuilder(IIndexlibAdapter *indexlibAdapter,
                                             const PartitionProperties &properties) const;
    virtual IncVersion getUnloadedIncVersion() { return -1; }

    // only supported for indexlibv2
    virtual std::shared_ptr<TableWriter> createTableWriter(const PartitionProperties &properties) const;
    virtual bool initLogReplicator(IIndexlibAdapter *indexlibAdapter, const PartitionProperties &properties);
    virtual void closeLogReplicator();

    bool cleanUnreferencedDeployIndexFiles(const IndexDeployer::IndexPathDetail &pathDetail,
                                           const indexlibv2::config::TabletOptions &targetTabletOptions,
                                           IncVersion targetVersion);
    void setIndexlibAdapter(std::shared_ptr<IIndexlibAdapter> indexlibAdapter);

    void doWorkBeforeForceLoad();
    bool doWorkAfterLoaded(IIndexlibAdapter *indexlibAdapter, const PartitionProperties &properties);
    void stopTableWriter();

protected:
    std::shared_ptr<IIndexlibAdapter> getIndexlibAdapter() const;

protected:
    std::unique_ptr<FileDeployer> _configDeployer;
    std::unique_ptr<IndexDeployer> _indexDeployer;
    indexlib::partition::PartitionGroupResourcePtr _globalIndexResource;
    build_service::workflow::BuildFlowThreadResource _globalBuilderThreadResource;
    build_service::util::SwiftClientCreatorPtr _swiftClientCreator;

    std::shared_ptr<IIndexlibAdapter> _indexlibAdapter;
    mutable autil::ThreadMutex _mutex; // lock for IIndexlibAdapter

    build_service::workflow::RealtimeInfoWrapper _realtimeInfo;
    std::unique_ptr<TableBuilder> _tableBuilder;
    mutable autil::ThreadMutex _builderMutex;

    std::shared_ptr<TableWriter> _tableWriter;
    mutable autil::ThreadMutex _writerMutex;

    bool _allowLoadUtilRtRecovered;
    mutable std::shared_ptr<std::atomic<int32_t>> _refCount;
};

using SuezIndexPartitionPtr = std::shared_ptr<SuezIndexPartition>;

} // namespace suez
