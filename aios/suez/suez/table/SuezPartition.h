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
#include <vector>

#include "build_service/workflow/RealtimeBuilderDefine.h"
#include "suez/sdk/SuezPartitionData.h"
#include "suez/table/InnerDef.h"
#include "suez/table/TableMeta.h"
#include "suez/table/TableVersion.h"

#define SUEZ_PREFIX_LOG(level, format, args...) AUTIL_LOG(level, "[%p,%s] " format, this, SUEZ_PREFIX, ##args)

namespace kmonitor {
class MetricsReporter;
using MetricsReporterPtr = std::shared_ptr<MetricsReporter>;
} // namespace kmonitor

namespace build_service::util {
class SwiftClientCreator;
using SwiftClientCreatorPtr = std::shared_ptr<SwiftClientCreator>;
} // namespace build_service::util

namespace indexlib::partition {
class PartitionGroupResource;
using PartitionGroupResourcePtr = std::shared_ptr<PartitionGroupResource>;
} // namespace indexlib::partition

namespace indexlibv2 {
class MemoryQuotaController;
}

namespace future_lite {
class Executor;
class TaskScheduler;
} // namespace future_lite

namespace suez {

class DiskQuotaController;
class DeployManager;
class TableWriter;
struct PartitionSourceReaderConfig;

struct TableResource {
    TableResource() {}

    TableResource(const PartitionId &pid_,
                  kmonitor::MetricsReporterPtr &metricsReporter_,
                  const indexlib::partition::PartitionGroupResourcePtr &globalIndexResource_,
                  const build_service::util::SwiftClientCreatorPtr &swiftClientCreator_,
                  DeployManager *deployManager_)
        : pid(pid_)
        , metricsReporter(metricsReporter_)
        , globalIndexResource(globalIndexResource_)
        , swiftClientCreator(swiftClientCreator_)
        , deployManager(deployManager_) {}
    // for ut
    TableResource(const PartitionId &pid_) : pid(pid_), deployManager(nullptr) {}
    PartitionId pid;
    kmonitor::MetricsReporterPtr metricsReporter;
    indexlib::partition::PartitionGroupResourcePtr globalIndexResource;
    build_service::workflow::BuildFlowThreadResource globalBuilderThreadResource;
    build_service::util::SwiftClientCreatorPtr swiftClientCreator;
    DeployManager *deployManager = nullptr;
    bool allowLoadUtilRtRecovered = true;

    // for Tablet
    future_lite::Executor *executor = nullptr;
    future_lite::Executor *dumpExecutor = nullptr;
    future_lite::TaskScheduler *taskScheduler = nullptr;
};

template <typename StatusType>
struct StatusAndError {
    StatusType status;
    SuezError error;

    StatusAndError() = default;

    StatusAndError(StatusType type) : status(type), error(ERROR_NONE) {}

    StatusAndError(StatusType type, SuezError se) : status(type), error(std::move(se)) {}

    void setError(const SuezError &error) { this->error = error; }
};

class SuezPartition {
public:
    enum StopWriteOption {
        SWO_DRC = 0x1,
        SWO_WRITE = 0x10,
        SWO_BUILD = 0x100,
        SWO_ALL = 0x111,
    };

public:
    SuezPartition();
    SuezPartition(const CurrentPartitionMetaPtr &partitionMeta,
                  const PartitionId &pid,
                  const kmonitor::MetricsReporterPtr &metricsReporter);
    virtual ~SuezPartition();

private:
    SuezPartition(const SuezPartition &);
    SuezPartition &operator=(const SuezPartition &);

public:
    virtual StatusAndError<DeployStatus> deploy(const TargetPartitionMeta &target, bool distDeploy) = 0;
    virtual void cancelDeploy() = 0;
    virtual void cancelLoad() = 0;
    virtual StatusAndError<TableStatus> load(const TargetPartitionMeta &target, bool force) = 0;
    virtual void unload() = 0;
    virtual void stopRt() = 0;
    virtual void suspendRt() = 0;
    virtual void resumeRt() = 0;
    virtual bool hasRt() const = 0;
    virtual bool isInUse() const = 0;
    virtual int32_t getUseCount() const = 0;
    virtual SuezPartitionDataPtr getPartitionData() const = 0;
    virtual bool isRecovered() const = 0;
    virtual void forceOnline() { _partitionMeta->setForceOnline(true); }
    virtual void setTimestampToSkip(int64_t timestamp) { _partitionMeta->setTimestampToSkip(timestamp); }
    virtual void cleanIndexFiles(const std::set<IncVersion> &inUseVersions);

    virtual bool needCommit() const;
    virtual std::pair<bool, TableVersion> commit();
    virtual std::shared_ptr<TableWriter> getTableWriter() const { return nullptr; }
    virtual void setTableWriter(const std::shared_ptr<TableWriter> &writer) {}
    virtual void stopWrite(StopWriteOption option = SWO_ALL) {}
    virtual bool initPartitionSourceReaderConfig(PartitionSourceReaderConfig &config) const { return false; }

public:
    const PartitionId &getPartitionId() const;
    void shutdown();

protected:
    int32_t getTotalPartitionCount() const;
    StatusAndError<DeployStatus> setDeployStatus(DeployStatus status, const TargetPartitionMeta &target);
    StatusAndError<DeployStatus> setDeployConfigStatus(DeployStatus status, const TargetPartitionMeta &target);
    std::string getLocalIndexPath() const;

protected:
    CurrentPartitionMetaPtr _partitionMeta;
    const PartitionId _pid;
    kmonitor::MetricsReporterPtr _metricsReporter;
    bool _shutdownFlag;
};

using SuezPartitionPtr = std::shared_ptr<SuezPartition>;

} // namespace suez
