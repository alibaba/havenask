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

#include "autil/LoopThread.h"
#include "build_service/build_task/BuildTaskCurrent.h"
#include "build_service/build_task/BuildTaskTarget.h"
#include "build_service/build_task/BuilderController.h"
#include "build_service/build_task/SingleBuilder.h"
#include "build_service/config/MergeControllerConfig.h"
#include "build_service/config/TaskTarget.h"
#include "build_service/proto/BuildTaskCurrentInfo.h"
#include "build_service/proto/BuildTaskTargetInfo.h"
#include "build_service/task_base/Task.h"
#include "build_service/util/Log.h"
#include "future_lite/TaskScheduler.h"
#include "indexlib/framework/ITablet.h"
#include "indexlib/framework/ITabletMergeController.h"
#include "indexlib/framework/IdGenerator.h"
#include "indexlib/framework/Version.h"

namespace build_service::build_task {

class BuildTask : public task_base::Task
{
public:
    static const std::string TASK_NAME;
    static const std::string STEP_FULL;
    static const std::string STEP_INC;

public:
    BuildTask() = default;
    ~BuildTask();

public:
    bool init(Task::TaskInitParam& initParam) override;
    bool handleTarget(const config::TaskTarget& target) override;
    bool isDone(const config::TaskTarget& target) override;
    indexlib::util::CounterMapPtr getCounterMap() override;
    std::string getTaskStatus() override;
    bool hasFatalError() override;
    proto::ProgressStatus getTaskProgress() const override;

private:
    std::pair<indexlib::Status, std::shared_ptr<indexlibv2::framework::ITablet>>
    prepareTablet(const std::shared_ptr<BuildTaskTarget>& target, const std::string& indexRoot,
                  const indexlibv2::framework::VersionCoord& versionCoord, bool autoMerge);
    std::pair<indexlib::Status, std::shared_ptr<indexlibv2::config::ITabletSchema>>
    getTabletSchema(const std::shared_ptr<indexlibv2::config::TabletOptions>& tabletOptions) const;
    std::string getBuilderPartitionIndexRoot() const;
    std::string getPartitionIndexRoot() const;
    std::shared_ptr<indexlibv2::framework::ITabletMergeController>
    createMergeController(const std::string& mergeControllerMode,
                          const std::shared_ptr<indexlibv2::config::TabletOptions>& tabletOptions,
                          const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema, uint64_t branchId) const;
    bool createQuotaController(int64_t buildTotalMemory);
    bool createFileBlockCacheContainer();
    bool createRole(const std::shared_ptr<BuildTaskTarget>& buildTarget);
    void workLoop();
    std::shared_ptr<indexlibv2::framework::IdGenerator> getIdGenerator() const;
    indexlib::Status loadVersion(const indexlibv2::framework::VersionMeta& versionMeta,
                                 indexlibv2::framework::Version& version) const;
    bool reopenPublishedVersion(const std::shared_ptr<BuildTaskTarget>& buildTaskTarget);
    bool commitAndReopen(versionid_t alignVersionId, const std::shared_ptr<BuildTaskTarget>& target,
                         const std::optional<indexlibv2::framework::Locator>& specifyLocator,
                         proto::VersionInfo* versionInfo);
    std::unique_ptr<future_lite::Executor> createExecutor(const std::string& executorName, uint32_t threadCount);
    void updateCurrent(const proto::VersionInfo& commitedVersionInfo, const versionid_t alignFailedVersionId,
                       const std::shared_ptr<BuildTaskTarget>& target);
    void collectErrorInfos() const;
    bool parseFromTaskTarget(const config::TaskTarget& target, proto::BuildTaskTargetInfo* buildTaskTargetInfo) const;
    std::pair<indexlib::Status, std::shared_ptr<indexlibv2::framework::ITabletMergeController>>
    getMergeController(const config::MergeControllerConfig& controllerConfig,
                       const std::shared_ptr<indexlibv2::config::TabletOptions>& tabletOptions, uint64_t branchId);
    bool prepareResource();
    int64_t getMachineTotalMemoryMb() const;
    proto::PartitionId getPartitionRange() const;
    indexlibv2::framework::Version getVersion();
    std::pair<std::string, indexlibv2::framework::VersionCoord>
    getVersionCoord(const std::shared_ptr<BuildTaskTarget>& buildTaskInfo) const;
    bool needManualMerge(const std::shared_ptr<BuildTaskTarget>& target,
                         const std::shared_ptr<BuildTaskCurrent>& current);
    bool executeManualMerge(const std::shared_ptr<BuildTaskTarget>& target);
    bool isBuildFinished(const std::shared_ptr<BuildTaskTarget>& target);
    bool needImportNewVersions(const std::shared_ptr<BuildTaskTarget>& buildTarget);
    versionid_t getAlignVersionId(const std::shared_ptr<BuildTaskTarget>& target,
                                  const std::shared_ptr<BuildTaskCurrent>& current);
    indexlib::Status cleanTmpBuildDir();
    bool cleanFullBuildIndex();
    std::pair<indexlib::Status, bool>
    hasNewMergedSegment(indexlibv2::versionid_t versionId,
                        const std::shared_ptr<indexlibv2::framework::ITablet>& tablet);

    std::pair<indexlib::Status, bool> isVersionMatchBatchId(indexlibv2::versionid_t versionId, int32_t batchId);

    bool isVersionAlreadyCommitted(versionid_t alignVersionId, const std::shared_ptr<BuildTaskTarget>& target);
    void updatePartitionDocCounter(const std::shared_ptr<indexlibv2::framework::ITablet>& tablet);
    bool preparePartitionDocCounter();
    // virtual for test
    virtual std::shared_ptr<indexlibv2::framework::ITablet>
    createTablet(const std::shared_ptr<BuildTaskTarget>& target,
                 const std::shared_ptr<indexlibv2::framework::ITabletMergeController>& mergeController,
                 const std::shared_ptr<indexlibv2::config::TabletOptions>& tabletOptions) const;
    virtual void handleFatalError(const std::string& message) override;
    bool prepareInitialVersion(const std::shared_ptr<BuildTaskTarget>& buildTarget);

private:
    mutable std::mutex _taskInfoMutex;
    mutable std::mutex _tabletMutex;
    mutable std::mutex _targetMutex;
    Task::TaskInitParam _initParam;
    std::shared_ptr<indexlibv2::framework::ITablet> _tablet;
    std::shared_ptr<autil::LoopThread> _workLoopThread;
    std::unique_ptr<SingleBuilder> _singleBuilder;
    std::unique_ptr<BuilderController> _builderController;
    std::unique_ptr<future_lite::Executor> _executor;
    std::unique_ptr<future_lite::Executor> _dumpExecutor;
    std::unique_ptr<future_lite::Executor> _localMergeExecutor;
    std::unique_ptr<future_lite::TaskScheduler> _taskScheduler;
    std::shared_ptr<indexlibv2::MemoryQuotaController> _totalMemoryController;
    std::shared_ptr<indexlibv2::MemoryQuotaController> _buildMemoryController;
    std::shared_ptr<indexlib::file_system::FileBlockCacheContainer> _fileBlockCacheContainer;
    std::shared_ptr<indexlib::util::StateCounter> _partitionDocCounter;

    std::shared_ptr<BuildTaskTarget> _target;
    std::shared_ptr<BuildTaskCurrent> _current;
    std::string _buildStep;
    proto::BuildMode _buildMode;

    std::atomic<bool> _hasFatalError = false;
    bool _isFullBuildIndexCleaned = false;
    config::BuildServiceConfig _buildServiceConfig;
    uint32_t _consecutiveIoErrorTimes = 0; // consecutive io error times when preparing tablet

    static constexpr const uint32_t CONSECUTIVE_IO_ERROR_LIMIT = 20;

private:
    BS_LOG_DECLARE();
};

} // namespace build_service::build_task
