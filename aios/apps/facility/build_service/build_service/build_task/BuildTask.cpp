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
#include "build_service/build_task/BuildTask.h"

#include <assert.h>
#include <map>
#include <stddef.h>
#include <tuple>
#include <vector>

#include "autil/EnvUtil.h"
#include "autil/Log.h"
#include "autil/MemUtil.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "autil/legacy/legacy_jsonizable_dec.h"
#include "build_service/config/BuilderConfig.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/config/TaskConfig.h"
#include "build_service/general_task/GeneralTask.h"
#include "build_service/merge/RemoteTabletMergeController.h"
#include "build_service/proto/DataDescription.h"
#include "build_service/util/IndexPathConstructor.h"
#include "build_service/util/ParallelIdGenerator.h"
#include "future_lite/ExecutorCreator.h"
#include "future_lite/coro/LazyHelper.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/Progress.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileBlockCacheContainer.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/framework/CommitOptions.h"
#include "indexlib/framework/IndexRoot.h"
#include "indexlib/framework/OpenOptions.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/TabletCreator.h"
#include "indexlib/framework/TabletId.h"
#include "indexlib/framework/TabletInfos.h"
#include "indexlib/framework/TabletSchemaLoader.h"
#include "indexlib/framework/VersionCleaner.h"
#include "indexlib/framework/VersionCoord.h"
#include "indexlib/framework/VersionLoader.h"
#include "indexlib/table/index_task/IndexTaskConstant.h"
#include "indexlib/table/index_task/LocalTabletMergeController.h"
#include "indexlib/util/IpConvertor.h"
#include "indexlib/util/JsonUtil.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/counter/CounterMap.h"
#include "indexlib/util/counter/StateCounter.h"
#include "indexlib/util/metrics/MetricProvider.h"
#include "kmonitor/client/MetricsReporter.h"

namespace build_service::build_task {

BS_LOG_SETUP(build_task, BuildTask);

const std::string BuildTask::TASK_NAME = "builderV2";

BuildTask::~BuildTask()
{
    if (_workLoopThread != nullptr) {
        _workLoopThread->stop();
        _workLoopThread.reset();
    }
    if (_partitionDocCounter) {
        _partitionDocCounter.reset();
    }
    if (_singleBuilder != nullptr) {
        _singleBuilder.reset();
    }
    if (_builderController != nullptr) {
        _builderController.reset();
    }
    if (_tablet != nullptr) {
        _tablet->Close();
    }
    if (_taskScheduler != nullptr) {
        _taskScheduler.reset();
    }
    if (_executor != nullptr) {
        _executor.reset();
    }
    BS_LOG(INFO, "end of build task.");
}

bool BuildTask::init(Task::TaskInitParam& initParam)
{
    _initParam = initParam;
    _current.reset(new BuildTaskCurrent);
    if (!_initParam.resourceReader->getConfigWithJsonPath(config::BUILD_APP_FILE_NAME, "", _buildServiceConfig)) {
        BS_LOG(ERROR, "failed to parse build_app.json, config path:%s",
               _initParam.resourceReader->getConfigPath().c_str());
        return false;
    }

    _schemaListKeeper.reset(new config::RealtimeSchemaListKeeper);
    _schemaListKeeper->init(_buildServiceConfig.getIndexRoot(), _initParam.clusterName,
                            _initParam.buildId.generationId);
    return true;
}

std::pair<indexlib::Status, std::shared_ptr<indexlibv2::config::ITabletSchema>>
BuildTask::getConfigTabletSchema(const std::shared_ptr<indexlibv2::config::TabletOptions>& tabletOptions) const
{
    auto tabletSchema = _initParam.resourceReader->getTabletSchema(_initParam.clusterName);
    if (tabletSchema == nullptr) {
        BS_LOG(ERROR, "get schema for cluster[%s] failed", _initParam.clusterName.c_str());
        return {indexlib::Status::InternalError(), nullptr};
    }

    const std::string indexPath = getBuilderPartitionIndexRoot();
    auto status =
        indexlibv2::framework::TabletSchemaLoader::ResolveSchema(tabletOptions, indexPath, tabletSchema.get());
    if (!status.IsOK()) {
        BS_LOG(ERROR, "resolve schema failed: %s", status.ToString().c_str());
        return {status, nullptr};
    }
    return {indexlib::Status::OK(), tabletSchema};
}

std::string BuildTask::getBuilderPartitionIndexRoot() const
{
    const std::string builderIndexRoot =
        _buildServiceConfig.getBuilderIndexRoot(_buildStep == config::BUILD_STEP_FULL_STR);
    auto pid = getPartitionRange();
    return util::IndexPathConstructor::constructIndexPath(builderIndexRoot, pid);
}

std::string BuildTask::getPartitionIndexRoot() const
{
    auto pid = getPartitionRange();
    return util::IndexPathConstructor::constructIndexPath(_buildServiceConfig.getIndexRoot(), pid);
}

proto::PartitionId BuildTask::getPartitionRange() const { return SingleBuilder::getPartitionRange(_initParam); }

std::shared_ptr<indexlibv2::framework::ITabletMergeController> BuildTask::createMergeController(
    const std::string& mergeControllerMode, const std::shared_ptr<indexlibv2::config::TabletOptions>& tabletOptions,
    const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema, uint64_t branchId) const
{
    if (mergeControllerMode == "local") {
        indexlibv2::table::LocalTabletMergeController::InitParam param;
        param.branchId = branchId;
        param.executor = _localMergeExecutor.get();
        param.schema = schema;
        param.options = tabletOptions;
        param.buildTempIndexRoot = getBuilderPartitionIndexRoot();
        param.partitionIndexRoot = getPartitionIndexRoot();
        param.metricProvider = _initParam.metricProvider;
        param.extendResources =
            task_base::GeneralTask::prepareExtendResource(_initParam.resourceReader, _initParam.clusterName);
        auto controller = std::make_shared<indexlibv2::table::LocalTabletMergeController>();
        auto status = controller->Init(param);
        if (!status.IsOK()) {
            BS_LOG(ERROR, "create local merge controller failed: %s", status.ToString().c_str());
            return nullptr;
        }
        return controller;
    } else if (mergeControllerMode == "remote") {
        build_service::merge::RemoteTabletMergeController::InitParam param;
        param.branchId = branchId;
        param.executor = _localMergeExecutor.get();
        param.schema = schema;
        param.options = std::make_shared<indexlibv2::config::TabletOptions>(*tabletOptions);
        param.buildTempIndexRoot = getBuilderPartitionIndexRoot();
        param.remotePartitionIndexRoot = getPartitionIndexRoot();
        param.generationId = _initParam.buildId.generationId;
        param.tableName = _initParam.clusterName;
        param.appName = _initParam.buildId.appName;
        param.rangeFrom = _initParam.pid.range().from();
        param.rangeTo = _initParam.pid.range().to();
        param.configPath = _initParam.resourceReader->getConfigPath();
        param.dataTable = _initParam.buildId.dataTable;
        param.extendResources =
            task_base::GeneralTask::prepareExtendResource(_initParam.resourceReader, _initParam.clusterName);

        auto controller = std::make_shared<build_service::merge::RemoteTabletMergeController>();
        auto status = controller->Init(param);
        if (!status.IsOK()) {
            BS_LOG(ERROR, "create remote merge controller failed: %s", status.ToString().c_str());
            return nullptr;
        }
        return controller;
    } else {
        BS_LOG(ERROR, "un-expected merge controller mode:%s", mergeControllerMode.c_str());
        assert(false);
        return nullptr;
    }
}

int64_t BuildTask::getMachineTotalMemoryMb() const
{
    int64_t totalMemoryBytes = autil::MemUtil::getMachineTotalMem();
    static constexpr double defaultAllowRatio = 0.85;
    auto allowRatio = autil::EnvUtil::getEnv<double>("bs_build_task_memory_allow_ratio", defaultAllowRatio);
    if (allowRatio <= 0 or allowRatio > 1.0) {
        BS_LOG(WARN, "bs_build_task_memory_allow_ratio [%lf] is not valid, set to %lf", allowRatio, defaultAllowRatio);
        allowRatio = defaultAllowRatio;
    }
    totalMemoryBytes = int64_t(totalMemoryBytes * allowRatio);
    return totalMemoryBytes / 1024 / 1024;
}

bool BuildTask::createQuotaController(int64_t buildTotalMemory)
{
    const std::string clusterPath = _initParam.resourceReader->getClusterConfRelativePath(_initParam.clusterName);
    config::BuilderConfig builderConfig;
    if (!_initParam.resourceReader->getConfigWithJsonPath(clusterPath, "build_option_config", builderConfig)) {
        BS_LOG(ERROR, "parse build_option_config failed, cluster:[%s]", _initParam.clusterName.c_str());
        return false;
    }

    int64_t totalMemory = getMachineTotalMemoryMb();
    if (totalMemory < buildTotalMemory) {
        BS_LOG(WARN, "machine total memory [%ld] < build total memory [%ld], will use machine totoal memory",
               totalMemory, buildTotalMemory);
        buildTotalMemory = totalMemory;
    }

    _totalMemoryController =
        std::make_shared<indexlibv2::MemoryQuotaController>("offline_total", buildTotalMemory * 1024 * 1024);
    _buildMemoryController =
        std::make_shared<indexlibv2::MemoryQuotaController>("offline_build", buildTotalMemory * 1024 * 1024);
    BS_LOG(INFO, "offline build, total memory quota[%ld], build memory quota[%ld]", buildTotalMemory, buildTotalMemory);
    return true;
}

bool BuildTask::createFileBlockCacheContainer()
{
    std::string blockCacheParam = autil::EnvUtil::getEnv("RS_BLOCK_CACHE");
    if (!blockCacheParam.empty()) {
        _fileBlockCacheContainer = std::make_shared<indexlib::file_system::FileBlockCacheContainer>();
        _fileBlockCacheContainer->Init(blockCacheParam, _totalMemoryController, nullptr, _initParam.metricProvider);
        BS_LOG(INFO, "offline build, use block cache [%s]", blockCacheParam.c_str());
    }
    return true;
}

std::unique_ptr<future_lite::Executor> BuildTask::createExecutor(const std::string& executorName, uint32_t threadCount)
{
    return future_lite::ExecutorCreator::Create(
        /*type*/ "async_io",
        future_lite::ExecutorCreator::Parameters().SetExecutorName(executorName).SetThreadNum(threadCount));
}

std::shared_ptr<indexlibv2::framework::IdGenerator> BuildTask::getIdGenerator() const
{
    auto versionMaskType =
        _buildMode & proto::PUBLISH ? indexlibv2::IdMaskType::BUILD_PUBLIC : indexlibv2::IdMaskType::BUILD_PRIVATE;
    auto idGenerator =
        std::make_shared<util::ParallelIdGenerator>(indexlibv2::IdMaskType::BUILD_PUBLIC, versionMaskType);
    const auto& instanceInfo = _initParam.instanceInfo;
    auto status = idGenerator->Init(instanceInfo.instanceId, instanceInfo.instanceCount);
    if (!status.IsOK()) {
        BS_LOG(ERROR, "init id generator failed.");
        return nullptr;
    }
    return idGenerator;
}

std::pair<indexlib::Status, std::shared_ptr<indexlibv2::framework::ITabletMergeController>>
BuildTask::getMergeController(const config::MergeControllerConfig& taskControllerConfig,
                              const std::shared_ptr<indexlibv2::config::TabletOptions>& tabletOptions,
                              uint64_t branchId)
{
    std::string mergeControllerMode = taskControllerConfig.mode;
    if (!_localMergeExecutor) {
        _localMergeExecutor = createExecutor("merge_controller", taskControllerConfig.threadNum);
    }
    auto [status, schema] = getConfigTabletSchema(tabletOptions);
    if (!status.IsOK()) {
        BS_LOG(ERROR, "get tablet schema failed");
        return {status, nullptr};
    }
    std::shared_ptr<indexlibv2::framework::ITabletMergeController> mergeController;
    mergeController = createMergeController(mergeControllerMode, tabletOptions, schema, branchId);
    if (mergeController == nullptr) {
        BS_LOG(ERROR, "create merge controller failed.");
        return {indexlib::Status::InternalError(), nullptr};
    }
    return {indexlib::Status::OK(), mergeController};
}

bool BuildTask::prepareResource()
{
    const std::string clusterConfig = _initParam.resourceReader->getClusterConfRelativePath(_initParam.clusterName);
    uint32_t executorThreadCount = 2; // prevent the executor from being blocked by a single task for so long
    uint32_t dumpThreadCount = 1;
    int64_t buildTotalMemory = 6 * 1024;
    if (!_initParam.resourceReader->getConfigWithJsonPath(
            clusterConfig, "offline_index_config.build_config.executor_thread_count", executorThreadCount)) {
        BS_LOG(ERROR, "read offline_index_config.build_config failed, cluster [%s]", _initParam.clusterName.c_str());
        return false;
    }
    if (!_initParam.resourceReader->getConfigWithJsonPath(
            clusterConfig, "offline_index_config.build_config.dump_thread_count", dumpThreadCount)) {
        BS_LOG(ERROR, "read offline_index_config.build_config failed, cluster [%s]", _initParam.clusterName.c_str());
        return false;
    }
    if (!_initParam.resourceReader->getConfigWithJsonPath(
            clusterConfig, "offline_index_config.build_config.build_total_memory", buildTotalMemory)) {
        BS_LOG(ERROR, "read offline_index_config.build_config failed, cluster [%s]", _initParam.clusterName.c_str());
        return false;
    }
    if (!_dumpExecutor) {
        _dumpExecutor = createExecutor("dump_executor", dumpThreadCount);
    }
    if (!_executor) {
        _executor = createExecutor("builder_executor", executorThreadCount);
        _taskScheduler = std::make_unique<future_lite::TaskScheduler>(_executor.get());
    }
    if (!_totalMemoryController || !_buildMemoryController) {
        if (!createQuotaController(buildTotalMemory)) {
            BS_LOG(ERROR, "invalid quota controller.");
            return false;
        }
    }
    createFileBlockCacheContainer();
    return true;
}

// 对于全量阶段存在ssd->hdd的recover场景, worker无法判断当前收到待reopen version是在ssd上还是hdd
// 因此通过之前反馈给admin的indexRoot决策, 并以此创建open tablet
// BUILD模式不用考虑indexRoot
std::pair<std::string, indexlibv2::framework::VersionCoord>
BuildTask::getVersionCoord(const std::shared_ptr<BuildTaskTarget>& buildTaskTarget) const
{
    std::string indexRoot;
    if (!buildTaskTarget->getIndexRoot().empty()) {
        indexRoot = buildTaskTarget->getIndexRoot();
    } else {
        indexRoot = getBuilderPartitionIndexRoot();
    }
    return {indexRoot, buildTaskTarget->getVersionCoord()};
}

std::shared_ptr<indexlibv2::framework::ITablet>
BuildTask::createTablet(const std::shared_ptr<BuildTaskTarget>& target,
                        const std::shared_ptr<indexlibv2::framework::ITabletMergeController>& mergeController,
                        const std::shared_ptr<indexlibv2::config::TabletOptions>& tabletOptions) const
{
    std::shared_ptr<kmonitor::MetricsReporter> reporter;
    if (_initParam.metricProvider != nullptr) {
        reporter = _initParam.metricProvider->GetReporter();
    }

    indexlib::framework::TabletId tid(_initParam.clusterName, _initParam.pid.buildid().generationid(),
                                      _initParam.pid.range().from(), _initParam.pid.range().to());
    tid.SetIp(_initParam.address.ip());
    tid.SetPort(_initParam.address.arpcport());

    return indexlibv2::framework::TabletCreator()
        .SetTabletId(tid)
        .SetExecutor(_dumpExecutor.get(), _taskScheduler.get())
        .SetMemoryQuotaController(_totalMemoryController, _buildMemoryController)
        .SetFileBlockCacheContainer(_fileBlockCacheContainer)
        .SetMetricsReporter(reporter)
        .SetMergeController(mergeController)
        .SetIdGenerator(getIdGenerator())
        .CreateTablet();
}

indexlib::Status BuildTask::prepareTablet(const std::shared_ptr<BuildTaskTarget>& target, const std::string& indexRoot,
                                          const indexlibv2::framework::VersionCoord& versionCoord, bool autoMerge)
{
    BS_LOG(INFO, "target isBatchBuild[%d]", target->isBatchBuild());
    BS_LOG(INFO, "create tablet with auto merge[%d]", autoMerge);

    if (!prepareResource()) {
        BS_LOG(ERROR, "prepare resource failed.");
        return indexlib::Status::InternalError();
    }

    auto tabletOptions = _initParam.resourceReader->getTabletOptions(_initParam.clusterName);
    if (tabletOptions == nullptr) {
        BS_LOG(ERROR, "init tablet options [%s] failed", _initParam.clusterName.c_str());
        return indexlib::Status::InternalError();
    }
    tabletOptions->SetIsOnline(false);
    tabletOptions->SetFlushRemote(true);
    tabletOptions->SetFlushLocal(false);
    tabletOptions->SetAutoMerge(autoMerge);
    tabletOptions->SetIsLeader(true);

    auto [status1, schema] = getConfigTabletSchema(tabletOptions);
    if (!status1.IsOK()) {
        BS_LOG(ERROR, "get tablet schema failed");
        return status1;
    }
    const std::string clusterConfig = _initParam.resourceReader->getClusterConfRelativePath(_initParam.clusterName);
    config::MergeControllerConfig controllerConfig;
    if (!_initParam.resourceReader->getConfigWithJsonPath(clusterConfig, "merge_controller", controllerConfig)) {
        BS_LOG(ERROR, "read merge controller config failed, cluster:%s", _initParam.clusterName.c_str());
        return indexlib::Status::InternalError();
    }

    auto [status2, mergeController] = getMergeController(controllerConfig, tabletOptions, target->getBranchId());
    if (!mergeController) {
        BS_LOG(ERROR, "get merge controller failed");
        return status2;
    }

    auto tablet = createTablet(target, mergeController, tabletOptions);
    std::string finalRoot = indexRoot;
    indexlibv2::framework::VersionCoord finalVersionCoord = versionCoord;
    if (_buildStep == config::BUILD_STEP_FULL_STR && (_buildMode & proto::PUBLISH)) {
        // try to recover merge result
        auto [status, manualMergeResult] = future_lite::coro::syncAwait(mergeController->GetLastMergeTaskResult());
        if (status.IsOK() && manualMergeResult != indexlibv2::INVALID_VERSIONID) {
            _current->markManualMergeFinished();
            finalRoot = getPartitionIndexRoot();
            finalVersionCoord = indexlibv2::framework::VersionCoord(manualMergeResult, "");
            BS_LOG(INFO, "recover full merge result version [%d]", manualMergeResult);
        }
    }
    if (target->isBatchBuild() && (_buildMode & proto::PUBLISH)) {
        int32_t batchId = target->getBatchId();
        // try to recover batch merge result
        auto [status, manualMergeResult] = future_lite::coro::syncAwait(mergeController->GetLastMergeTaskResult());
        if (status.IsOK() && manualMergeResult != indexlibv2::INVALID_VERSIONID) {
            auto [status, batchIdMatch] = isVersionMatchBatchId(manualMergeResult, batchId);
            if (status.IsOK() && batchIdMatch) {
                _current->markManualMergeFinished();
                finalRoot = getPartitionIndexRoot();
                finalVersionCoord = indexlibv2::framework::VersionCoord(manualMergeResult, "");
                BS_LOG(INFO, "recover batch merge result version [%d]", manualMergeResult);
            }
        }
    }
    indexlibv2::framework::IndexRoot finalIndexRoot(finalRoot, finalRoot);
    indexlibv2::framework::Version version;
    if (auto status = loadVersion(finalRoot, finalVersionCoord, &version); !status.IsOK()) {
        BS_LOG(ERROR, "load version [%d] failed, indexRoot[%s]", finalVersionCoord.GetVersionId(), finalRoot.c_str());
        return status;
    }
    if (version.GetVersionId() == indexlib::INVALID_VERSIONID || version.GetSchemaId() == schema->GetSchemaId()) {
        auto status = tablet->Open(finalIndexRoot, schema, tabletOptions, finalVersionCoord);
        if (!status.IsOK()) {
            BS_LOG(ERROR, "open tablet for cluster [%s] with indexRoot [%s] failed.", _initParam.clusterName.c_str(),
                   finalRoot.c_str());
            return status;
        }
    } else {
        if (!_schemaListKeeper->updateSchemaCache()) {
            BS_LOG(ERROR, "udpate schema cache failed");
            return indexlib::Status::Corruption();
        }
        auto indexSchema = _schemaListKeeper->getTabletSchema(version.GetSchemaId());
        if (!indexSchema) {
            BS_LOG(ERROR, "get tablet schema [%u] failed", version.GetSchemaId());
            return indexlib::Status::Corruption();
        }
        auto status = tablet->Open(finalIndexRoot, indexSchema, tabletOptions, finalVersionCoord);
        if (!status.IsOK()) {
            BS_LOG(ERROR, "open tablet for cluster [%s] with indexRoot [%s] failed.", _initParam.clusterName.c_str(),
                   finalRoot.c_str());
            return status;
        }
        std::vector<std::shared_ptr<indexlibv2::config::TabletSchema>> schemaList;
        if (!_schemaListKeeper->getSchemaList(version.GetSchemaId() + 1, schema->GetSchemaId(), schemaList)) {
            BS_LOG(ERROR, "get schema list failed");
            return indexlib::Status::Corruption();
        }
        for (auto tmpSchema : schemaList) {
            status = tablet->AlterTable(tmpSchema);
            if (!status.IsOK()) {
                BS_LOG(ERROR, "alter table with schemaid [%u] failed", schema->GetSchemaId());
                return status;
            }
        }

        while (!tablet->NeedCommit()) {
            // wait alter table commit version
            usleep(1000);
        }
        proto::VersionInfo committedVersionInfo = _current->getCommitedVersionInfo();
        _tablet = tablet;
        if (!commitAndReopen(indexlibv2::INVALID_VERSIONID, target, std::nullopt, &committedVersionInfo)) {
            _tablet.reset();
            BS_LOG(ERROR, "commit alter table version failed.");
            return indexlib::Status::Corruption();
        }
    }
    _tablet = tablet;
    return indexlib::Status::OK();
}

bool BuildTask::prepareInitialVersion(const std::shared_ptr<BuildTaskTarget>& buildTarget)
{
    const auto& dataDescription = buildTarget->getDataDescription();
    if (dataDescription.empty()) {
        BS_LOG(INFO, "no data description return without prepare init version");
        return true;
    }
    uint64_t srcSignature = 0;
    if (!buildTarget->getSrcSignature(&srcSignature)) {
        BS_LOG(ERROR, "get src signature failed");
        return false;
    }
    auto iter = dataDescription.find(config::READ_SRC_TYPE);
    if (iter == dataDescription.end()) {
        BS_LOG(ERROR, "data description read src type not found");
        return false;
    }
    bool isSwiftSrc = (iter->second == "swift") || (iter->second == config::SWIFT_PROCESSED_READ_SRC);
    int64_t locatorOffset = -1;
    if (isSwiftSrc) {
        iter = dataDescription.find(config::SWIFT_START_TIMESTAMP);
        if (iter != dataDescription.end()) {
            if (!autil::StringUtil::fromString(iter->second, locatorOffset)) {
                BS_LOG(ERROR, "parse swift start timestamp failed");
                return false;
            }
        }
    }

    auto lastLocator = _tablet->GetTabletInfos()->GetLatestLocator();
    if (!lastLocator.IsValid() || lastLocator.GetSrc() != srcSignature) {
        indexlibv2::framework::Locator specifyLocator;
        specifyLocator.SetSrc(srcSignature);
        auto pid = _initParam.pid;
        uint32_t from = pid.range().from();
        uint32_t to = pid.range().to();
        bool enableFastSlowQueue = false;
        if (!_initParam.resourceReader->getClusterConfigWithJsonPath(
                _initParam.clusterName, "cluster_config.enable_fast_slow_queue", enableFastSlowQueue)) {
            BS_LOG(ERROR, "parse cluster_config.enable_fast_slow_queue for [%s] failed",
                   _initParam.clusterName.c_str());
            return false;
        }
        size_t topicSize = enableFastSlowQueue ? 2 : 1;
        if (!lastLocator.IsValid()) {
            indexlibv2::base::MultiProgress multiProgress;
            multiProgress.resize(topicSize, {indexlibv2::base::Progress(from, to, {locatorOffset, 0})});
            specifyLocator.SetMultiProgress(multiProgress);
            BS_LOG(INFO, "init initialize locator [%s]", specifyLocator.DebugString().c_str());
        } else {
            indexlibv2::base::MultiProgress multiProgress;
            if (lastLocator.IsLegacyLocator()) {
                multiProgress.resize(topicSize, {indexlibv2::base::Progress(from, to, lastLocator.GetOffset())});
            } else {
                multiProgress.resize(topicSize, {indexlibv2::base::Progress(from, to, {locatorOffset, 0})});
            }
            specifyLocator.SetMultiProgress(multiProgress);
            BS_LOG(INFO,
                   "diff src found, commit version with same src locator, origin locator [%s], target locator [%s]",
                   lastLocator.DebugString().c_str(), specifyLocator.DebugString().c_str());
        }

        proto::VersionInfo committedVersionInfo = _current->getCommitedVersionInfo();
        if (!commitAndReopen(indexlibv2::INVALID_VERSIONID, buildTarget, specifyLocator, &committedVersionInfo)) {
            BS_LOG(ERROR, "commit version failed.");
            return false;
        }
        updateCurrent(committedVersionInfo, indexlibv2::INVALID_VERSIONID, buildTarget);
        updatePartitionDocCounter(_tablet);
    }
    return true;
}

bool BuildTask::createRole(const std::shared_ptr<BuildTaskTarget>& buildTarget)
{
    _buildStep = buildTarget->getBuildStep();
    _buildMode = buildTarget->getBuildMode();
    bool autoMerge = false;
    if (_buildStep == config::BUILD_STEP_INC_STR && _buildMode & proto::PUBLISH) {
        autoMerge = !buildTarget->isBatchBuild(); // Batch build should disable autoMerge
    }

    auto [indexRoot, versionCoord] = getVersionCoord(buildTarget);
    auto st = prepareTablet(buildTarget, indexRoot, versionCoord, autoMerge);
    if (st.IsIOError()) {
        if (++_consecutiveIoErrorTimes > CONSECUTIVE_IO_ERROR_LIMIT) {
            BS_LOG(ERROR, "prepare tablet io failed over [%u] times, will exit.", CONSECUTIVE_IO_ERROR_LIMIT);
            _hasFatalError = true;
            handleFatalError("prepare tablet io failed");
        }
    } else {
        _consecutiveIoErrorTimes = 0;
    }
    if (!st.IsOK()) {
        BS_LOG(ERROR, "create tablet failed.");
        return false;
    }

    if (!prepareInitialVersion(buildTarget)) {
        return false;
    }

    if (!preparePartitionDocCounter()) {
        return false;
    }
    if (_buildMode == proto::PUBLISH) {
        _builderController = std::make_unique<BuilderController>(_initParam, _tablet);
    } else {
        assert(_buildMode & proto::BUILD);
        if (!buildTarget->isFinished()) {
            _singleBuilder = std::make_unique<SingleBuilder>(_initParam, _tablet);
        }
    }
    auto loopInteralInMs = autil::EnvUtil::getEnv<int64_t>("bs_build_task_work_loop_interval_ms", /*default=*/2);
    _workLoopThread = autil::LoopThread::createLoopThread([this]() { workLoop(); }, loopInteralInMs * 1000,
                                                          /*name=*/"BuildTaskWorkLoop");
    if (_workLoopThread == nullptr) {
        _tablet.reset();
        _builderController.reset();
        _singleBuilder.reset();
        return false;
    }

    auto latestPublishedVersion = buildTarget->getLatestPublishedVersion();
    if (latestPublishedVersion == versionCoord.GetVersionId()) {
        _current->updateOpenedPublishedVersion(latestPublishedVersion);
    }

    return true;
}

bool BuildTask::handleTarget(const config::TaskTarget& target)
{
    BS_INTERVAL_LOG(300, INFO, "target status: %s", autil::legacy::ToJsonString(target).c_str());
    BS_LOG(INFO, "start handeTarget");
    auto buildTarget = std::make_shared<BuildTaskTarget>();
    if (!buildTarget->init(target)) {
        BS_LOG(ERROR, "parse build task target info to string failed.");
        return false;
    }
    if (!_tablet) {
        if (!createRole(buildTarget)) {
            BS_LOG(ERROR, "create role failed, build mode[%d], build step[%s]", buildTarget->getBuildMode(),
                   buildTarget->getBuildStep().c_str());
            return false;
        }
    }
    bool isMasterBuilder = _buildMode & proto::PUBLISH;
    auto latestPublishedVersion = buildTarget->getLatestPublishedVersion();
    if (latestPublishedVersion != _current->getOpenedPublishedVersion()) {
        if (!isMasterBuilder && !reopenPublishedVersion(buildTarget)) {
            BS_LOG(ERROR, "reopen latest published version failed.");
            return false;
        }
        _current->updateOpenedPublishedVersion(latestPublishedVersion);
        if (_singleBuilder) {
            _singleBuilder->updateOpenedPublishedVersion(latestPublishedVersion);
        }
    }
    if (_singleBuilder != nullptr && !_singleBuilder->handleTarget(target)) {
        BS_LOG(ERROR, "handle target failed.");
        return false;
    } else if (_builderController != nullptr && needImportNewVersions(buildTarget)) {
        if (!_builderController->handleTarget(target)) {
            BS_LOG(ERROR, "handle target failed.");
            return false;
        }
    }
    std::lock_guard<std::mutex> lock(_targetMutex);
    _target = buildTarget;
    return true;
}

bool BuildTask::needManualMerge(const std::shared_ptr<BuildTaskTarget>& target,
                                const std::shared_ptr<BuildTaskCurrent>& current)
{
    // wait to be done
    std::string specifyMergeConfig = target->getSpecifyMergeConfig();
    if (_buildStep != config::BUILD_STEP_FULL_STR && !target->isBatchBuild() && specifyMergeConfig.empty()) {
        return false;
    }
    if (!target->isFinished()) {
        return false;
    }
    if (_tablet && _tablet->NeedCommit()) {
        return false;
    }
    // is done
    if (current->isManualMergeFinished()) {
        return false;
    }
    return true;
}

bool BuildTask::preparePartitionDocCounter()
{
    if (_buildMode & proto::PUBLISH) {
        auto counterMap = getCounterMap();
        if (counterMap == nullptr) {
            BS_LOG(ERROR, "get counter map failed");
            return false;
        }
        _partitionDocCounter = counterMap->GetStateCounter("offline.partitionDocCount");
        if (_partitionDocCounter == nullptr) {
            BS_LOG(ERROR, "create partition doc counter failed");
            return false;
        }
    }
    return true;
}

versionid_t BuildTask::getAlignVersionId(const std::shared_ptr<BuildTaskTarget>& target,
                                         const std::shared_ptr<BuildTaskCurrent>& current)
{
    // build only align version when manual merge finished
    if ((_buildStep == config::BUILD_STEP_FULL_STR || target->isBatchBuild()) && !current->isManualMergeFinished()) {
        return indexlibv2::INVALID_VERSIONID;
    }
    auto currentAlignVersionId = current->getAlignVersionId();
    auto targetAlignVersionId = target->getAlignVersionId();
    return (currentAlignVersionId != targetAlignVersionId) ? targetAlignVersionId : indexlibv2::INVALID_VERSIONID;
}

bool BuildTask::needImportNewVersions(const std::shared_ptr<BuildTaskTarget>& buildTarget)
{
    auto builderRoot = getBuilderPartitionIndexRoot();
    auto [indexRoot, versionCoord] = getVersionCoord(buildTarget);
    if (indexRoot != builderRoot) {
        BS_LOG(INFO, "switch index root. do not import new versions.");
        return false;
    }
    if (_current->isManualMergeFinished()) {
        BS_LOG(INFO, "manual merge finished. do not import new versions.");
        return false;
    }
    return true;
}

indexlibv2::framework::Version BuildTask::getVersion()
{
    if (_buildMode & proto::PUBLISH) {
        return _tablet->GetTabletInfos()->GetLoadedPublishVersion();
    } else {
        return _tablet->GetTabletInfos()->GetLoadedPrivateVersion();
    }
}

bool BuildTask::executeManualMerge(const std::shared_ptr<BuildTaskTarget>& target)
{
    BS_LOG(INFO, "begin execute manual merge task.");
    assert(target);
    if (target->needSkipMerge()) {
        BS_LOG(INFO, "end execute manual merge task. needSkipMerge = true, no need merge.");
        return true;
    }

    auto sourceVersion = getVersion();
    // builder commit initial version, use version segment count judge no data
    if (sourceVersion.GetSegmentCount() == 0) {
        BS_LOG(INFO, "end execute manual merge task. no data, no need merge.");
        return true;
    }

    indexlibv2::framework::VersionCoord versionCoord(sourceVersion.GetVersionId(), sourceVersion.GetFenceName());

    if (_buildServiceConfig.getBuilderIndexRoot(true) != _buildServiceConfig.getIndexRoot()) {
        versionCoord = indexlibv2::framework::VersionCoord(indexlibv2::INVALID_VERSIONID, "");
        _tablet.reset();
        auto st = prepareTablet(target, getPartitionIndexRoot(), versionCoord, /*autoMerge=*/false);
        if (st.IsIOError()) {
            if (++_consecutiveIoErrorTimes > CONSECUTIVE_IO_ERROR_LIMIT) {
                BS_LOG(ERROR, "prepare tablet io failed over [%u] times, will exit.", CONSECUTIVE_IO_ERROR_LIMIT);
                _hasFatalError = true;
                handleFatalError("prepare tablet io failed");
            }
        } else {
            _consecutiveIoErrorTimes = 0;
        }
        if (!st.IsOK()) {
            BS_LOG(ERROR, "end execute manual merge task. prepare new tablet failed [%s]", st.ToString().c_str());
            return false;
        }
        if (!preparePartitionDocCounter()) {
            BS_LOG(ERROR, "end execute manual merge task. prepare partition doc counter failed");
            return false;
        }
    }

    bool isFullMerge = (_buildStep == config::BUILD_STEP_FULL_STR);
    std::string taskType = indexlibv2::table::MERGE_TASK_TYPE;
    std::string taskName = target->getSpecifyMergeConfig();
    std::map<std::string, std::string> params;
    if (taskName.empty()) {
        taskName = isFullMerge ? indexlibv2::table::DESIGNATE_FULL_MERGE_TASK_NAME
                               : indexlibv2::table::DESIGNATE_BATCH_MODE_MERGE_TASK_NAME;
    }
    if (isFullMerge) {
        params[indexlibv2::table::IS_OPTIMIZE_MERGE] = "true";
        params[indexlibv2::table::NEED_CLEAN_OLD_VERSIONS] = "false";
    }
    auto [status, _] = _tablet->ExecuteTask(sourceVersion, taskType, taskName, params);
    if (!status.IsOK()) {
        BS_LOG(ERROR, "execute manual merge failed, sourceVersionId[%d].", sourceVersion.GetVersionId());
        _hasFatalError = true;
        handleFatalError("execute manual merge failed");
    }
    BS_LOG(INFO, "end execute manual merge done success");
    return true;
}

bool BuildTask::reopenPublishedVersion(const std::shared_ptr<BuildTaskTarget>& buildTaskTarget)
{
    std::lock_guard<std::mutex> lock(_tabletMutex);
    indexlibv2::versionid_t versionId = buildTaskTarget->getLatestPublishedVersion();
    auto [status, hasNewMergedSegment_] = hasNewMergedSegment(versionId, _tablet);
    if (!status.IsOK()) {
        return false;
    }
    if (!hasNewMergedSegment_) {
        return true;
    }

    if (versionId == indexlibv2::INVALID_VERSIONID) {
        return true;
    }
    indexlibv2::framework::ReopenOptions reopenOptions;
    status = _tablet->Reopen(reopenOptions, versionId);
    if (!status.IsOK()) {
        return false;
    }
    if (!_target) {
        // first reopen do not commit new version
        return true;
    }
    proto::VersionInfo committedVersionInfo = _current->getCommitedVersionInfo();
    if (!commitAndReopen(indexlibv2::INVALID_VERSIONID, buildTaskTarget, std::nullopt, &committedVersionInfo)) {
        BS_LOG(ERROR, "commit publish version failed.");
        _hasFatalError = true;
        handleFatalError("commit publish version failed");
        return false;
    }
    updateCurrent(committedVersionInfo, indexlibv2::INVALID_VERSIONID, buildTaskTarget);
    updatePartitionDocCounter(_tablet);
    return true;
}

void BuildTask::updatePartitionDocCounter(const std::shared_ptr<indexlibv2::framework::ITablet>& tablet)
{
    if (tablet && _partitionDocCounter) {
        int64_t tabletDocCount = tablet->GetTabletInfos()->GetTabletDocCount();
        _partitionDocCounter->Set(tabletDocCount);
    }
}

std::pair<indexlib::Status, bool>
BuildTask::hasNewMergedSegment(indexlibv2::versionid_t versionId,
                               const std::shared_ptr<indexlibv2::framework::ITablet>& tablet)
{
    std::string indexRoot = getBuilderPartitionIndexRoot();
    auto rootDir = indexlib::file_system::IDirectory::GetPhysicalDirectory(indexRoot);
    auto [status, version] = indexlibv2::framework::VersionLoader::GetVersion(rootDir, versionId);
    if (!status.IsOK()) {
        BS_LOG(ERROR, "load version [%d] failed", versionId);
        return {status, false};
    }
    auto currentVersion = getVersion();
    for (const auto& [segmentId, schemaId] : *version) {
        if (indexlibv2::framework::Segment::IsMergedSegmentId(segmentId) && !currentVersion.HasSegment(segmentId)) {
            return {indexlib::Status::OK(), true};
        }
    }
    return {indexlib::Status::OK(), false};
}

std::pair<indexlib::Status, bool> BuildTask::isVersionMatchBatchId(indexlibv2::versionid_t versionId, int32_t batchId)
{
    std::string indexRoot = getBuilderPartitionIndexRoot();
    auto rootDir = indexlib::file_system::IDirectory::GetPhysicalDirectory(indexRoot);
    auto [status, version] = indexlibv2::framework::VersionLoader::GetVersion(rootDir, versionId);
    if (!status.IsOK()) {
        BS_LOG(ERROR, "load version [%d] failed", versionId);
        return {status, false};
    }
    std::string batchIdStr;
    if (!version->GetDescription("batchId", batchIdStr)) {
        return {indexlib::Status::OK(), false};
    }
    int32_t versionBatchId;
    if (!autil::StringUtil::fromString(batchIdStr, versionBatchId)) {
        BS_LOG(ERROR, "invalid batchId [%s] in version [%d] failed", batchIdStr.c_str(), versionId);
        return {indexlib::Status::OK(), false};
    }
    bool match = (versionBatchId == batchId);
    return {indexlib::Status::OK(), match};
}

bool BuildTask::hasFatalError()
{
    bool hasFatalError = false;
    if (_singleBuilder != nullptr) {
        hasFatalError = _singleBuilder->hasFatalError();
    } else if (_builderController != nullptr) {
        hasFatalError = _builderController->hasFatalError();
    } else {
        BS_LOG(DEBUG, "un-expected, both single-builder and builder-controller are nullptr.");
    }
    if (!_hasFatalError) {
        _hasFatalError = hasFatalError;
    }
    return _hasFatalError;
}

bool BuildTask::commitAndReopen(versionid_t alignVersionId, const std::shared_ptr<BuildTaskTarget>& target,
                                const std::optional<indexlibv2::framework::Locator>& specifyLocator,
                                proto::VersionInfo* versionInfo)
{
    AUTIL_LOG(INFO, "begin commit and reopen, align version id [%d]", alignVersionId);
    bool needPublish = _buildMode & proto::PUBLISH;
    // TODO maybe support flush local in future, need set commit option
    assert(!_tablet->GetTabletOptions()->FlushLocal());
    indexlibv2::framework::CommitOptions options;
    if (specifyLocator) {
        options.SetSpecificLocator(specifyLocator.value());
    }
    options.SetNeedReopenInCommit(true);
    options.SetNeedPublish(needPublish).SetTargetVersionId(alignVersionId);
    options.AddVersionDescription("generation", autil::StringUtil::toString(_initParam.buildId.generationId));
    if (target && target->isBatchBuild()) {
        options.AddVersionDescription("batch_id", autil::StringUtil::toString(target->getBatchId()));
        std::string batchMask;
        if (target->getBatchMask(&batchMask)) {
            options.AddVersionDescription("batch_mask", batchMask);
        }
    }

    std::pair<indexlib::Status, indexlibv2::framework::VersionMeta> ret = _tablet->Commit(options);
    if (!ret.first.IsOK()) {
        if (ret.first.IsExist()) {
            BS_LOG(WARN, "align to specified version [%d] failed.", alignVersionId);
            return false;
        }
        BS_LOG(ERROR, "align to specified version [%d] failed. Not IsExist Error. [%s]", alignVersionId,
               ret.first.ToString().c_str());
        _hasFatalError = true;
        handleFatalError("commit in workloop failed.");
    }
    const indexlibv2::framework::VersionMeta& versionMeta = ret.second;
    updatePartitionDocCounter(_tablet);
    if (versionInfo != nullptr) {
        auto version = getVersion();
        versionInfo->indexRoot = _tablet->GetTabletInfos()->GetIndexRoot().GetLocalRoot();
        versionInfo->versionMeta = versionMeta;
    }
    AUTIL_LOG(INFO, "end commit and reopen, align version id [%d]", alignVersionId);
    return true;
}

bool BuildTask::cleanFullBuildIndex()
{
    auto builderIndexRoot = _buildServiceConfig.getBuilderIndexRoot(_buildStep == config::BUILD_STEP_FULL_STR);
    auto indexRoot = _buildServiceConfig.getIndexRoot();
    if (builderIndexRoot != indexRoot) {
        BS_LOG(INFO, "no need clean full build index for builder root [%s] != index root [%s]",
               builderIndexRoot.c_str(), indexRoot.c_str());
        // tmp generation root will be deleted by admin
        return true;
    } else {
        auto version = getVersion();
        auto versionId = version.GetVersionId();
        auto partitionRoot = getBuilderPartitionIndexRoot();
        BS_LOG(INFO, "begin clean full build index [%s]", partitionRoot.c_str());
        autil::ScopedTime2 timer;
        auto physicalDir = indexlib::file_system::Directory::GetPhysicalDirectory(partitionRoot)->GetIDirectory();
        indexlibv2::framework::VersionCleaner versionCleaner;
        indexlibv2::framework::VersionCleaner::VersionCleanerOptions options;
        options.keepVersionCount = 1;
        options.keepVersionHour = 0;
        options.currentMaxVersionId = versionId;
        auto status = versionCleaner.Clean(physicalDir, options);
        BS_LOG(INFO, "end clean full build index [%s], use [%.1f]s, status [%s]", partitionRoot.c_str(),
               timer.done_sec(), status.ToString().c_str());
        return status.IsOK();
    }
    return true;
}

std::tuple<std::vector<std::string>, std::shared_ptr<indexlibv2::framework::ImportExternalFileOptions>,
           indexlibv2::framework::Action>
BuildTask::prepareBulkloadParams(const std::string& externalFilesStr, const std::string& optionsStr,
                                 const std::string& actionStr, std::string* errMsg)
{
    std::string tmpExternalFilesStr = externalFilesStr;
    std::string tmpOptionsStr = optionsStr;
    std::string tmpActionStr = actionStr;

    std::vector<std::string> externalFiles;
    std::shared_ptr<indexlibv2::framework::ImportExternalFileOptions> options = nullptr;
    if (tmpExternalFilesStr.empty()) {
        tmpExternalFilesStr = autil::legacy::FastToJsonString(std::vector<std::string>());
    }
    if (tmpOptionsStr.empty()) {
        tmpOptionsStr = autil::legacy::FastToJsonString(indexlibv2::framework::ImportExternalFileOptions());
    }
    auto status = indexlib::util::JsonUtil::FromString(tmpExternalFilesStr, &externalFiles);
    if (!status.IsOK()) {
        *errMsg = "external files [" + tmpExternalFilesStr + "] parse from string failed";
    }

    status = indexlib::util::JsonUtil::FromString(tmpOptionsStr, &options);
    if (!status.IsOK()) {
        *errMsg = "import external file options [" + tmpOptionsStr + "] parse from string failed";
    }

    if (tmpActionStr.empty()) {
        tmpActionStr = "add";
    }
    indexlibv2::framework::Action action = indexlibv2::framework::ActionConvertUtil::StrToAction(tmpActionStr);
    assert(action != indexlibv2::framework::Action::UNKNOWN);

    return std::make_tuple(externalFiles, options, action);
}

bool BuildTask::executeBulkload(const std::shared_ptr<BuildTaskTarget>& target)
{
    if (target == nullptr) {
        return false;
    }

    const auto& buildTaskInfo = target->getBuildTaskTargetInfo();
    if (buildTaskInfo.buildStep == config::BUILD_STEP_INC_STR) {
        for (const auto& request : buildTaskInfo.requestQueue) {
            if (request.GetTaskType() != indexlibv2::framework::BULKLOAD_TASK_TYPE) {
                continue;
            }
            auto params = request.GetParams();
            std::string bulkloadId = params[indexlibv2::framework::PARAM_BULKLOAD_ID];
            if (bulkloadId.empty()) {
                BS_LOG(ERROR, "invalid bulkload info, bulkload id is empty");
                return false;
            }
            std::string externalFilesStr = params[indexlibv2::framework::PARAM_EXTERNAL_FILES];
            std::string optionsStr = params[indexlibv2::framework::PARAM_IMPORT_EXTERNAL_FILE_OPTIONS];
            std::string actionStr = params[indexlibv2::framework::ACTION_KEY];
            std::string errMsg;
            auto [externalFiles, options, action] =
                prepareBulkloadParams(externalFilesStr, optionsStr, actionStr, &errMsg);
            if (!errMsg.empty()) {
                BS_LOG(ERROR, "%s", errMsg.c_str());
            }
            auto status =
                _tablet->ImportExternalFiles(bulkloadId, externalFiles, options, action, request.GetEventTimeInSecs());
            if (status.IsOK()) {
                BS_LOG(INFO, "bulkload external files success, bulkload id %s", bulkloadId.c_str());
            } else if (status.IsInvalidArgs()) {
                BS_LOG(ERROR, "bulkload failed, bulkload id %s, status: %s", bulkloadId.c_str(),
                       status.ToString().c_str());
            } else {
                _hasFatalError = true;
                BS_LOG(ERROR, "%s", status.ToString().c_str());
                return false;
            }
        }
    }
    return true;
}

void BuildTask::workLoop()
{
    std::lock_guard<std::mutex> lock(_tabletMutex);
    autil::ScopedTime2 timer;
    std::shared_ptr<BuildTaskTarget> target;
    {
        std::lock_guard<std::mutex> targetLock(_targetMutex);
        target = _target;
    }
    if (!target) {
        if (_tablet && _tablet->NeedCommit()) {
            BS_LOG(INFO, "begin workloop tablet need commit");
            proto::VersionInfo committedVersionInfo;
            if (!commitAndReopen(indexlibv2::INVALID_VERSIONID, target, std::nullopt, &committedVersionInfo)) {
                BS_LOG(ERROR, "commit and reopen failed.");
            }
            updateCurrent(committedVersionInfo, indexlibv2::INVALID_VERSIONID, target);
            updatePartitionDocCounter(_tablet);
        }
        if (timer.done_sec() > 10) {
            BS_LOG(WARN, "end workloop cost [%.1f]s", timer.done_sec());
        }
        return;
    }

    if (!executeBulkload(target)) {
        BS_LOG(ERROR, "execute bulkload failed.");
    }

    if (needManualMerge(target, _current)) {
        if (!executeManualMerge(target)) {
            BS_LOG(ERROR, "execute manual merge failed.");
        } else {
            _current->markManualMergeFinished();
            BS_LOG(INFO, "end manual merge.");
        }
    }

    auto alignVersionId = getAlignVersionId(target, _current);

    proto::VersionInfo committedVersionInfo; // = _current->getCommitedVersionInfo();/////
    versionid_t alignFailedVersionId = indexlibv2::INVALID_VERSIONID;
    if (_tablet->NeedCommit() || alignVersionId != indexlibv2::INVALID_VERSIONID) {
        BS_LOG(INFO, "tablet need commit");
        if (alignVersionId != indexlibv2::INVALID_VERSIONID && isVersionAlreadyCommitted(alignVersionId, target)) {
            alignFailedVersionId = alignVersionId;
        } else {
            if (!commitAndReopen(alignVersionId, target, std::nullopt, &committedVersionInfo)) {
                alignFailedVersionId = alignVersionId;
                BS_LOG(ERROR, "commit and reopen failed. version id");
            }
        }
    }
    if (_buildStep == config::BUILD_STEP_FULL_STR && _current->isManualMergeFinished() && !_isFullBuildIndexCleaned) {
        if (!cleanFullBuildIndex()) {
            BS_LOG(ERROR, "clean tmp build dir failed");
            _hasFatalError = true;
            handleFatalError("clean tmp build dir failed");
        } else {
            _isFullBuildIndexCleaned = true;
        }
    }
    updateCurrent(committedVersionInfo, alignFailedVersionId, target);
    updatePartitionDocCounter(_tablet);
    if (timer.done_sec() > 10) {
        BS_LOG(WARN, "end workloop cost [%.1f]s", timer.done_sec());
    }
    return;
}

bool BuildTask::isVersionAlreadyCommitted(versionid_t alignVersionId, const std::shared_ptr<BuildTaskTarget>& target)
{
    if (alignVersionId <= target->getLatestPublishedVersion()) {
        return true;
    }
    proto::VersionInfo committedVersionInfo = _current->getCommitedVersionInfo();
    if (committedVersionInfo.versionMeta.GetVersionId() != indexlibv2::INVALID_VERSIONID &&
        alignVersionId <= committedVersionInfo.versionMeta.GetVersionId()) {
        return true;
    }
    return false;
}

void BuildTask::collectErrorInfos() const
{
    std::vector<proto::ErrorInfo> errorInfos;
    if (_singleBuilder != nullptr) {
        _singleBuilder->fillErrorInfos(errorInfos);
    } else if (_builderController != nullptr) {
        _builderController->fillErrorInfos(errorInfos);
    }
    addErrorInfos(errorInfos);
}

bool BuildTask::isDone(const config::TaskTarget& target)
{
    auto buildTarget = std::make_shared<BuildTaskTarget>();
    if (!buildTarget->init(target)) {
        return false;
    }
    return _current->reachedTarget(buildTarget);
}

std::shared_ptr<indexlib::util::CounterMap> BuildTask::getCounterMap()
{
    if (_singleBuilder != nullptr) {
        return _singleBuilder->getCounterMap();
    }
    if (_tablet) {
        return _tablet->GetTabletInfos()->GetCounterMap();
    }
    return nullptr;
}

void BuildTask::updateCurrent(const proto::VersionInfo& committedVersionInfo, const versionid_t alignFailedVersionId,
                              const std::shared_ptr<BuildTaskTarget>& target)
{
    collectErrorInfos();
    std::lock_guard<std::mutex> lock(_taskInfoMutex);
    auto tablet = _tablet;
    if (!tablet) {
        return;
    }
    bool buildFinished = isBuildFinished(target);
    _current->updateCurrent(committedVersionInfo, alignFailedVersionId, buildFinished, target);
}

bool BuildTask::isBuildFinished(const std::shared_ptr<BuildTaskTarget>& target)
{
    if (!target) {
        return false;
    }
    if (target->isFinished()) {
        return true;
    }
    if (_buildMode & proto::BUILD) {
        if (_singleBuilder && _singleBuilder->isDone() && !_tablet->NeedCommit()) {
            return true;
        }
        return false;
    }

    assert(_buildMode == proto::PUBLISH);
    return false;
}

std::string BuildTask::getTaskStatus()
{
    std::lock_guard<std::mutex> lock(_taskInfoMutex);
    return _current->getTaskStatus();
}

proto::ProgressStatus BuildTask::getTaskProgress() const
{
    static const proto::ProgressStatus progressStatus;
    return progressStatus;
}

void BuildTask::handleFatalError(const std::string& message)
{
    if (!hasFatalError()) {
        return;
    }
    Task::handleFatalError(message);
}

indexlib::Status BuildTask::loadVersion(const std::string& indexRoot,
                                        const indexlibv2::framework::VersionCoord& versionCoord,
                                        indexlibv2::framework::Version* version) const
{
    auto versionId = versionCoord.GetVersionId();
    if (versionId == indexlib::INVALID_VERSIONID) {
        return indexlib::Status::OK();
    }
    auto versionRoot = indexRoot;
    if (!versionCoord.GetVersionFenceName().empty()) {
        versionRoot = indexlib::util::PathUtil::JoinPath(versionRoot, versionCoord.GetVersionFenceName());
    }
    auto versionRootDir = indexlib::file_system::Directory::GetPhysicalDirectory(versionRoot);
    return indexlibv2::framework::VersionLoader::GetVersion(versionRootDir, versionId, version);
}

} // namespace build_service::build_task
