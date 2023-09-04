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
#include "build_service/local_job/MergeInstanceWorkItemV2.h"

#include "autil/EnvUtil.h"
#include "autil/MemUtil.h"
#include "build_service/config/BuilderConfig.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/config/OfflineIndexConfigMap.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/config/TaskConfig.h"
#include "build_service/util/IndexPathConstructor.h"
#include "build_service/util/ParallelIdGenerator.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/util/FileUtil.h"
#include "future_lite/ExecutorCreator.h"
#include "indexlib/base/PathUtil.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/JsonUtil.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/framework/ITabletFactory.h"
#include "indexlib/framework/IdGenerator.h"
#include "indexlib/framework/TabletCreator.h"
#include "indexlib/framework/TabletFactoryCreator.h"
#include "indexlib/framework/TabletId.h"
#include "indexlib/framework/TabletInfos.h"
#include "indexlib/framework/TabletSchemaLoader.h"
#include "indexlib/framework/Version.h"
#include "indexlib/framework/VersionCoord.h"
#include "indexlib/framework/VersionLoader.h"
#include "indexlib/table/index_task/IndexTaskConstant.h"
#include "indexlib/table/index_task/LocalTabletMergeController.h"
#include "indexlib/util/PathUtil.h"

namespace build_service::local_job {

BS_LOG_SETUP(local_job, MergeInstanceWorkItemV2);

void MergeInstanceWorkItemV2::process()
{
    if (!task_base::TaskBase::init(_jobParams)) {
        setFailFlag();
        BS_LOG(ERROR, "invalid job param[%s]", _jobParams.c_str());
        return;
    }
    // BUILD_MODE 根据mode指定 indexRoot & merge strategy
    _buildMode = getValueFromKeyValueMap(_kvMap, "build_mode");

    proto::PartitionId partitionId = createPartitionId(_instanceId, _mode, "merger");
    const std::string indexPath = getValueFromKeyValueMap(_kvMap, config::INDEX_ROOT_PATH);
    const std::string finalIndexPath = getValueCopyFromKeyValueMap(_kvMap, "final_index_root");
    _indexRoot = util::IndexPathConstructor::constructIndexPath(indexPath, partitionId);
    _finalIndexRoot = util::IndexPathConstructor::constructIndexPath(finalIndexPath, partitionId);

    indexlibv2::framework::Version version;
    auto st = getLatestVersion(&version);
    if (!st.IsOK()) {
        setFailFlag();
        return;
    }
    auto tablet = prepareTablet();
    if (tablet == nullptr) {
        setFailFlag();
        BS_LOG(ERROR, "prepare tablet failed");
        return;
    }
    indexlibv2::framework::IndexRoot finalIndexRoot(_finalIndexRoot, _finalIndexRoot);
    indexlibv2::framework::VersionCoord versionCoord(indexlibv2::INVALID_VERSIONID, "");
    st = tablet->Open(finalIndexRoot, _tabletSchema, _tabletOptions, versionCoord);
    if (!st.IsOK()) {
        setFailFlag();
        BS_LOG(ERROR, "tablet open failed");
        return;
    }

    const std::string taskType = indexlibv2::table::MERGE_TASK_TYPE;
    const std::string taskName = indexlibv2::table::DESIGNATE_FULL_MERGE_TASK_NAME;

    std::map<std::string, std::string> params;
    if (_buildMode == "full") {
        params[indexlibv2::table::IS_OPTIMIZE_MERGE] = "true";
    }

    auto [status, _] = tablet->ExecuteTask(version, taskType, taskName, params);
    if (!status.IsOK()) {
        setFailFlag();
        BS_LOG(ERROR, "execute task [%s:%s] failed, base version[%d]", taskType.c_str(), taskName.c_str(),
               version.GetVersionId());
        return;
    }
    BS_LOG(INFO, "success merge, index path[%s]", _finalIndexRoot.c_str());
    st = tablet->Commit(indexlibv2::framework::CommitOptions().SetNeedPublish(true)).first;
    if (!st.IsOK()) {
        setFailFlag();
        BS_LOG(ERROR, "tablet commit failed");
    }
    return;
}

void MergeInstanceWorkItemV2::setFailFlag() { *_failFlag = true; }

indexlib::Status MergeInstanceWorkItemV2::getLatestVersion(indexlibv2::framework::Version* version) const
{
    std::string sourceVersionRoot;
    if (_buildMode == "full") {
        sourceVersionRoot = _indexRoot;
    } else if (_buildMode == "incremental") {
        sourceVersionRoot = _finalIndexRoot;
    } else {
        return indexlib::Status::InternalError("invalid build mode");
    }
    auto indexDir = indexlib::file_system::Directory::GetPhysicalDirectory(sourceVersionRoot);

    fslib::FileList fileList;
    auto st = indexlibv2::framework::VersionLoader::ListVersion(indexDir, &fileList);
    RETURN_IF_STATUS_ERROR(st, "list version in path [%s] failed.", sourceVersionRoot.c_str());
    const auto& versionFileName = *fileList.rbegin();
    st = indexlibv2::framework::VersionLoader::LoadVersion(indexDir, versionFileName, version);
    RETURN_IF_STATUS_ERROR(st, "load version [%s:%s] failed.", sourceVersionRoot.c_str(), versionFileName.c_str());

    return indexlib::Status::OK();
}

std::unique_ptr<future_lite::Executor> MergeInstanceWorkItemV2::createExecutor(const std::string& executorName,
                                                                               uint32_t threadCount) const
{
    return future_lite::ExecutorCreator::Create(
        /*type*/ "async_io",
        future_lite::ExecutorCreator::Parameters().SetExecutorName(executorName).SetThreadNum(threadCount));
}

int64_t MergeInstanceWorkItemV2::getMachineTotalMemoryMb() const
{
    int64_t totalMemoryBytes = autil::MemUtil::getMachineTotalMem();
    static constexpr double defaultAllowRatio = 0.95;
    auto allowRatio = autil::EnvUtil::getEnv<double>("bs_build_task_memory_allow_ratio", defaultAllowRatio);
    if (allowRatio <= 0 or allowRatio > 1.0) {
        BS_LOG(WARN, "bs_build_task_memory_allow_ratio [%lf] is not valid, set to %lf", allowRatio, defaultAllowRatio);
        allowRatio = defaultAllowRatio;
    }
    totalMemoryBytes = int64_t(totalMemoryBytes * allowRatio);
    return totalMemoryBytes / 1024 / 1024;
}

bool MergeInstanceWorkItemV2::createQuotaController(int64_t buildTotalMemory)
{
    auto clusterName = _jobConfig.getClusterName();
    const std::string clusterPath = _resourceReader->getClusterConfRelativePath(clusterName);
    config::BuilderConfig builderConfig;
    if (!_resourceReader->getConfigWithJsonPath(clusterPath, "build_option_config", builderConfig)) {
        BS_LOG(ERROR, "parse build_option_config failed, cluster:[%s]", clusterName.c_str());
        return false;
    }

    int64_t totalMemory = getMachineTotalMemoryMb();
    if (totalMemory < buildTotalMemory) {
        buildTotalMemory = totalMemory;
    }

    _totalMemoryController =
        std::make_shared<indexlibv2::MemoryQuotaController>("offline_total", buildTotalMemory * 1024 * 1024);
    _buildMemoryController =
        std::make_shared<indexlibv2::MemoryQuotaController>("offline_build", buildTotalMemory * 1024 * 1024);
    BS_LOG(INFO, "offline build, total memory quota[%ld], build memory quota[%ld]", buildTotalMemory, buildTotalMemory);
    return true;
}

bool MergeInstanceWorkItemV2::prepareResource()
{
    auto clusterName = _jobConfig.getClusterName();
    const std::string clusterConfig = _resourceReader->getClusterConfRelativePath(clusterName);

    uint32_t executorThreadCount = 1;
    uint32_t dumpThreadCount = 1;
    int64_t buildTotalMemory = getMachineTotalMemoryMb();
    if (!_resourceReader->getConfigWithJsonPath(
            clusterConfig, "offline_index_config.build_config.executor_thread_count", executorThreadCount)) {
        BS_LOG(ERROR, "read offline_index_config.build_config failed, cluster [%s]", clusterName.c_str());
        return false;
    }
    if (!_resourceReader->getConfigWithJsonPath(clusterConfig, "offline_index_config.build_config.dump_thread_count",
                                                dumpThreadCount)) {
        BS_LOG(ERROR, "read offline_index_config.build_config failed, cluster [%s]", clusterName.c_str());
        return false;
    }
    if (!_resourceReader->getConfigWithJsonPath(clusterConfig, "offline_index_config.build_config.build_total_memory",
                                                buildTotalMemory)) {
        BS_LOG(ERROR, "read offline_index_config.build_config failed, cluster [%s]", clusterName.c_str());
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
    return true;
}

std::shared_ptr<indexlibv2::framework::ITablet> MergeInstanceWorkItemV2::prepareTablet()
{
    if (!prepareResource()) {
        BS_LOG(ERROR, "prepare resource failed.");
        return nullptr;
    }
    const std::string clusterName = _jobConfig.getClusterName();
    auto tabletOptions = _resourceReader->getTabletOptions(clusterName);
    if (tabletOptions == nullptr) {
        BS_LOG(ERROR, "init tablet options [%s] failed", clusterName.c_str());
        return nullptr;
    }

    tabletOptions->SetAutoMerge(false);
    tabletOptions->SetIsOnline(false);
    tabletOptions->SetFlushRemote(true);
    tabletOptions->SetFlushLocal(false);
    tabletOptions->SetIsLeader(true);

    _tabletSchema = getTabletSchema(tabletOptions);
    assert(_tabletSchema != nullptr);
    _tabletOptions = tabletOptions;

    std::shared_ptr<kmonitor::MetricsReporter> reporter;
    if (_metricProvider != nullptr) {
        reporter = _metricProvider->GetReporter();
    }

    std::shared_ptr<indexlibv2::framework::ITabletMergeController> mergeController = createMergeController();
    if (mergeController == nullptr) {
        BS_LOG(ERROR, "get merge controller failed");
        return nullptr;
    }

    auto tablet = indexlibv2::framework::TabletCreator()
                      .SetTabletId(indexlib::framework::TabletId(clusterName))
                      .SetExecutor(_dumpExecutor.get(), _taskScheduler.get())
                      .SetMemoryQuotaController(_totalMemoryController, _buildMemoryController)
                      .SetMetricsReporter(reporter)
                      .SetMergeController(mergeController)
                      .SetIdGenerator(getIdGenerator())
                      .CreateTablet();
    return tablet;
}

std::shared_ptr<indexlibv2::framework::IdGenerator> MergeInstanceWorkItemV2::getIdGenerator() const
{
    auto versionMaskType = indexlibv2::IdMaskType::BUILD_PUBLIC;
    auto idGenerator =
        std::make_shared<util::ParallelIdGenerator>(indexlibv2::IdMaskType::BUILD_PUBLIC, versionMaskType);
    auto status = idGenerator->Init(0, 1);
    if (!status.IsOK()) {
        BS_LOG(ERROR, "init id generator failed.");
        return nullptr;
    }
    return idGenerator;
}

std::shared_ptr<indexlibv2::framework::ITabletMergeController> MergeInstanceWorkItemV2::createMergeController()
{
    if (!_localMergeExecutor) {
        _localMergeExecutor = createExecutor("merge_controller", 1);
    }
    assert(_localMergeExecutor != nullptr);
    indexlibv2::table::LocalTabletMergeController::InitParam param;
    param.executor = _localMergeExecutor.get();
    param.schema = _tabletSchema;
    param.options = _tabletOptions;
    param.buildTempIndexRoot = _buildMode == "full" ? _indexRoot : _finalIndexRoot;
    param.partitionIndexRoot = _finalIndexRoot;
    param.metricProvider = _metricProvider;

    auto controller = std::make_shared<indexlibv2::table::LocalTabletMergeController>();
    auto status = controller->Init(param);
    if (!status.IsOK()) {
        BS_LOG(ERROR, "create local merge controller failed: %s", status.ToString().c_str());
        return nullptr;
    }
    return controller;
}

std::shared_ptr<indexlibv2::config::ITabletSchema>
MergeInstanceWorkItemV2::getTabletSchema(const std::shared_ptr<indexlibv2::config::TabletOptions>& tabletOptions) const
{
    auto clusterName = _jobConfig.getClusterName();
    auto tabletSchema = _resourceReader->getTabletSchema(clusterName);
    if (tabletSchema == nullptr) {
        BS_LOG(ERROR, "get schema for cluster[%s] failed", clusterName.c_str());
        return nullptr;
    }

    auto status =
        indexlibv2::framework::TabletSchemaLoader::ResolveSchema(tabletOptions, _indexRoot, tabletSchema.get());
    if (!status.IsOK()) {
        BS_LOG(ERROR, "resolve schema failed: %s", status.ToString().c_str());
        return nullptr;
    }

    return tabletSchema;
}

} // namespace build_service::local_job
