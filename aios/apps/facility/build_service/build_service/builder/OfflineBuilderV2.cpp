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
#include "build_service/builder/OfflineBuilderV2.h"

#include "autil/MemUtil.h"
#include "build_service/builder/BuilderV2Impl.h"
#include "fslib/fs/FileSystem.h"
#include "future_lite/ExecutorCreator.h"
#include "future_lite/TaskScheduler.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/MemoryQuotaController.h"
#include "indexlib/config/BuildConfig.h"
#include "indexlib/config/OfflineConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/framework/TabletCreator.h"
#include "indexlib/framework/TabletId.h"
#include "indexlib/framework/VersionCoord.h"
#include "indexlib/framework/VersionLoader.h"
#include "indexlib/table/index_task/LocalTabletMergeController.h"
#include "indexlib/util/metrics/MetricProvider.h"

CHECK_FUTURE_LITE_EXECUTOR(async_io);

namespace build_service::builder {
BS_LOG_SETUP(builder, OfflineBuilderV2);

OfflineBuilderV2::OfflineBuilderV2(const std::string& clusterName, const proto::PartitionId& partitionId,
                                   const config::ResourceReaderPtr& resourceReader, const std::string& indexRoot)
    : BuilderV2(partitionId.buildid())
    , _clusterName(clusterName)
    , _resourceReader(resourceReader)
    , _indexRoot(indexRoot)
    , _partitionId(partitionId)
{
}

OfflineBuilderV2::~OfflineBuilderV2()
{
    _impl.reset();
    _taskScheduler.reset();
    _executor.reset();
}

bool OfflineBuilderV2::init(const config::BuilderConfig& builderConfig,
                            std::shared_ptr<indexlib::util::MetricProvider> metricProvider)
{
    auto tabletOptions = _resourceReader->getTabletOptions(_clusterName);
    if (!tabletOptions) {
        BS_LOG(ERROR, "read tablet options for %s failed", _clusterName.c_str());
        return false;
    }
    tabletOptions->SetAutoMerge(false);
    tabletOptions->SetIsLeader(true);
    tabletOptions->SetFlushRemote(true);
    tabletOptions->SetFlushLocal(false);
    tabletOptions->SetIsOnline(false);

    _executor = future_lite::ExecutorCreator::Create(
        /*type*/ "async_io",
        future_lite::ExecutorCreator::Parameters()
            .SetExecutorName("tablet_dump" + autil::StringUtil::toString(_partitionId.range().from()) + "_" +
                             autil::StringUtil::toString(_partitionId.range().to()))
            .SetThreadNum(tabletOptions->GetOfflineConfig().GetBuildConfig().GetDumpThreadCount()));

    _taskScheduler = std::make_unique<future_lite::TaskScheduler>(_executor.get());

    auto schema = _resourceReader->getTabletSchema(_clusterName);
    if (!schema) {
        BS_LOG(ERROR, "read tablet schema for %s failed", _clusterName.c_str());
        return false;
    }

    int64_t totalMemory = autil::MemUtil::getMachineTotalMem();
    static constexpr float BUILD_MEMORY_RATIO = 0.9f; // TODO: overwrite from config or env
    totalMemory = int64_t(totalMemory * BUILD_MEMORY_RATIO);
    auto memoryController = std::make_shared<indexlibv2::MemoryQuotaController>("offline_build", totalMemory);

    if (metricProvider != nullptr) {
        _metricProvider = metricProvider;
    }
    std::shared_ptr<indexlibv2::framework::ITabletMergeController> mergeController =
        createMergeController(schema, tabletOptions);
    auto tablet = indexlibv2::framework::TabletCreator()
                      .SetTabletId(indexlib::framework::TabletId(_clusterName))
                      .SetExecutor(_executor.get(), _taskScheduler.get())
                      .SetMemoryQuotaController(memoryController, nullptr)
                      .SetMetricsReporter(_metricProvider ? metricProvider->GetReporter() : nullptr)
                      .SetMergeController(mergeController)
                      .CreateTablet();

    if (!tablet) {
        BS_LOG(ERROR, "create tablet failed for cluster %s", _clusterName.c_str());
        return false;
    }

    indexlibv2::framework::IndexRoot indexRoot(_indexRoot, _indexRoot);
    indexlibv2::framework::VersionCoord versionCoord(indexlibv2::INVALID_VERSIONID, "");

    fslib::ErrorCode ec = fslib::fs::FileSystem::isExist(_indexRoot);
    if (ec == fslib::EC_TRUE) {
        auto [st, vs] = getLatestVersion();
        if (!st.IsOK()) {
            BS_LOG(ERROR, "get latest version in path %s failed", _indexRoot.c_str());
            return false;
        }
        versionCoord = vs;
    }
    auto s = tablet->Open(indexRoot, schema, std::move(tabletOptions), versionCoord);
    if (!s.IsOK()) {
        BS_LOG(ERROR, "open tablet for cluster[%s] with indexRoot[%s] failed, error: %s", _clusterName.c_str(),
               _indexRoot.c_str(), s.ToString().c_str());
        return false;
    }
    _impl = std::make_unique<BuilderV2Impl>(std::move(tablet), _buildId);
    if (!_impl->init(builderConfig, std::move(metricProvider))) {
        BS_LOG(ERROR, "init builder impl failed for cluster: %s", _clusterName.c_str());
        return false;
    }
    return true;
}

std::unique_ptr<future_lite::Executor> OfflineBuilderV2::createExecutor(const std::string& executorName,
                                                                        uint32_t threadCount) const
{
    return future_lite::ExecutorCreator::Create(
        /*type*/ "async_io",
        future_lite::ExecutorCreator::Parameters().SetExecutorName(executorName).SetThreadNum(threadCount));
}

std::shared_ptr<indexlibv2::framework::ITabletMergeController>
OfflineBuilderV2::createMergeController(const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema,
                                        const std::shared_ptr<indexlibv2::config::TabletOptions>& options)
{
    if (!_localMergeExecutor) {
        std::string mergeExecutorName = "merge_controller_" + autil::StringUtil::toString(_partitionId.range().from()) +
                                        "_" + autil::StringUtil::toString(_partitionId.range().to());
        _localMergeExecutor = createExecutor(mergeExecutorName, 1);
    }
    assert(_localMergeExecutor != nullptr);
    indexlibv2::table::LocalTabletMergeController::InitParam param;
    param.executor = _localMergeExecutor.get();
    param.schema = schema;
    param.options = options;
    param.buildTempIndexRoot = _indexRoot;
    param.partitionIndexRoot = _indexRoot;
    param.metricProvider = _metricProvider;

    auto controller = std::make_shared<indexlibv2::table::LocalTabletMergeController>();
    auto status = controller->Init(param);
    if (!status.IsOK()) {
        BS_LOG(ERROR, "create local merge controller failed: %s", status.ToString().c_str());
        return nullptr;
    }
    return controller;
}

std::pair<indexlib::Status, indexlibv2::framework::VersionCoord> OfflineBuilderV2::getLatestVersion() const
{
    auto indexDir = indexlib::file_system::Directory::GetPhysicalDirectory(_indexRoot);

    fslib::FileList fileList;
    auto status = indexlibv2::framework::VersionLoader::ListVersion(indexDir, &fileList);
    if (!status.IsOK()) {
        BS_LOG(ERROR, "list version in path %s failed", _indexRoot.c_str());
        return {indexlib::Status::InternalError(),
                indexlibv2::framework::VersionCoord(indexlibv2::INVALID_VERSIONID, "")};
    }
    if (!fileList.empty()) {
        const auto& versionFileName = *fileList.rbegin();
        auto [st, version] =
            indexlibv2::framework::VersionLoader::LoadVersion(indexDir->GetIDirectory(), versionFileName);
        if (!st.IsOK()) {
            BS_LOG(ERROR, "load version %s in path %s failed", versionFileName.c_str(), _indexRoot.c_str());
            return {indexlib::Status::InternalError(),
                    indexlibv2::framework::VersionCoord(indexlibv2::INVALID_VERSIONID, "")};
        }
        BS_LOG(INFO, "load version %s in path %s", versionFileName.c_str(), _indexRoot.c_str());
        return {indexlib::Status::OK(),
                indexlibv2::framework::VersionCoord(version->GetVersionId(), version->GetFenceName())};
    }
    BS_LOG(INFO, "no version in path %s, will open invalid version", _indexRoot.c_str());
    return {indexlib::Status::OK(), indexlibv2::framework::VersionCoord(indexlibv2::INVALID_VERSIONID, "")};
}

bool OfflineBuilderV2::build(const std::shared_ptr<indexlibv2::document::IDocumentBatch>& batch)
{
    return _impl->build(batch);
}

bool OfflineBuilderV2::merge() { return _impl->merge(); }

void OfflineBuilderV2::stop(std::optional<int64_t> stopTimestamp, bool needSeal, bool immediately)
{
    // maybe always setSealed for offline build
    _impl->stop(stopTimestamp, needSeal, immediately);
    auto commitOptions = indexlibv2::framework::CommitOptions().SetNeedPublish(true).SetNeedReopenInCommit(true);
    commitOptions.AddVersionDescription("generation", autil::StringUtil::toString(_buildId.generationid()));
    auto [s, version] = _impl->commit(commitOptions);
    if (!s.IsOK()) {
        BS_LOG(ERROR, "commit version failed, error: %s", s.ToString().c_str());
        setFatalError();
        return;
    } else {
        BS_LOG(INFO, "commit version is: %s", ToJsonString(version).c_str());
    }
}

int64_t OfflineBuilderV2::getIncVersionTimestamp() const { return _impl->getIncVersionTimestamp(); }

indexlibv2::framework::Locator OfflineBuilderV2::getLastLocator() const { return _impl->getLastLocator(); }
indexlibv2::framework::Locator OfflineBuilderV2::getLatestVersionLocator() const
{
    return _impl->getLatestVersionLocator();
}
indexlibv2::framework::Locator OfflineBuilderV2::getLastFlushedLocator() const
{
    return _impl->getLastFlushedLocator();
}
bool OfflineBuilderV2::hasFatalError() const { return _impl->hasFatalError(); }
bool OfflineBuilderV2::needReconstruct() const { return _impl->needReconstruct(); }
bool OfflineBuilderV2::isSealed() const { return _impl->isSealed(); }
void OfflineBuilderV2::setFatalError() { return _impl->setFatalError(); }
std::shared_ptr<indexlib::util::CounterMap> OfflineBuilderV2::GetCounterMap() const { return _impl->GetCounterMap(); }
void OfflineBuilderV2::switchToConsistentMode() {} /* do nothing */

} // namespace build_service::builder
