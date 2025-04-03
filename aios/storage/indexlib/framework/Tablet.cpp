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
#include "indexlib/framework/Tablet.h"

#include <algorithm>
#include <assert.h>
#include <chrono>
#include <exception>
#include <limits>
#include <ostream>
#include <stddef.h>
#include <type_traits>

#include "autil/CommonMacros.h"
#include "autil/EnvUtil.h"
#include "autil/Scope.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/UnitUtil.h"
#include "autil/WorkItemQueue.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "autil/legacy/legacy_jsonizable_dec.h"
#include "future_lite/TaskScheduler.h"
#include "future_lite/Try.h"
#include "future_lite/coro/Lazy.h"
#include "future_lite/coro/LazyHelper.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/MemoryQuotaController.h"
#include "indexlib/base/MemoryQuotaSynchronizer.h"
#include "indexlib/base/PathUtil.h"
#include "indexlib/config/BackgroundTaskConfig.h"
#include "indexlib/config/BuildConfig.h"
#include "indexlib/config/FieldConfig.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/config/IndexTaskConfig.h"
#include "indexlib/config/OnlineConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/document/IDocumentBatch.h"
#include "indexlib/document/IDocumentParser.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FSResult.h"
#include "indexlib/file_system/FileBlockCacheContainer.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/FileSystemMetricsReporter.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/LifecycleConfig.h"
#include "indexlib/file_system/LifecycleTable.h"
#include "indexlib/file_system/MountOption.h"
#include "indexlib/file_system/ReaderOption.h"
#include "indexlib/file_system/fslib/FenceContext.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/load_config/LoadConfigList.h"
#include "indexlib/framework/BuildResource.h"
#include "indexlib/framework/DefaultMemoryControlStrategy.h"
#include "indexlib/framework/DeployIndexUtil.h"
#include "indexlib/framework/DiskSegment.h"
#include "indexlib/framework/Fence.h"
#include "indexlib/framework/IMemoryControlStrategy.h"
#include "indexlib/framework/IMetrics.h"
#include "indexlib/framework/ITabletImporter.h"
#include "indexlib/framework/ITabletLoader.h"
#include "indexlib/framework/IndexRecoverStrategy.h"
#include "indexlib/framework/MemSegmentCreator.h"
#include "indexlib/framework/MetricsManager.h"
#include "indexlib/framework/ResourceMap.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/SegmentDumper.h"
#include "indexlib/framework/SegmentInfo.h"
#include "indexlib/framework/SegmentMeta.h"
#include "indexlib/framework/TabletCommitter.h"
#include "indexlib/framework/TabletDataSchemaGroup.h"
#include "indexlib/framework/TabletDumper.h"
#include "indexlib/framework/TabletFactoryCreator.h"
#include "indexlib/framework/TabletId.h"
#include "indexlib/framework/TabletInfos.h"
#include "indexlib/framework/TabletReader.h"
#include "indexlib/framework/TabletReaderContainer.h"
#include "indexlib/framework/TabletSchemaLoader.h"
#include "indexlib/framework/TabletSchemaManager.h"
#include "indexlib/framework/TaskType.h"
#include "indexlib/framework/VersionDeployDescription.h"
#include "indexlib/framework/VersionLine.h"
#include "indexlib/framework/VersionLoader.h"
#include "indexlib/framework/VersionMetaCreator.h"
#include "indexlib/framework/cleaner/DropIndexCleaner.h"
#include "indexlib/framework/cleaner/OnDiskIndexCleaner.h"
#include "indexlib/framework/cleaner/ResourceCleaner.h"
#include "indexlib/framework/index_task/Constant.h"
#include "indexlib/framework/lifecycle/LifecycleTableCreator.h"
#include "indexlib/framework/mem_reclaimer/EpochBasedMemReclaimer.h"
#include "indexlib/framework/mem_reclaimer/MemReclaimerMetrics.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/KeyValueMap.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/cache/SearchCachePartitionWrapper.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"
#include "indexlib/util/memory_control/MemoryReserver.h"
#include "indexlib/util/metrics/Metric.h"
#include "indexlib/util/metrics/MetricProvider.h"

namespace indexlibv2::framework {

AUTIL_LOG_SETUP(indexlib.framework, Tablet);

#define TABLET_LOG(level, format, args...)                                                                             \
    AUTIL_LOG(level, "[%s] [%p] " format, _tabletInfos->GetTabletName().c_str(), this, ##args)

#define TABLET_INTERVAL_LOG2(logInterval, level, format, args...)                                                      \
    AUTIL_INTERVAL_LOG2(logInterval, level, "[%s] [%p] " format, _tabletInfos->GetTabletName().c_str(), this, ##args)

Tablet::Tablet(const TabletResource& resource)
    : _tabletInfos(std::make_unique<TabletInfos>())
    , _needSeek(false)
    , _tabletCommitter(std::make_unique<TabletCommitter>(resource.tabletId.GenerateTabletName()))
    , _tabletDumper(std::make_unique<TabletDumper>(resource.tabletId.GenerateTabletName(), resource.dumpExecutor,
                                                   _tabletCommitter.get()))
    , _mergeController(resource.mergeController)
    , _idGenerator(resource.idGenerator)
    , _isClosed(false)
{
    _tabletInfos->SetTabletId(resource.tabletId);
    if (resource.memoryQuotaController) {
        _tabletMemoryQuotaController = std::make_shared<MemoryQuotaController>(resource.tabletId.GenerateTabletName(),
                                                                               resource.memoryQuotaController);
    } else {
        _tabletMemoryQuotaController = std::make_shared<MemoryQuotaController>(
            resource.tabletId.GenerateTabletName(), /*totalQuota=*/std::numeric_limits<int64_t>::max());
    }
    if (resource.buildMemoryQuotaController) {
        auto buildMemoryQuotaController = std::make_shared<MemoryQuotaController>(
            resource.tabletId.GenerateTabletName() + "_build", resource.buildMemoryQuotaController);
        _buildMemoryQuotaSynchronizer = std::make_unique<MemoryQuotaSynchronizer>(buildMemoryQuotaController);
    }
    if (resource.taskScheduler) {
        _taskScheduler = std::make_unique<future_lite::NamedTaskScheduler>(resource.taskScheduler);
    }
    if (resource.fileBlockCacheContainer) {
        _fileBlockCacheContainer = resource.fileBlockCacheContainer;
    } else {
        _fileBlockCacheContainer = std::make_shared<indexlib::file_system::FileBlockCacheContainer>();
        _fileBlockCacheContainer->Init(/*configStr=*/"", /*globalMemoryQuotaController=*/nullptr,
                                       /*taskScheduler=*/nullptr,
                                       /*metricProvider=*/nullptr);
    }
    if (resource.searchCache) {
        _searchCache = std::make_shared<indexlib::util::SearchCachePartitionWrapper>(
            resource.searchCache, resource.tabletId.GenerateTabletName());
    }
    _metricsManager.reset(new MetricsManager(resource.tabletId.GenerateTabletName(), resource.metricsReporter));
    // now we only support epoch base mem reclaimer
    if (_enableMemReclaimer) {
        auto memReclaimerMetrics = std::dynamic_pointer_cast<MemReclaimerMetrics>(
            _metricsManager->CreateMetrics("MEM_RECLAIMER", [&]() -> std::shared_ptr<framework::IMetrics> {
                return std::make_shared<MemReclaimerMetrics>(_metricsManager->GetMetricsReporter());
            }));
        assert(nullptr != memReclaimerMetrics);
        _memReclaimer.reset(new EpochBasedMemReclaimer(memReclaimerMetrics));
    }
    if (resource.consistentModeBuildThreadPool != nullptr) {
        _consistentModeBuildThreadPool = resource.consistentModeBuildThreadPool;
    }
    if (resource.inconsistentModeBuildThreadPool != nullptr) {
        _inconsistentModeBuildThreadPool = resource.inconsistentModeBuildThreadPool;
    }

    auto range = _tabletInfos->GetTabletId().GetRange();
    TABLET_LOG(INFO, "create tablet end, ipPort[%s:%u], range[%u_%u]", _tabletInfos->GetTabletId().GetIp().c_str(),
               _tabletInfos->GetTabletId().GetPort(), range.first, range.second);
}

Tablet::~Tablet() { Close(); }

std::shared_ptr<ITabletReader> Tablet::GetTabletReader() const
{
    std::lock_guard<std::mutex> guard(_readerMutex);
    return _tabletFactory->CreateTabletSessionReader(_tabletReader, _memReclaimer);
}

const TabletInfos* Tablet::GetTabletInfos() const { return _tabletInfos.get(); }

std::shared_ptr<config::ITabletSchema> Tablet::GetTabletSchema() const
{
    auto tabletData = GetTabletData();
    if (tabletData == nullptr) {
        return nullptr;
    }
    return tabletData->GetWriteSchema();
}

std::shared_ptr<config::TabletOptions> Tablet::GetTabletOptions() const { return _tabletOptions; }

Status
Tablet::OpenWriterAndReader(std::shared_ptr<TabletData> tabletData, const framework::OpenOptions& openOptions,
                            const std::shared_ptr<indexlibv2::framework::VersionDeployDescription>& versionDpDesc)
{
    // create and set reader
    ReadResource readResource = GenerateReadResource();
    auto readSchema = GetReadSchema(tabletData);
    auto tabletReader = _tabletFactory->CreateTabletReader(readSchema);
    if (_tabletOptions->IsOnline()) {
        auto versionId = tabletData->GetOnDiskVersion().GetVersionId();
        TABLET_LOG(INFO, "begin open tablet reader, version id[%d]", versionId);
        auto st = tabletReader->Open(tabletData, readResource);
        if (!st.IsOK()) {
            TABLET_LOG(ERROR, "tablet reader open failed");
            return st;
        }
    } else {
        TABLET_LOG(INFO, "skip open tablet reader for offline");
    }
    {
        std::lock_guard<std::mutex> guard(_readerMutex);
        _tabletReader = std::move(tabletReader);
        _tabletReaderContainer->AddTabletReader(tabletData, _tabletReader, versionDpDesc);
    }
    TABLET_LOG(INFO, "end open tablet reader");

    // create and set writer
    if (unlikely(_tabletOptions->IsReadOnly())) {
        TABLET_LOG(INFO, "table is readonly not open writer");
    } else {
        TABLET_LOG(INFO, "begin open tablet writer, version id[%d]", tabletData->GetOnDiskVersion().GetVersionId());
        decltype(_tabletWriter) tabletWriter;
        auto writeSchema = tabletData->GetWriteSchema();
        assert(writeSchema);
        auto slice = tabletData->CreateSlice(Segment::SegmentStatus::ST_BUILDING);
        if (!slice.empty()) {
            BuildResource buildResource = GenerateBuildResource(COUNTER_PREFIX);
            tabletWriter = _tabletFactory->CreateTabletWriter(writeSchema);
            Status st = tabletWriter->Open(tabletData, buildResource, openOptions);
            if (!st.IsOK()) {
                TABLET_LOG(ERROR, "tablet writer open failed");
                return st;
            }
        } else {
            auto currentLocator = GetTabletData()->GetLocator();
            assert(!_tabletOptions->IsOnline() || currentLocator == GetTabletInfos()->GetLatestLocator());
            if (_tabletOptions->IsOnline() && currentLocator.IsValid() && currentLocator != tabletData->GetLocator()) {
                TABLET_LOG(WARN, "drop realtime, request reseek upstream, current locator[%s], new locator[%s]",
                           currentLocator.DebugString().c_str(), tabletData->GetLocator().DebugString().c_str());
                _tabletInfos->SetBuildLocator(Locator());
                _needSeek.store(true);
            }
        }
        CloseWriterUnsafe();
        _tabletWriter = std::move(tabletWriter);
        TABLET_LOG(INFO, "end open tablet writer");
    }

    SetTabletData(std::move(tabletData));
    _tabletInfos->SetLastReopenLocator(GetTabletData()->GetLocator());
    UpdateDocCount();
    ReportMetrics(_tabletWriter);
    return Status::OK();
}

Status Tablet::ReopenNewSegment(const std::shared_ptr<config::ITabletSchema>& schema)
{
    return RefreshTabletData(RefreshStrategy::NEW_BUILDING_SEGMENT, schema);
}

Status Tablet::RefreshTabletData(RefreshStrategy strategy, const std::shared_ptr<config::ITabletSchema>& writeSchema)
{
    // assert dataMutex is held
    indexlib::util::ScopeLatencyReporter scopeLatency(_tabletMetrics->GetreopenRealtimeLatencyMetric().get());
    assert(GetTabletData() != nullptr);

    auto tabletData = GetTabletData();
    auto slice = tabletData->CreateSlice();
    std::vector<std::shared_ptr<Segment>> segments;
    segments.insert(segments.end(), slice.begin(), slice.end());
    if (strategy == RefreshStrategy::REPLACE_BUILDING_SEGMENT) {
        if (!slice.empty() && (*slice.rbegin())->GetSegmentStatus() == Segment::SegmentStatus::ST_BUILDING) {
            segments.pop_back();
        }
    }
    if (strategy == RefreshStrategy::NEW_BUILDING_SEGMENT || strategy == RefreshStrategy::REPLACE_BUILDING_SEGMENT) {
        BuildResource buildResource = GenerateBuildResource(COUNTER_PREFIX);
        auto [status, memSegment] = _memSegmentCreator->CreateMemSegment(buildResource, GetTabletData(), writeSchema);
        if (!status.IsOK()) {
            TABLET_LOG(ERROR, "create mem segment failed, memSegment is null");
            return status;
        }
        assert(memSegment != nullptr);
        segments.push_back(memSegment);
    }

    auto newTabletData = std::make_shared<TabletData>(_tabletInfos->GetTabletName());
    assert(tabletData->GetResourceMap());
    auto st = newTabletData->Init(tabletData->GetOnDiskVersion().Clone(), std::move(segments),
                                  tabletData->GetResourceMap()->Clone());
    if (!st.IsOK()) {
        TABLET_LOG(ERROR, "init tablet data failed: %s", st.ToString().c_str());
        return st;
    }
    st = FinalizeTabletData(newTabletData.get(), writeSchema);
    if (!st.IsOK()) {
        TABLET_LOG(ERROR, "finalize tablet data failed: %s", st.ToString().c_str());
        return st;
    }
    st = OpenWriterAndReader(std::move(newTabletData), _openOptions, _tabletInfos->GetLoadedVersionDeployDescription());
    return st;
}

Status Tablet::CheckDoc(document::IDocumentBatch* batch)
{
    // check if source sealed
    if (_sealedSourceLocator && batch->GetLastLocator().IsSameSrc(_sealedSourceLocator.value(), false)) {
        TABLET_LOG(WARN, "source locator [%s] sealed", _sealedSourceLocator.value().DebugString().c_str());
        return Status::Sealed("sealed");
    }
    if (_sealedSourceLocator) {
        _tabletCommitter->SetSealed(false);
        _sealedSourceLocator.reset();
    }
    return Status::OK();
}

Status Tablet::Build(const std::shared_ptr<document::IDocumentBatch>& batch)
{
    if (unlikely(_tabletOptions->IsReadOnly())) {
        return Status::Unimplement("readonly tablet not support build");
    }
    if (unlikely(batch == nullptr)) {
        return Status::InvalidArgs("document batch is nullptr");
    }
    auto memStatus = GetTabletInfos()->GetMemoryStatus();
    if (memStatus == indexlibv2::framework::MemoryStatus::REACH_TOTAL_MEM_LIMIT ||
        memStatus == indexlibv2::framework::MemoryStatus::REACH_MAX_RT_INDEX_SIZE) {
        return Status::NoMem("memory status: %d", int(memStatus));
    }
    std::lock_guard<std::mutex> guard(_dataMutex);

    auto status = CheckDoc(batch.get());
    if (!status.IsOK()) {
        return status;
    }

    if (_needSeek.load()) {
        _needSeek.store(false);
        return Status::Uninitialize("need re seek source");
    }

    // check locator forward, ignore locator backward
    auto latestLocator = _tabletInfos->GetLatestLocator();
    const Locator& docLocator = batch->GetLastLocator();
    if ((latestLocator.IsValid() && docLocator.IsValid())                      /*locator need valid*/
        && (!latestLocator.IsLegacyLocator() && !docLocator.IsLegacyLocator()) /*locator can not be legacy locator*/
        && latestLocator.IsSameSrc(docLocator, false)) {                       /*need be same src*/
        auto [from, to] = docLocator.GetLocatorRange();
        if (!latestLocator.ShrinkToRange(from, to) ||
            docLocator.IsFasterThan(latestLocator, true) != Locator::LocatorCompareResult::LCR_FULLY_FASTER) {
            _tabletMetrics->AddTabletFault("locator rollback");
            if (_tabletOptions->GetOnlineConfig().GetAllowLocatorRollback()) {
                TABLET_INTERVAL_LOG2(
                    120, WARN,
                    "doc locator [%s] , tablet lastest locator [%s], shrink or compare failed, but not skip it",
                    batch->GetLastLocator().DebugString().c_str(),
                    _tabletInfos->GetLatestLocator().DebugString().c_str());
            } else {
                TABLET_INTERVAL_LOG2(
                    120, ERROR, "skip doc, doc locator [%s] , tablet lastest locator [%s], shrink or compare failed",
                    batch->GetLastLocator().DebugString().c_str(),
                    _tabletInfos->GetLatestLocator().DebugString().c_str());
                return Status::OK();
            }
        }
    }

    if (_tabletWriter == nullptr) {
        Status st = ReopenNewSegment(GetTabletSchema());
        if (!st.IsOK()) {
            CloseWriterUnsafe();
            TABLET_LOG(ERROR, "reopen new segment failed, error: %s", st.ToString().c_str());
            return st;
        }
    }

    status = _tabletWriter->Build(batch);
    if (status.IsNeedDump()) {
        auto segmentDumper = _tabletWriter->CreateSegmentDumper();
        if (segmentDumper == nullptr) {
            return Status::Corruption("create segment dumper failed");
        }
        _tabletDumper->PushSegmentDumper(std::move(segmentDumper));
        Status st = ReopenNewSegment(GetTabletSchema());
        if (!st.IsOK()) {
            CloseWriterUnsafe();
            TABLET_LOG(ERROR, "reopen new segment failed");
            return st;
        }
        status = _tabletWriter->Build(batch);
    } else if (status.IsNoMem()) {
        // TODO(tianwei) Add background task to clean resource.
    }
    if (status.IsOK()) {
        auto memSegLocator = GetMemSegmentLocator();
        if (memSegLocator.IsValid()) {
            _tabletInfos->SetBuildLocator(memSegLocator);
        }
        UpdateDocCount();
    }
    return status;
}

Locator Tablet::GetMemSegmentLocator()
{
    auto slice = GetTabletData()->CreateSlice(Segment::SegmentStatus::ST_BUILDING);
    if (!slice.empty()) {
        return (*slice.begin())->GetLocator();
    }
    return Locator();
}

std::shared_ptr<indexlib::file_system::Directory> Tablet::GetRootDirectory() const
{
    if (!_fence.GetFileSystem()) {
        return nullptr;
    }
    return indexlib::file_system::Directory::Get(_fence.GetFileSystem());
}

bool Tablet::NeedCommit() const { return _tabletCommitter->NeedCommit(); }

std::pair<Status, VersionMeta> Tablet::Commit(const CommitOptions& commitOptions)
{
    auto currentTabletData = GetTabletData();
    TABLET_LOG(INFO, "begin commit version, target version id [%d]", commitOptions.GetTargetVersionId());
    // do not clean in-memory index while commit
    // otherwise mounted merged version will be erased
    RETURN2_IF_STATUS_ERROR(RenewFenceLease(/*createIfNotExist=*/true), VersionMeta(), "commit version failed");
    VersionMeta commitedVersionMeta;
    Locator commitedLocator;
    std::lock_guard<std::mutex> lockReopen(_reopenMutex);
    {
        std::lock_guard<std::mutex> lockCleaner(_cleanerMutex);
        auto [status, version] = _tabletCommitter->Commit(currentTabletData, _fence,
                                                          _tabletOptions->GetBuildConfig().GetMaxCommitRetryCount(),
                                                          _idGenerator.get(), commitOptions);
        if (!status.IsOK()) {
            TABLET_LOG(ERROR, "commit version failed, error[%s]", status.ToString().c_str());
            return std::make_pair(status, VersionMeta());
        }
        auto [status2, versionMeta] = VersionMetaCreator::Create(GetRootDirectory(), *currentTabletData, version);
        if (!status2.IsOK()) {
            TABLET_LOG(ERROR, "create version meta failed");
            return {status2, VersionMeta()};
        }
        TABLET_LOG(INFO, "end commit version[%d]", version.GetVersionId());
        commitedLocator = version.GetLocator();
        commitedVersionMeta = versionMeta;
    }
    if (commitOptions.NeedReopenInCommit()) {
        TABLET_LOG(INFO, "reopen in commit, versionid[%d]", commitedVersionMeta.GetVersionId());
        auto status = DoReopenUnsafe(ReopenOptions(_openOptions), VersionCoord {commitedVersionMeta.GetVersionId(),
                                                                                commitedVersionMeta.GetFenceName()});
        if (!status.IsOK()) {
            TABLET_LOG(ERROR, "reopen commited version failed");
            return {status, VersionMeta()};
        }
    }
    _tabletInfos->SetLastCommittedLocator(commitedLocator);
    TABLET_LOG(INFO, "end commit version id [%d] fence [%s], with [%ld] docs, [%lu] segments, size [%.2fG]",
               commitedVersionMeta.GetVersionId(), commitedVersionMeta.GetFenceName().c_str(),
               commitedVersionMeta.GetDocCount(), commitedVersionMeta.GetSegments().size(),
               commitedVersionMeta.GetIndexSize() / 1024.0 / 1024 / 1024);
    return {Status::OK(), commitedVersionMeta};
}

Status Tablet::LoadVersion(const VersionCoord& versionCoord, Version* version, std::string* versionRoot) const
{
    auto versionId = versionCoord.GetVersionId();
    if (versionId == INVALID_VERSIONID) {
        *version = MakeEmptyVersion(GetTabletSchema()->GetSchemaId());
        return Status::OK();
    }
    // 1. if read from remote: root is remote root.
    // 2. if read from local(dp mode): remote root should be set to local root by suez.
    auto indexRoot = _tabletInfos->GetIndexRoot();
    bool fromPublicRoot = false;
    if ((versionId & Version::PRIVATE_VERSION_ID_MASK) && _tabletOptions->IsOnline()) {
        if (_tabletOptions->FlushRemote()) {
            assert(false);
            auto st = Status::Corruption("private mask with flush remote");
            TABLET_LOG(ERROR, "%s", st.ToString().c_str());
            return st;
        } else {
            *versionRoot = PathUtil::JoinPath(indexRoot.GetLocalRoot(), _fence.GetFenceName());
        }
    } else {
        // "" root in entry_table means use the path where entry_table is, so we must use remote root for leader
        *versionRoot = (_tabletOptions->FlushRemote() || _tabletOptions->GetNeedReadRemoteIndex())
                           ? indexRoot.GetRemoteRoot()
                           : indexRoot.GetLocalRoot();
        if (!versionCoord.GetVersionFenceName().empty()) {
            *versionRoot = PathUtil::JoinPath(*versionRoot, versionCoord.GetVersionFenceName());
        } else {
            fromPublicRoot = true;
        }
    }
    auto versionRootDir = indexlib::file_system::Directory::GetPhysicalDirectory(*versionRoot);
    auto status = VersionLoader::GetVersion(versionRootDir, versionId, version);
    if (!status.IsOK()) {
        TABLET_LOG(ERROR, "load version [%s] from [%s] failed: %s", versionCoord.DebugString().c_str(),
                   versionRoot->c_str(), status.ToString().c_str());
        return status;
    }
    if (fromPublicRoot) {
        // public version is just a copy without entry table, should locate version file in fence.
        *versionRoot = PathUtil::JoinPath(*versionRoot, version->GetFenceName());
    }
    return Status::OK();
}

std::pair<Status, Version>
Tablet::MountOnDiskVersion(const VersionCoord& versionCoord,
                           const std::shared_ptr<indexlibv2::framework::VersionDeployDescription>& versionDpDesc)
{
    Version version;
    std::string versionRoot;
    auto status = LoadVersion(versionCoord, &version, &versionRoot);
    if (!status.IsOK()) {
        return {status, Version()};
    }
    if (!version.IsValid()) {
        return {Status::OK(), MakeEmptyVersion(GetTabletSchema()->GetSchemaId())};
    }
    auto lfs = _fence.GetFileSystem();
    assert(lfs != nullptr);

    std::shared_ptr<indexlib::file_system::LifecycleTable> lifecycleTable;
    if (_tabletOptions->IsOnline()) {
        if (versionDpDesc != nullptr) {
            lifecycleTable = versionDpDesc->GetLifecycleTable();
        }
    }
    status = indexlib::file_system::toStatus(lfs->MountVersion(versionRoot, versionCoord.GetVersionId(),
                                                               /*rawLogicalPath=*/"",
                                                               indexlib::file_system::FSMT_READ_ONLY, lifecycleTable));
    if (!status.IsOK()) {
        TABLET_LOG(ERROR, "Mount version[%s] on [%s] failed: %s", versionCoord.DebugString().c_str(),
                   versionRoot.c_str(), status.ToString().c_str());
        return {status, Version()};
    }
    // used for update metrics for PartitionIndexSize later
    [[maybe_unused]] auto ret = lfs->CalculateVersionFileSize(versionRoot,
                                                              /*rawLogicalPath=*/"", versionCoord.GetVersionId());
    return std::make_pair(Status::OK(), std::move(version));
}

Status Tablet::FinalizeTabletData(TabletData* tabletData, const std::shared_ptr<config::ITabletSchema>& writeSchema)
{
    assert(tabletData);
    auto resourceMap = tabletData->GetResourceMap();
    if (!resourceMap) {
        auto st = Status::InternalError("resource map is empty");
        TABLET_LOG(ERROR, "%s", st.ToString().c_str());
        return st;
    }
    const auto& version = tabletData->GetOnDiskVersion();
    auto schemaGroup = std::make_shared<TabletDataSchemaGroup>();
    schemaGroup->multiVersionSchemas = _tabletSchemaMgr->GetTabletSchemaCache();
    schemaGroup->writeSchema = writeSchema;
    auto schema = _tabletSchemaMgr->GetSchema(version.GetSchemaId());
    assert(schema);
    schemaGroup->onDiskWriteSchema = schema;
    schema = _tabletSchemaMgr->GetSchema(version.GetReadSchemaId());
    assert(schema);
    schemaGroup->onDiskReadSchema = schema;
    auto status = resourceMap->AddVersionResource(TabletDataSchemaGroup::NAME, schemaGroup);
    if (!status.IsOK()) {
        TABLET_LOG(ERROR, "add [%s] to resource map failed: %s", TabletDataSchemaGroup::NAME,
                   status.ToString().c_str());
        return status;
    }
    // declare task configs
    const auto& taskConfigs = _tabletOptions->GetAllIndexTaskConfigs();
    for (auto& config : taskConfigs) {
        tabletData->DeclareTaskConfig(config.GetTaskName(), config.GetTaskType());
    }
    return Status::OK();
}

int64_t Tablet::GetSuggestBuildingSegmentMemoryUse()
{
    if (_tabletOptions->IsOnline()) {
        return _tabletOptions->GetOnlineConfig().GetMaxRealtimeMemoryUse() / 3;
    }
    if (_buildMemoryQuotaSynchronizer &&
        _buildMemoryQuotaSynchronizer->GetTotalQuota() != std::numeric_limits<int64_t>::max()) {
        return _buildMemoryQuotaSynchronizer->GetTotalQuota();
    }
    return 6L * 1024 * 1024 * 1024; // default 6G
}

Status Tablet::PrepareEmptyTabletData(const std::shared_ptr<config::ITabletSchema>& tabletSchema)
{
    // assert dataMutex has been locked or not necessary
    _tabletData = std::make_unique<TabletData>(_tabletInfos->GetTabletName());
    auto emptyVersion = MakeEmptyVersion(tabletSchema->GetSchemaId());
    auto status = _tabletData->Init(/*onDiskVersion=*/emptyVersion, /*segments=*/ {}, std::make_shared<ResourceMap>());
    assert(status.IsOK());
    status = FinalizeTabletData(_tabletData.get(), tabletSchema);
    if (!status.IsOK()) {
        TABLET_LOG(ERROR, "finalize tablet data failed: %s", status.ToString().c_str());
        assert(false);
        return status;
    }
    return Status::OK();
}

bool Tablet::NeedRecoverFromLocal() const
{
    return (!_tabletOptions->IsLeader() && _tabletOptions->IsOnline() && _tabletOptions->FlushLocal() &&
            !_tabletOptions->FlushRemote() && _tabletOptions->GetOnlineConfig().LoadRemainFlushRealtimeIndex());
}

Status Tablet::Open(const IndexRoot& indexRoot, const std::shared_ptr<config::ITabletSchema>& tabletSchema,
                    const std::shared_ptr<config::TabletOptions>& options, const VersionCoord& versionCoord)
{
    TABLET_LOG(INFO, "open tablet begin, indexRoot[%s], version[%s], leader[%d], flushlocal[%d], flushremote[%d]",
               indexRoot.ToString().c_str(), versionCoord.DebugString().c_str(), options->IsLeader(),
               options->FlushLocal(), options->FlushRemote());
    autil::ScopedTime2 timer;
    _tabletInfos->SetIndexRoot(indexRoot);

    _tabletOptions = std::make_shared<config::TabletOptions>(*options);
    _tabletOptions->SetTabletName(_tabletInfos->GetTabletName());
    _tabletFactory = CreateTabletFactory(tabletSchema->GetTableType(), _tabletOptions);
    if (_tabletFactory == nullptr) {
        auto st = Status::ConfigError("create tablet factory failed");
        TABLET_LOG(ERROR, "%s", st.ToString().c_str());
        return st;
    }

    auto status = TabletSchemaLoader::ResolveSchema(_tabletOptions, indexRoot.GetRemoteRoot(), tabletSchema.get());
    if (!status.IsOK()) {
        TABLET_LOG(ERROR, "resolve schema failed: %s", status.ToString().c_str());
        return status;
    }

    if (_idGenerator == nullptr) {
        _idGenerator = std::make_shared<IdGenerator>(_tabletOptions->FlushRemote() ? IdMaskType::BUILD_PUBLIC
                                                                                   : IdMaskType::BUILD_PRIVATE);
        TABLET_LOG(INFO, "use built-in id generator");
    }
    status = _tabletOptions->Check(_tabletInfos->GetIndexRoot().GetRemoteRoot(),
                                   _tabletInfos->GetIndexRoot().GetLocalRoot());
    if (!status.IsOK()) {
        TABLET_LOG(ERROR, "check tablet options failed");
        return status;
    }

    try {
        auto status = PrepareIndexRoot(tabletSchema);
        if (!status.IsOK()) {
            TABLET_LOG(ERROR, "tablet prepare index root [%s] failed", indexRoot.ToString().c_str());
            return status;
        }
        _tabletSchemaMgr = std::make_unique<TabletSchemaManager>(_tabletFactory, _tabletOptions, _fence.GetGlobalRoot(),
                                                                 indexRoot.GetRemoteRoot(), _fence.GetFileSystem());
        _tabletSchemaMgr->InsertSchemaToCache(tabletSchema);
        _memSegmentCreator = std::make_unique<MemSegmentCreator>(_tabletInfos->GetTabletName(), _tabletOptions.get(),
                                                                 _tabletFactory.get(), _idGenerator.get(),
                                                                 GetRootDirectory()->GetIDirectory());

        status = PrepareEmptyTabletData(tabletSchema);
        if (!status.IsOK()) {
            TABLET_LOG(ERROR, "tablet prepare empty tablet data failed");
            return status;
        }
        status = PrepareResource();
        if (!status.IsOK()) {
            TABLET_LOG(ERROR, "tablet prepare resource failed");
            return status;
        }
        versionid_t loadedLocalVersionId = INVALID_VERSIONID;
        autil::ScopeGuard tabletPhaseGuard = _tabletMetrics->CreateTabletPhaseGuard(TabletPhase::OPEN);
        if (NeedRecoverFromLocal()) {
            std::string fenceName =
                Fence::GenerateNewFenceName(_tabletOptions->FlushRemote(), _tabletInfos->GetTabletId());
            std::string localFenceRoot = PathUtil::JoinPath(_tabletInfos->GetIndexRoot().GetLocalRoot(), fenceName);
            auto [recoverStatus, localVersion] = RecoverLatestVersion(localFenceRoot);
            if (!recoverStatus.IsOK()) {
                auto cleanStatus = DropIndexCleaner::DropPrivateFence(_tabletInfos->GetIndexRoot().GetLocalRoot());
                RETURN_IF_STATUS_ERROR(cleanStatus, "clean local index failed");
                TABLET_LOG(WARN, "local index is cleaned [%s]", localFenceRoot.c_str());
                return recoverStatus;
            }
            Version targetVersion;
            std::string versionRoot;
            status = LoadVersion(versionCoord, &targetVersion, &versionRoot);
            if (!status.IsOK()) {
                TABLET_LOG(WARN, "load version[%s] failed", versionCoord.DebugString().c_str());
                return status;
            }
            if (!localVersion.IsValid() || localVersion.GetVersionId() == INVALID_VERSIONID ||
                targetVersion.GetLocator().IsFasterThan(localVersion.GetLocator(), /*ignoreLegacyDiffSrc=*/true) ==
                    Locator::LocatorCompareResult::LCR_FULLY_FASTER) {
                // do nothing
            } else if (!targetVersion.CanFastFowardFrom(localVersion.GetVersionLine().GetHeadVersion(),
                                                        /*isDirty*/ false)) {
                auto cleanStatus = DropIndexCleaner::DropPrivateFence(_tabletInfos->GetIndexRoot().GetLocalRoot());
                RETURN_IF_STATUS_ERROR(cleanStatus, "clean local index failed");
                TABLET_LOG(WARN, "local index is cleaned [%s]", localFenceRoot.c_str());
                status = Status::Abort("local private version [%d] is not compatible with target version[%s]",
                                       localVersion.GetVersionId(), versionCoord.DebugString().c_str());
                TABLET_LOG(ERROR, "%s", status.ToString().c_str());
                return status;
            } else {
                std::lock_guard<std::mutex> lockReopen(_reopenMutex);
                status = DoReopenUnsafe(ReopenOptions(_openOptions), localVersion.GetVersionId());
                if (!status.IsOK()) {
                    TABLET_LOG(ERROR, "recover from local version [%d] failed", localVersion.GetVersionId());
                    auto cleanStatus = DropIndexCleaner::DropPrivateFence(_tabletInfos->GetIndexRoot().GetLocalRoot());
                    RETURN_IF_STATUS_ERROR(cleanStatus, "clean local index failed");
                    TABLET_LOG(WARN, "local index is cleaned [%s]", localFenceRoot.c_str());
                    return status;
                }
                loadedLocalVersionId = localVersion.GetVersionId();
                TABLET_LOG(INFO, "recover from local version[%d] succeed.", loadedLocalVersionId);
            }
        }
        if (loadedLocalVersionId != INVALID_VERSIONID && versionCoord.GetVersionId() == INVALID_VERSIONID) {
            // do nothing
        } else {
            std::lock_guard<std::mutex> lockReopen(_reopenMutex);
            status = DoReopenUnsafe(ReopenOptions(_openOptions), versionCoord);
            if (!status.IsOK()) {
                TABLET_LOG(ERROR, "tablet load version [%s] failed", versionCoord.DebugString().c_str());
                return status;
            }
        }
    } catch (const indexlib::util::FileIOException& e) {
        // TODO(hanyao): fill status message b exception
        TABLET_LOG(ERROR, "open caught file io exception: %s", e.what());
        return Status::IOError("open caught fail io exception: ", e.what());
    } catch (const autil::legacy::ExceptionBase& e) {
        TABLET_LOG(ERROR, "open caught exception, %s", e.what());
        return Status::Unknown("open caught exception: ", e.what());
    } catch (const std::exception& e) {
        // TODO(hanyao): fill status message b exception
        TABLET_LOG(ERROR, "open caught exception, %s", e.what());
        return Status::Unknown("open caught exception: ", e.what());
    } catch (...) {
        TABLET_LOG(ERROR, "open caught exception");
        return Status::Unknown("open caught exception");
    }

    if (!StartIntervalTask()) {
        TABLET_LOG(ERROR, "start interval task failed");
        return Status::Corruption("Start interval task failed.");
    }

    _tabletCommitter->Init(_versionMerger, _tabletData);
    TABLET_LOG(INFO, "open tablet end, used [%.3f]s", timer.done_sec());
    _isClosed = false;
    _needSeek.store(false);
    return Status::OK();
}

void Tablet::UpdateDocCount()
{
    _tabletData->RefreshDocCount();
    _tabletInfos->UpdateTabletDocCount(_tabletData->GetTabletDocCount());
}

// This is used inside Reopen() and updates runtime state of the tablet. Data and internal data structures should not be
// changed inside this function.
Status Tablet::UpdateControlFlow(const ReopenOptions& reopenOptions)
{
    assert(reopenOptions.GetOpenOptions().GetUpdateControlFlowOnly());
    if (_consistentModeBuildThreadPool == nullptr &&
        reopenOptions.GetOpenOptions().GetConsistentModeBuildThreadCount() > 0) {
        size_t queueSize = size_t(-1);
        auto threadPoolQueueFactory = std::make_shared<autil::ThreadPoolQueueFactory>();
        _consistentModeBuildThreadPool =
            std::make_shared<autil::ThreadPool>(reopenOptions.GetOpenOptions().GetConsistentModeBuildThreadCount(),
                                                queueSize, threadPoolQueueFactory, "consistent-parallel-build");
    }
    if (_inconsistentModeBuildThreadPool == nullptr &&
        reopenOptions.GetOpenOptions().GetInconsistentModeBuildThreadCount() > 0) {
        size_t queueSize = size_t(-1);
        auto threadPoolQueueFactory = std::make_shared<autil::ThreadPoolQueueFactory>();
        _inconsistentModeBuildThreadPool =
            std::make_shared<autil::ThreadPool>(reopenOptions.GetOpenOptions().GetInconsistentModeBuildThreadCount(),
                                                queueSize, threadPoolQueueFactory, "inconsistent-parallel-build");
    }
    std::lock_guard<std::mutex> guard(_dataMutex);
    if (_tabletWriter == nullptr) {
        // Writer is not initialized yet, so there is nothing to update.
        return Status::OK();
    }
    BuildResource buildResource = GenerateBuildResource(COUNTER_PREFIX);
    return _tabletWriter->Open(/*tabletData=*/nullptr, buildResource, reopenOptions.GetOpenOptions());
}

Status Tablet::Reopen(const ReopenOptions& reopenOptions, const VersionCoord& versionCoord)
{
    const auto& versionId = versionCoord.GetVersionId();
    TABLET_LOG(INFO, "reopen tablet begin, force[%d], version [%d]", reopenOptions.IsForceReopen(), versionId);
    if (reopenOptions.IsForceReopen()) {
        return Status::Unimplement("tablet not support force reopen, try reload, version [%d]", versionId);
    }
    autil::ScopedTime2 timer;
    if (versionId == CONTROL_FLOW_VERSIONID) {
        TABLET_LOG(INFO, "reopen as entrance to update control flow only.");
        auto status = UpdateControlFlow(reopenOptions);
        _openOptions = reopenOptions.GetOpenOptions();
        _openOptions.SetUpdateControlFlowOnly(false);
        TABLET_LOG(INFO, "reopen tablet end, version [%d], status [%s], used [%.3f]s", versionId,
                   status.ToString().c_str(), timer.done_sec());
        return status;
    }
    ReopenOptions clonedReopenOptions;
    clonedReopenOptions.SetOpenOptions(_openOptions);
    if (versionId == INVALID_VERSIONID) {
        TABLET_LOG(ERROR, "reopen failed, version invalid");
        return Status::InvalidArgs("version invalid");
    }
    try {
        TabletPhase tabletPhase =
            clonedReopenOptions.IsForceReopen() ? TabletPhase::FORCE_REOPEN : TabletPhase::NORMAL_REOPEN;
        autil::ScopeGuard tabletPhaseGuard = _tabletMetrics->CreateTabletPhaseGuard(tabletPhase);
        std::lock_guard<std::mutex> lockReopen(_reopenMutex);
        auto status = DoReopenUnsafe(clonedReopenOptions, versionCoord);
        TABLET_LOG(INFO, "reopen tablet end, force[%d], version [%s], status [%s], used [%.3f]s",
                   clonedReopenOptions.IsForceReopen(), versionCoord.DebugString().c_str(), status.ToString().c_str(),
                   timer.done_sec());
        return status;
    } catch (const indexlib::util::FileIOException& e) {
        TABLET_LOG(ERROR, "reopen caught file io exception: %s", e.what());
        return Status::IOError("reopen caught faile io exception", e.what());
    } catch (const std::exception& e) {
        TABLET_LOG(ERROR, "reopen caught exception: %s", e.what());
        return Status::Unknown("reopen caught std exception");
    } catch (...) {
        TABLET_LOG(ERROR, "reopen caught unknown exception");
        return Status::Unknown("reopen caught unknown exception");
    }
}

VersionCoord CalculateHead(const std::shared_ptr<TabletData>& currentTabletData, const Fence& fence,
                           bool* hasBuildingSegment)
{
    assert(currentTabletData);
    auto ondiskVersion = currentTabletData->GetOnDiskVersion();
    auto headVersion = ondiskVersion.GetVersionLine().GetHeadVersion();
    versionid_t headVersionId = headVersion.GetVersionId();
    std::string headFence = headVersion.GetVersionFenceName();
    auto segments = currentTabletData->CreateSlice();
    // 有building segment的情况下，说明当前tablet构建了数据，就不能reopen其他fence产出的version
    // 否则有可能出现segment号，相同的情况
    *hasBuildingSegment = false;
    for (const auto& segmentPtr : segments) {
        if (segmentPtr->GetSegmentStatus() != Segment::SegmentStatus::ST_BUILT &&
            Segment::IsPublicSegmentId(segmentPtr->GetSegmentId())) {
            *hasBuildingSegment = true;
        }
    }
    if (*hasBuildingSegment) {
        headFence = fence.GetFenceName();
    }
    return VersionCoord(headVersionId, headFence);
}

Status Tablet::DoReopenUnsafe(const ReopenOptions& reopenOptions, const VersionCoord& versionCoord)
{
    // do not clean in-memory index while reopen
    // otherwise mounted version may be erased by mistake.
    std::lock_guard<std::mutex> lockCleaner(_cleanerMutex);
    BuildResource buildResource = GenerateBuildResource(COUNTER_PREFIX);
    indexlib::util::ScopeLatencyReporter reopenLatency(_tabletMetrics->GetreopenIncLatencyMetric().get());

    auto statusOrDpDesc = CreateVersionDeployDescription(versionCoord.GetVersionId());
    RETURN_IF_STATUS_ERROR(statusOrDpDesc, "create version deploy description failed for version [%d]",
                           versionCoord.GetVersionId());
    auto versionDpDesc = std::move(statusOrDpDesc.steal_value());
    auto [mountStatus, version] = MountOnDiskVersion(versionCoord, versionDpDesc);
    if (!mountStatus.IsOK()) {
        TABLET_LOG(ERROR, "mount version [%s] failed: %s", versionCoord.DebugString().c_str(),
                   mountStatus.ToString().c_str());
        return mountStatus;
    }
    std::shared_ptr<TabletData> currentTabletData;
    {
        std::lock_guard<std::mutex> lock(_dataMutex);
        currentTabletData = _tabletData;
    }
    TABLET_LOG(INFO, "do reopen, version [%d => %d]", currentTabletData->GetOnDiskVersion().GetVersionId(),
               version.GetVersionId());
    bool hasBuildingSegment;
    auto headVersionCoord = CalculateHead(currentTabletData, _fence, &hasBuildingSegment);
    bool isPrivateVersion = (versionCoord.GetVersionId() & Version::PRIVATE_VERSION_ID_MASK) > 0;
    if (_tabletOptions->IsOnline() && !isPrivateVersion &&
        !version.CanFastFowardFrom(headVersionCoord, hasBuildingSegment)) {
        TABLET_LOG(ERROR,
                   "do reopen failed, version can't fast from onDiskVersion, head version [%s], version line [%s]",
                   autil::legacy::ToJsonString(headVersionCoord, true).c_str(),
                   autil::legacy::ToJsonString(version.GetVersionLine()).c_str());
        return Status::Corruption("reopen fastford failed");
    }
    _idGenerator->UpdateBaseVersion(version);

    auto loadSchemaStatus = _tabletSchemaMgr->LoadAllSchema(version);
    if (!loadSchemaStatus.IsOK()) {
        TABLET_LOG(ERROR, "do reopen failed, load all schema failed");
        return loadSchemaStatus;
    }

    TABLET_LOG(INFO, "begin load segments, force[%d], segments [%s]", reopenOptions.IsForceReopen(),
               GetDiffSegmentDebugString(version, currentTabletData).c_str());
    auto readSchema = _tabletSchemaMgr->GetSchema(version.GetReadSchemaId());
    if (currentTabletData) {
        auto status =
            DropIndexCleaner::CleanIndexInLogical(currentTabletData, readSchema, GetRootDirectory()->GetIDirectory());
        if (!status.IsOK()) {
            TABLET_LOG(ERROR, "do reopen failed, clean drop index failed");
            return status;
        }
    }
    auto currentLifecycleTable = LifecycleTableCreator::CreateLifecycleTable(
        version, _tabletOptions->GetOnlineConfig().GetLifecycleConfig(),
        {{indexlib::file_system::LifecyclePatternBase::CURRENT_TIME,
          std::to_string(indexlib::file_system::LifecycleConfig::CurrentTimeInSeconds())}});

    if (currentLifecycleTable == nullptr) {
        TABLET_LOG(ERROR, "do reopen failed, create LifecycleTable failed for version[%d]", version.GetVersionId());
        return Status::Corruption("reopen failed due to null lifecycleTable");
    }

    std::vector<std::pair<std::shared_ptr<Segment>, bool>> segmentPairs;
    std::shared_ptr<indexlib::file_system::Directory> root = GetRootDirectory();
    for (auto [segmentId, schemaId] : version) {
        auto seg = currentTabletData->GetSegment(segmentId);
        auto segmentDirName = version.GetSegmentDirName(segmentId);
        auto currentLifecycle = currentLifecycleTable->GetLifecycle(segmentDirName + "/");
        bool needLoadDiskSegment = true;
        if (seg != nullptr && seg->GetSegmentStatus() == Segment::SegmentStatus::ST_BUILT) {
            seg->GetSegmentDirectory()->SetLifecycle(currentLifecycle);
            auto segmentSchemaId = seg->GetSegmentSchema()->GetSchemaId();
            auto preLifecycle = seg->GetSegmentLifecycle();
            if (segmentSchemaId != schemaId) {
                auto diskSegment = std::dynamic_pointer_cast<DiskSegment>(seg);
                assert(diskSegment != nullptr);
                std::vector<std::shared_ptr<config::ITabletSchema>> tabletSchemas;
                auto status = _tabletSchemaMgr->GetSchemaList(segmentSchemaId, schemaId, version, tabletSchemas);
                RETURN_IF_STATUS_ERROR(status, "get schema list failed, segmentSchemaId[%d], segmentId[%d]",
                                       segmentSchemaId, segmentId);
                if (preLifecycle != currentLifecycle) {
                    RETURN_IF_STATUS_ERROR(
                        status, "config conficts, segmentId[%d] schemaId updated [%d->%d], lifecycle updated[%s -> %s]",
                        segmentId, segmentSchemaId, schemaId, preLifecycle.c_str(), currentLifecycle.c_str());
                }
                status = diskSegment->Reopen(tabletSchemas);
                RETURN_IF_STATUS_ERROR(status, "disk segment open failed, segmentId[%d]", segmentId);
                needLoadDiskSegment = false;
            } else {
                if (preLifecycle != currentLifecycle) {
                    TABLET_LOG(INFO, "built segmentId[%d] lifecycle updated [%s] -> [%s]", segmentId,
                               preLifecycle.c_str(), currentLifecycle.c_str());
                } else {
                    needLoadDiskSegment = false;
                }
            }
        }
        if (!needLoadDiskSegment) {
            segmentPairs.emplace_back(std::make_pair(seg, /*needOpen=*/false));
        } else {
            auto segDir = root->GetDirectory(segmentDirName,
                                             /*throwExceptionIfNotExist=*/false);
            if (!segDir) {
                auto st = Status::IOError("get segment[%d] dir failed", segmentId);
                TABLET_LOG(ERROR, "do reopen failed, %s", st.ToString().c_str());
                return st;
            }
            segDir->SetLifecycle(currentLifecycle);
            SegmentMeta segmentMeta;
            segmentMeta.segmentId = segmentId;
            segmentMeta.lifecycle = currentLifecycle;
            auto schema = _tabletSchemaMgr->GetSchema(schemaId);
            segmentMeta.schema = schema;
            segmentMeta.segmentDir = segDir;
            auto readerOption = indexlib::file_system::ReaderOption::PutIntoCache(indexlib::file_system::FSOT_MEM);
            if (!segmentMeta.segmentInfo->Load(segDir->GetIDirectory(), readerOption).IsOK()) {
                auto st = Status::Corruption("load segment info[%s] failed", segDir->GetLogicalPath().c_str());
                TABLET_LOG(ERROR, "do reopen failed, %s", st.ToString().c_str());
                return st;
            }
            auto diskSegment =
                std::shared_ptr<Segment>(_tabletFactory->CreateDiskSegment(segmentMeta, buildResource).release());
            assert(diskSegment);
            segmentPairs.emplace_back(std::make_pair(diskSegment, /*needOpen=*/true));
        }
    }

    TABLET_LOG(INFO, "end load segments, segment count [%lu]", segmentPairs.size());

    TABLET_LOG(INFO, "begin load tablet data, fence name[%s]", _fence.GetFenceName().c_str());
    const indexlib::util::MemoryReserverPtr memReserver = _fence.GetFileSystem()->CreateMemoryReserver("load_segments");
    auto tabletLoader = _tabletFactory->CreateTabletLoader(_fence.GetFenceName());
    assert(tabletLoader);
    auto versionSchema = _tabletSchemaMgr->GetSchema(version.GetSchemaId());
    tabletLoader->Init(_tabletMemoryQuotaController, versionSchema, memReserver, _tabletOptions->IsOnline());

    {
        indexlib::util::ScopeLatencyReporter preloadLatency(_tabletMetrics->GetpreloadLatencyMetric().get());
        auto status = tabletLoader->PreLoad(*currentTabletData, std::move(segmentPairs), version);
        if (!status.IsOK()) {
            TABLET_LOG(ERROR, "do reopen failed, tablet preload version [%s] failed", version.DebugString().c_str());
            return status;
        }
    }

    indexlib::util::ScopeLatencyReporter finalLoadLatency(_tabletMetrics->GetfinalLoadLatencyMetric().get());
    std::lock_guard lock(_dataMutex);
    if (version.GetSchemaId() > GetTabletSchema()->GetSchemaId()) {
        auto status = DoAlterTable(versionSchema);
        if (!status.IsOK()) {
            TABLET_LOG(ERROR, "do reopen failed, do alter table on schema[%u] failed: %s", version.GetSchemaId(),
                       status.ToString().c_str());
            return status;
        }
    }
    auto [status, newTabletData] = tabletLoader->FinalLoad(*_tabletData);
    if (!status.IsOK()) {
        TABLET_LOG(ERROR, "do reopen failed, tablet final load failed");
        return status;
    }

    status = FinalizeTabletData(newTabletData.get(), GetTabletSchema());
    if (!status.IsOK()) {
        TABLET_LOG(ERROR, "do reopen failed, finalize tablet data failed: %s", status.ToString().c_str());
        return status;
    }
    assert(newTabletData->GetResourceMap());
    newTabletData->ReclaimSegmentResource();
    TABLET_LOG(INFO, "end load tablet data");

    _tabletDumper->TrimDumpingQueue(*newTabletData);
    if (version.GetVersionId() & Version::PRIVATE_VERSION_ID_MASK) {
        versionDpDesc = _tabletInfos->GetLoadedVersionDeployDescription();
    }
    status = OpenWriterAndReader(std::move(newTabletData), reopenOptions.GetOpenOptions(), versionDpDesc);
    if (status.IsOK()) {
        if (!(version.GetVersionId() & Version::PRIVATE_VERSION_ID_MASK)) {
            _tabletInfos->SetLoadedPublishVersion(version);
            _tabletInfos->SetLoadedVersionDeployDescription(versionDpDesc);
        } else {
            _tabletInfos->SetLoadedPrivateVersion(version);
        }
        if (_versionMerger) {
            _versionMerger->UpdateVersion(version);
        }
    } else {
        TABLET_LOG(ERROR, "do reopen failed, tablet open writer and reader failed");
    }
    if (!GetMemSegmentLocator().IsValid()) {
        if (_tabletInfos->GetLoadedPublishVersion().IsSealed()) {
            _sealedSourceLocator = _tabletInfos->GetLoadedPublishVersion().GetLocator();
            if (_sealedSourceLocator) {
                _tabletCommitter->SetSealed(true);
            }
        } else {
            if (_sealedSourceLocator) {
                _sealedSourceLocator.reset();
                _tabletCommitter->SetSealed(false);
            }
        }
    }
    _tabletCommitter->SetLastPublicVersion(version);
    RETURN_IF_STATUS_ERROR(status, "open writer and reader failed");

    return Status::OK();
}

void Tablet::Close()
{
    if (_isClosed) {
        return;
    }

    TABLET_LOG(INFO, "close tablet [%p] begin", this);
    if (_taskScheduler) {
        _taskScheduler->CleanTasks();
        _taskScheduler.reset();
    }
    if (_versionMerger) {
        _versionMerger->WaitStop();
    }
    _tabletReader.reset();
    if (_tabletReaderContainer) {
        _tabletReaderContainer->Close();
    }

    TABLET_LOG(INFO, "close tablet [%p] end", this);
    _isClosed = true;
    if (_closeRunFunc) {
        _closeRunFunc();
    }
}

Status Tablet::PrepareIndexRoot(const std::shared_ptr<config::ITabletSchema>& schema)
{
    // TODO memory controller
    TABLET_LOG(INFO, "begin prepare index root");
    auto status = InitIndexDirectory(_tabletInfos->GetIndexRoot());
    if (!status.IsOK()) {
        return status;
    }
    status = InitBasicIndexInfo(schema);
    if (!status.IsOK()) {
        return status;
    }

    TABLET_LOG(INFO, "end prepare index root");
    return Status::OK();
}

bool Tablet::IsEmptyDir(std::shared_ptr<indexlib::file_system::Directory> directory,
                        const std::shared_ptr<config::ITabletSchema>& schema)
{
    std::string rootDir = directory->GetOutputPath();
    std::string schemaFileName = schema->GetSchemaFileName();
    std::string schemaFilePath = PathUtil::JoinPath(rootDir, schemaFileName);
    bool schemaExist = indexlib::file_system::FslibWrapper::IsExist(schemaFilePath).GetOrThrow();
    if (!schemaExist) {
        TABLET_LOG(INFO, "empty index dir, no schema file");
        return true;
    }
    std::string indexFormatVersionPath = PathUtil::JoinPath(rootDir, INDEX_FORMAT_VERSION_FILE_NAME);
    bool formatVersionExist = indexlib::file_system::FslibWrapper::IsExist(indexFormatVersionPath).GetOrThrow();
    if (!formatVersionExist) {
        TABLET_LOG(INFO, "empty index dir, no [%s] file", INDEX_FORMAT_VERSION_FILE_NAME);
        return true;
    }
    return false;
}

Status Tablet::InitBasicIndexInfo(const std::shared_ptr<config::ITabletSchema>& schema)
{
    TABLET_LOG(INFO, "begin init index dir");
    try {
        auto rootDirectory = indexlib::file_system::Directory::GetPhysicalDirectory(_fence.GetGlobalRoot());
        if (!IsEmptyDir(rootDirectory, schema) || _tabletOptions->IsReadOnly()) {
            return Status::OK();
        }

        // BINARY_FORMAT_VERSION = 1
        std::string indexFormatVersionStr = std::string(R"({"index_format_version":")") +
                                            indexlib::INDEX_FORMAT_VERSION +
                                            R"(","inverted_index_binary_format_version":1})";
        std::string formatVersionPath =
            indexlib::util::PathUtil::JoinPath(_fence.GetGlobalRoot(), INDEX_FORMAT_VERSION_FILE_NAME);
        auto ec = indexlib::file_system::FslibWrapper::AtomicStore(formatVersionPath, indexFormatVersionStr).Code();
        if (ec != indexlib::file_system::FSEC_OK && ec != indexlib::file_system::FSEC_EXIST) {
            return Status::IOError();
        }

        std::string schemaFileName = schema->GetSchemaFileName();
        std::string content;
        if (!schema->Serialize(/*isCompact*/ false, &content)) {
            TABLET_LOG(ERROR, "schema serialize failed");
            return Status::IOError();
        }
        std::string schemaPath = indexlib::util::PathUtil::JoinPath(_fence.GetGlobalRoot(), schemaFileName);
        ec = indexlib::file_system::FslibWrapper::AtomicStore(schemaPath, content).Code();
        if (ec != indexlib::file_system::FSEC_OK && ec != indexlib::file_system::FSEC_EXIST) {
            return Status::IOError();
        }
        rootDirectory->Sync(true);
        _fence.GetFileSystem()
            ->MountFile(rootDirectory->GetRootDir(), INDEX_FORMAT_VERSION_FILE_NAME, INDEX_FORMAT_VERSION_FILE_NAME,
                        indexlib::file_system::FSMT_READ_ONLY, indexFormatVersionStr.size(), false)
            .GetOrThrow();
        _fence.GetFileSystem()
            ->MountFile(rootDirectory->GetRootDir(), schemaFileName, schemaFileName,
                        indexlib::file_system::FSMT_READ_ONLY, content.size(), false)
            .GetOrThrow();
    } catch (const autil::legacy::ExceptionBase& e) {
        TABLET_LOG(ERROR, "init index dir failed io exception [%s]", e.what());
        return Status::IOError();
    }
    TABLET_LOG(INFO, "end init index dir");
    return Status::OK();
}

Status Tablet::PrepareResource()
{
    TABLET_LOG(INFO, "begin prepare resource");
    if (_tabletReaderContainer) {
        return Status::OK();
    }
    auto planCreator = _tabletFactory->CreateIndexTaskPlanCreator();
    if (planCreator && _mergeController && _tabletOptions->IsLeader()) {
        auto indexRoot = _tabletInfos->GetIndexRoot().GetRemoteRoot();
        _versionMerger =
            std::make_shared<VersionMerger>(_tabletInfos->GetTabletName(), std::move(indexRoot), _mergeController,
                    std::move(planCreator), _metricsManager.get(), _tabletOptions->IsOnline());
        TABLET_LOG(INFO, "version merger created, isOnline[%d]", _tabletOptions->IsOnline());
    }

    if (_buildMemoryQuotaSynchronizer == nullptr) {
        auto buildMemoryQuotaController = std::make_shared<MemoryQuotaController>(
            _tabletInfos->GetTabletName() + "_build", _tabletOptions->GetBuildMemoryQuota());
        _buildMemoryQuotaSynchronizer = std::make_shared<MemoryQuotaSynchronizer>(buildMemoryQuotaController);
    }
    auto reporter = _fence.GetFileSystem()->GetFileSystemMetricsReporter();
    if (reporter != nullptr) {
        indexlib::util::KeyValueMap tagMap;
        tagMap["schema_name"] = GetTabletSchema()->GetTableName();
        // string generationStr;
        // if (onDiskVersion.GetDescription("generation", generationStr)) {
        //     tagMap["generation"] = generationStr;
        // }
        reporter->UpdateGlobalTags(tagMap);
    }

    _tabletInfos->SetCounterMap(_metricsManager->GetCounterMap());

    auto status = _tabletInfos->InitCounter(_tabletOptions->IsOnline());
    if (!status.IsOK()) {
        TABLET_LOG(ERROR, "init counter failed [%s]", status.ToString().c_str());
        return status;
    }
    _tabletReaderContainer = std::make_shared<TabletReaderContainer>(_tabletInfos->GetTabletName());
    _tabletDumper->Init(_tabletOptions->GetMaxDumpIntervalSecond());

    assert(!_tabletMetrics);
    _tabletMetrics = std::dynamic_pointer_cast<TabletMetrics>(
        _metricsManager->CreateMetrics("tablet", [this]() -> std::shared_ptr<IMetrics> {
            return std::make_shared<TabletMetrics>(_metricsManager->GetMetricsReporter(),
                                                   _tabletMemoryQuotaController.get(), _tabletInfos->GetTabletName(),
                                                   _tabletDumper.get(), _versionMerger.get());
        }));
    assert(_tabletMetrics);
    _tabletInfos->SetTabletMetrics(_tabletMetrics);
    _memControlStrategy = _tabletFactory->CreateMemoryControlStrategy(_buildMemoryQuotaSynchronizer);
    if (_memControlStrategy == nullptr) {
        _memControlStrategy.reset(new DefaultMemoryControlStrategy(_tabletOptions, _buildMemoryQuotaSynchronizer));
    }

    TABLET_LOG(INFO, "end prepare resource");
    return Status::OK();
}

std::shared_ptr<document::IDocumentParser> Tablet::CreateDocumentParser()
{
    // TODO: support built in indexlibv2::document::DocumentParser
    return std::shared_ptr<document::IDocumentParser>();
}

std::unique_ptr<ITabletFactory> Tablet::CreateTabletFactory(const std::string& tableType,
                                                            const std::shared_ptr<config::TabletOptions>& options) const
{
    auto tabletFactoryCreator = TabletFactoryCreator::GetInstance();
    auto tabletFactory = tabletFactoryCreator->Create(tableType);
    if (!tabletFactory) {
        TABLET_LOG(ERROR, "create tablet factory with type [%s] failed, registered types [%s]", tableType.c_str(),
                   autil::legacy::ToJsonString(tabletFactoryCreator->GetRegisteredType(), true).c_str());
        return nullptr;
    }
    if (!tabletFactory->Init(options, _metricsManager.get())) {
        TABLET_LOG(ERROR, "init tablet factory with type [%s] failed", tableType.c_str());
        return nullptr;
    }
    return tabletFactory;
}

Status Tablet::InitIndexDirectory(const IndexRoot& indexRoot)
{
    TABLET_LOG(INFO, "begin init index directory, IndexRoot [%s]", indexRoot.ToString().c_str());
    auto metricProvider = std::make_shared<indexlib::util::MetricProvider>(_metricsManager->GetMetricsReporter());
    indexlib::file_system::FileSystemOptions fsOptions;
    // TODO(hanyao): set default root path on load config list
    // TODO(hanyao): get config from options
    // memory quota
    // file block cache controller
    fsOptions.outputStorage = indexlib::file_system::FSST_MEM;
    if (_tabletOptions->GetBuildConfig().GetIsEnablePackageFile()) {
        fsOptions.outputStorage = indexlib::file_system::FSST_PACKAGE_MEM;
    }

    fsOptions.memoryQuotaControllerV2 = _tabletMemoryQuotaController;
    fsOptions.loadConfigList = _tabletOptions->GetLoadConfigList();
    fsOptions.fileBlockCacheContainer = _fileBlockCacheContainer;
    fsOptions.redirectPhysicalRoot = _tabletOptions->IsOnline();

    std::string primaryRoot = _tabletOptions->FlushRemote() ? indexRoot.GetRemoteRoot() : indexRoot.GetLocalRoot();
    std::string fenceName = Fence::GenerateNewFenceName(_tabletOptions->FlushRemote(), _tabletInfos->GetTabletId());
    auto status = RecoverIndexInfo(primaryRoot, fenceName);
    if (!status.IsOK()) {
        TABLET_LOG(ERROR, "recover index from[%s][%s] failed", primaryRoot.c_str(), fenceName.c_str());
        return status;
    }
    std::string fenceRoot = PathUtil::JoinPath(primaryRoot, fenceName);
    auto [fsStatus, fsPtr] = indexlib::file_system::FileSystemCreator::Create(
                                 _tabletInfos->GetTabletName(), fenceRoot, fsOptions, metricProvider,
                                 /*isOverride=*/false, indexlib::file_system::FenceContext::NoFence())
                                 .StatusWith();
    if (!fsStatus.IsOK()) {
        TABLET_LOG(ERROR, "init file system with fence[%s][%s] failed", primaryRoot.c_str(), fenceName.c_str());
        return fsStatus;
    }
    bool exist = false;
    status = indexlib::file_system::FslibWrapper::IsExist(fenceRoot, exist).Status();
    if (!status.IsOK()) {
        TABLET_LOG(ERROR, "is exist on [%s] failed: %s", fenceRoot.c_str(), status.ToString().c_str());
        return status;
    }
    if (exist) {
        status = fsPtr
                     ->MountVersion(fenceRoot, INVALID_VERSIONID, /*logicalPath=*/"",
                                    indexlib::file_system::FSMT_READ_ONLY, nullptr)
                     .Status();
        if (!status.IsOK()) {
            TABLET_LOG(ERROR, "mount fence[%s] failed: %s", fenceRoot.c_str(), status.ToString().c_str());
            return status;
        }
    }
    fsPtr->SetDefaultRootPath(indexRoot.GetLocalRoot(), indexRoot.GetRemoteRoot());
    Fence fence(primaryRoot, fenceName, std::move(fsPtr));
    _fence = std::move(fence);
    TABLET_LOG(INFO, "end init file system, IndexRoot [%s], newFence[%s]", indexRoot.ToString().c_str(),
               _fence.GetFenceName().c_str());
    return Status::OK();
}

std::pair<Status, Version> Tablet::RecoverLatestVersion(const std::string& path) const
{
    auto directory = indexlib::file_system::Directory::GetPhysicalDirectory(path);
    Version emptyVersion;
    if (!directory) {
        auto status = Status::IOError("get path[%s] physical root failed", path.c_str());
        TABLET_LOG(ERROR, "%s", status.ToString().c_str());
        return {status, emptyVersion};
    }
    // remote index can be cleaned by VersionCleaner only
    auto [status, version] =
        IndexRecoverStrategy::RecoverLatestVersion(directory, /*cleanBrokenSegments=*/_tabletOptions->FlushLocal());
    if (!status.IsOK()) {
        TABLET_LOG(ERROR, "recover latest version from [%s] failed", path.c_str());
        return {status, emptyVersion};
    }
    return {Status::OK(), version};
}

Status Tablet::RecoverIndexInfo(const std::string& indexRoot, const std::string& fenceName)
{
    assert(!indexRoot.empty());
    auto [status, version] = RecoverLatestVersion(PathUtil::JoinPath(indexRoot, fenceName));
    if (!status.IsOK()) {
        return status;
    }
    _idGenerator->UpdateBaseVersion(version);
    auto [mainStatus, mainVersion] = RecoverLatestVersion(indexRoot);
    if (!mainStatus.IsOK()) {
        return mainStatus;
    }
    _idGenerator->UpdateBaseVersion(mainVersion);
    return Status::OK();
}

Status Tablet::FlushUnsafe()
{
    [[maybe_unused]] bool r = SealSegmentUnsafe();
    auto status = _tabletDumper->Dump(_tabletOptions->GetBuildConfig().GetDumpThreadCount());
    return status;
}

Status Tablet::Flush()
{
    TABLET_LOG(INFO, "flush tablet begin");
    Status result;
    {
        std::lock_guard<std::mutex> guard(_dataMutex);
        result = FlushUnsafe();
    }
    TABLET_LOG(INFO, "flush tablet end , result [%s]", result.ToString().c_str());
    return result;
}

Status Tablet::Seal()
{
    std::lock_guard<std::mutex> guard(_dataMutex);
    TABLET_LOG(INFO, "seal tablet begin");
    if (_sealedSourceLocator) {
        TABLET_LOG(INFO, "tablet already sealed on srouce locator [%s]",
                   _sealedSourceLocator.value().DebugString().c_str());
        return Status::OK();
    }
    auto slice = _tabletData->CreateSlice();
    if (slice.empty()) {
        TABLET_LOG(INFO, "empty tablet will not seal");
        return Status::OK();
    }
    auto status = FlushUnsafe();
    if (!status.IsOK()) {
        TABLET_LOG(ERROR, "dump failed: %s", status.ToString().c_str());
        return status;
    }
    status = _tabletDumper->Seal(_tabletOptions->GetBuildConfig().GetDumpThreadCount());
    if (!status.IsOK()) {
        TABLET_LOG(ERROR, "seal failed: %s", status.ToString().c_str());
        return status;
    }
    auto latestLocator = GetTabletInfos()->GetLatestLocator();
    _sealedSourceLocator = latestLocator;
    TABLET_LOG(INFO, "seal tablet end, latestLocator [%s]", latestLocator.DebugString().c_str());
    return Status::OK();
}

bool Tablet::SealSegmentUnsafe()
{
    if (!_tabletWriter) {
        return false;
    }
    auto segmentDumper = _tabletWriter->CreateSegmentDumper();
    if (!segmentDumper) {
        return false;
    }
    TABLET_LOG(INFO, "seal segment [%d]", segmentDumper->GetSegmentId());
    _tabletDumper->PushSegmentDumper(std::move(segmentDumper));
    CloseWriterUnsafe();
    return true;
}

// Check whether long time no dump, if true, dump the current building segment.
void Tablet::DumpSegmentOverInterval()
{
    std::lock_guard<std::mutex> guard(_dataMutex);
    if (!_tabletWriter) {
        return;
    }

    // NeedDumpOverInterval() is true when the delta of current time and last dump's timestamp is less
    // then the MaxDumpInterval, as Build() will update the last dump's timestamp also, the check is
    // under lock.
    if (!_tabletDumper->NeedDumpOverInterval()) {
        return;
    }
    bool sealed = SealSegmentUnsafe();
    TABLET_LOG(INFO, "seal segment by internal [%d s] done [%d]", _tabletOptions->GetMaxDumpIntervalSecond(), sealed);
    if (sealed) {
        auto status = ReopenNewSegment(GetTabletSchema());
        if (!status.IsOK()) {
            TABLET_LOG(ERROR, "reopen new segment after seal failed");
            CloseWriterUnsafe();
        }
    }
}

Status Tablet::CleanIndexFiles(const std::vector<versionid_t>& reservedVersionIds)
{
    if (unlikely(_tabletOptions->IsReadOnly())) {
        TABLET_LOG(WARN, "tablet is open in readonly mode, clean index do nothing");
        return Status::OK();
    }

    std::lock_guard<std::mutex> lockCleaner(_cleanerMutex);
    OnDiskIndexCleaner cleaner(_tabletInfos->GetIndexRoot().GetLocalRoot(), _tabletInfos->GetTabletName(),
                               _tabletOptions->GetBuildConfig().GetKeepVersionCount(), _tabletReaderContainer.get());
    return cleaner.Clean(reservedVersionIds);
}

Status Tablet::CleanUnreferencedDeployFiles(const std::set<std::string>& toKeepFiles)
{
    if (unlikely(_tabletOptions->IsReadOnly())) {
        TABLET_LOG(WARN, "tablet is open in readonly mode, clean index do nothing");
        return Status::OK();
    }

    std::lock_guard<std::mutex> lockCleaner(_cleanerMutex);
    OnDiskIndexCleaner cleaner(_tabletInfos->GetIndexRoot().GetLocalRoot(), _tabletInfos->GetTabletName(),
                               _tabletOptions->GetBuildConfig().GetKeepVersionCount(), _tabletReaderContainer.get());
    return cleaner.Clean(GetRootDirectory(), _tabletInfos->GetLoadedPublishVersion(), toKeepFiles);
}

Status Tablet::CheckAlterTableCompatible(const std::shared_ptr<TabletData>& tabletData,
                                         const std::shared_ptr<config::ITabletSchema>& oldSchema,
                                         const std::shared_ptr<config::ITabletSchema>& newSchema)
{
    // check ignore new schema
    // env ignore_alter_table_schema_ids = 1;2;3
    std::string ignoreAlterSchemaIdStr;
    if (autil::EnvUtil::getEnvWithoutDefault("ignore_alter_table_schema_ids", ignoreAlterSchemaIdStr)) {
        std::vector<schemaid_t> ignoreSchemaIds;
        autil::StringUtil::fromString(ignoreAlterSchemaIdStr, ignoreSchemaIds, ";");
        auto schemaId = newSchema->GetSchemaId();
        for (auto ignoreSchema : ignoreSchemaIds) {
            if (schemaId == ignoreSchema) {
                TABLET_LOG(ERROR, "alter table with ignore schema id [%d], all ignore schemas [%s]", schemaId,
                           ignoreAlterSchemaIdStr.c_str());
                return Status::InvalidArgs();
            }
        }
    }
    // check duplicate index
    for (auto indexConfig : newSchema->GetIndexConfigs()) {
        auto indexType = indexConfig->GetIndexType();
        auto indexName = indexConfig->GetIndexName();
        if (!oldSchema->GetIndexConfig(indexType, indexName)) {
            // indexConfig is add index config
            // check index config has in old segment
            auto slice = tabletData->CreateSlice();
            for (auto segment : slice) {
                auto segmentSchema = segment->GetSegmentSchema();
                if (segmentSchema->GetIndexConfig(indexType, indexName)) {
                    TABLET_LOG(ERROR, "alter table add same old index, wait old index delete, then readd");
                    return Status::InvalidArgs();
                }
            }
        }
    }
    return config::TabletSchema::CheckUpdateSchema(oldSchema, newSchema);
}

Status Tablet::AlterTable(const std::shared_ptr<config::ITabletSchema>& newSchema)
{
    if (unlikely(_tabletOptions->IsReadOnly())) {
        return Status::Unimplement("readonly tablet not support build");
    }
    TABLET_LOG(INFO, "begin alter tablet for schema[%u]", newSchema->GetSchemaId());
    auto status = TabletSchemaLoader::ResolveSchema(_tabletOptions, _tabletInfos->GetIndexRoot().GetRemoteRoot(),
                                                    newSchema.get());
    if (!status.IsOK()) {
        TABLET_LOG(ERROR, "resolve schema [%u] failed: %s", newSchema->GetSchemaId(), status.ToString().c_str());
        return status;
    }
    auto oldSchema = GetTabletSchema();
    if (oldSchema->GetSchemaId() == newSchema->GetSchemaId()) {
        TABLET_LOG(WARN, "schema [%s] already exist", newSchema->GetSchemaFileName().c_str());
        return Status::OK();
    }

    {
        std::lock_guard<std::mutex> guard(_dataMutex);
        status = CheckAlterTableCompatible(_tabletData, oldSchema, newSchema);
        if (!status.IsOK()) {
            TABLET_LOG(ERROR, "new schema [%u] not compatible with old schema [%u]: %s", newSchema->GetSchemaId(),
                       oldSchema->GetSchemaId(), status.ToString().c_str());
            return status;
        }
    }

    status = _tabletSchemaMgr->StoreSchema(*newSchema);
    if (status.IsExist()) {
        TABLET_LOG(WARN, "schema [%s] already exist", newSchema->GetSchemaFileName().c_str());
    } else if (!status.IsOK()) {
        TABLET_LOG(ERROR, "store schema [%u] failed: %s", newSchema->GetSchemaId(), status.ToString().c_str());
        return status;
    }

    std::lock_guard<std::mutex> guard(_dataMutex);
    status = DoAlterTable(newSchema);
    if (!status.IsOK()) {
        return status;
    }
    _tabletDumper->AlterTable(newSchema->GetSchemaId());
    TABLET_LOG(INFO, "end alter tablet for schema[%u]", newSchema->GetSchemaId());
    return Status::OK();
}

Status Tablet::DoAlterTable(const std::shared_ptr<config::ITabletSchema>& newSchema)
{
    bool segmentSealed = SealSegmentUnsafe();
    _tabletSchemaMgr->InsertSchemaToCache(newSchema);
    auto status = Status::OK();
    if (segmentSealed) {
        status = ReopenNewSegment(newSchema);
    } else {
        RefreshStrategy strategy = RefreshStrategy::KEEP;
        if (_tabletWriter) {
            if (_tabletWriter->IsDirty()) {
                TABLET_LOG(ERROR, "tablet writer is dirty, but segment is not sealed!");
                return Status::InternalError("tablet writer is dirty, but segment is not sealed!");
            }
            strategy = RefreshStrategy::REPLACE_BUILDING_SEGMENT;
        }
        TABLET_LOG(INFO, "refresh tablet data by [%s]",
                   strategy == RefreshStrategy::KEEP ? "keep" : "replace building");
        status = RefreshTabletData(strategy, newSchema);
    }
    if (!status.IsOK()) {
        TABLET_LOG(ERROR, "reopen new segment failed: %s", status.ToString().c_str());
        CloseWriterUnsafe();
        return status;
    }
    TABLET_LOG(INFO, "schema[%u] updated", newSchema->GetSchemaId());
    return Status::OK();
}

Status Tablet::RenewFenceLease(bool createIfNotExist)
{
    if (!_tabletOptions->IsLeader() || !_tabletOptions->FlushRemote()) {
        // only public fence need renew lease.
        return Status::OK();
    }
    auto status = _fence.RenewFenceLease(createIfNotExist);
    if (!status.IsOK()) {
        TABLET_LOG(ERROR, "%s", status.ToString().c_str());
    }
    return status;
}

std::shared_ptr<framework::ResourceCleaner> Tablet::CreateResourceCleaner()
{
    return std::make_shared<ResourceCleaner>(_tabletReaderContainer.get(), GetRootDirectory(),
                                             _tabletInfos->GetTabletName(), !_tabletOptions->IsLeader(), &_cleanerMutex,
                                             _tabletOptions->GetBackgroundTaskConfig().GetCleanResourceIntervalMs());
}

bool Tablet::StartIntervalTask()
{
    TABLET_LOG(INFO, "begin start interval task");
    if (!_taskScheduler) {
        TABLET_LOG(WARN, "TaskScheduler is nullptr.");
        return true;
    }
    _taskScheduler->CleanTasks();

    const config::BackgroundTaskConfig& taskConfig = _tabletOptions->GetBackgroundTaskConfig();

    auto startTask = [this](auto taskType, auto func, auto interval) {
        auto taskString = TaskTypeToString(taskType);
        if (interval <= 0) {
            TABLET_LOG(INFO, "task[%s] disabled, interval[%ld]", taskString, interval);
            return true;
        }
        if (!_taskScheduler->StartIntervalTask(taskString, std::move(func), std::chrono::milliseconds(interval))) {
            TABLET_LOG(ERROR, "Start interval task %s failed.", taskString);
            return false;
        }
        return true;
    };

    if (!startTask(
            TaskType::TT_RENEW_FENCE_LEASE,
            [this]() { [[maybe_unused]] auto status = RenewFenceLease(/*createIfNotExist=*/false); },
            taskConfig.GetRenewFenceLeaseIntervalMs())) {
        return false;
    }
    auto cleanResourceTask = CreateResourceCleaner();
    if (!startTask(
            TaskType::TT_CLEAN_RESOURCE,
            [this, cleanResourceTask]() {
                cleanResourceTask->Run();
                if (_enableMemReclaimer) {
                    MemoryReclaim();
                }
            },
            taskConfig.GetCleanResourceIntervalMs())) {
        return false;
    }
    if (!startTask(
            TaskType::TT_INTERVAL_DUMP, [this]() { DumpSegmentOverInterval(); }, taskConfig.GetDumpIntervalMs())) {
        return false;
    }
    if (!startTask(
            TaskType::TT_REPORT_METRICS, [this]() { ReportMetrics(GetTabletWriter()); },
            taskConfig.GetReportMetricsIntervalMs())) {
        return false;
    }
    if (!startTask(
            TaskType::TT_ASYNC_DUMP,
            [this]() { (void)_tabletDumper->Dump(_tabletOptions->GetBuildConfig().GetDumpThreadCount()); },
            taskConfig.GetAsyncDumpIntervalMs())) {
        return false;
    }
    if (_versionMerger && _tabletOptions->AutoMerge()) {
        if (!startTask(
                TaskType::TT_MERGE_VERSION,
                [this, versionMerger = _versionMerger]() {
                    versionMerger->Run()
                        .via(_taskScheduler->GetTaskScheduler()->GetExecutor())
                        .start([versionMerger = _versionMerger](future_lite::Try<std::pair<Status, versionid_t>>&&) {});
                },
                taskConfig.GetMergeIntervalMs())) {
            return false;
        }
    }
    // only for compute-storage separation
    if (_tabletOptions->GetNeedReadRemoteIndex() &&
        DeployIndexUtil::NeedSubscribeRemoteIndex(_tabletInfos->GetIndexRoot().GetRemoteRoot())) {
        if (!startTask(
                TaskType::TT_SUBSCRIBE_REMOTE_INDEX,
                [this]() {
                    DeployIndexUtil::SubscribeRemoteIndex(_tabletInfos->GetIndexRoot().GetRemoteRoot(),
                                                          _tabletInfos->GetLoadedPublishVersion().GetVersionId());
                },
                taskConfig.GetSubscribeRemoteIndexIntervalMs())) {
            return false;
        }
    }

    TABLET_LOG(INFO, "end start interval task");
    return true;
}

void Tablet::MemoryReclaim()
{
    if (_memReclaimer) {
        _memReclaimer->TryReclaim();
    }
}

void Tablet::PrepareTabletMetrics(const std::shared_ptr<TabletWriter>& tabletWriter)
{
    auto tabletData = GetTabletData();
    _tabletMetrics->UpdateMetrics(_tabletReaderContainer, tabletData, tabletWriter,
                                  _tabletOptions->GetBuildMemoryQuota(), _fence.GetFileSystem());
    _memControlStrategy->SyncMemoryQuota(_tabletMetrics);
    MemoryStatus memoryStatus = CheckMemoryStatus();
    _tabletInfos->SetMemoryStatus(memoryStatus);
    _tabletMetrics->SetMemoryStatus(memoryStatus);
    _tabletMetrics->PrintMetrics(_tabletInfos->GetTabletName(), _tabletOptions->GetPrintMetricsInterval());
}

void Tablet::ReportMetrics(const std::shared_ptr<TabletWriter>& tabletWriter)
{
    PrepareTabletMetrics(tabletWriter);
    _metricsManager->ReportMetrics();
    _fence.GetFileSystem()->ReportMetrics();
    if (tabletWriter != nullptr) {
        tabletWriter->ReportMetrics();
    }
}

MemoryStatus Tablet::CheckMemoryStatus() const
{
    auto memStatus = _memControlStrategy->CheckRealtimeIndexMemoryQuota(_tabletMetrics);
    if (memStatus != MemoryStatus::OK) {
        return memStatus;
    }
    return _memControlStrategy->CheckTotalMemoryQuota(_tabletMetrics);
}

std::shared_ptr<TabletWriter> Tablet::GetTabletWriter() const
{
    std::lock_guard<std::mutex> guard(_dataMutex);
    return _tabletWriter;
}

BuildResource Tablet::GenerateBuildResource(const std::string& counterPrefix)
{
    BuildResource buildResource;
    buildResource.memController = _tabletMemoryQuotaController;
    buildResource.metricsManager = _metricsManager.get();
    buildResource.buildDocumentMetrics = _tabletMetrics->GetBuildDocumentMetrics();
    auto buildingMemLimit = _tabletOptions->GetBuildConfig().GetBuildingMemoryLimit();
    if (buildingMemLimit == -1) {
        buildingMemLimit = GetSuggestBuildingSegmentMemoryUse();
        TABLET_LOG(INFO, "using suggest building segment memory [%s]",
                   autil::UnitUtil::GiBDebugString(buildingMemLimit).c_str());
    }
    buildResource.buildingMemLimit = buildingMemLimit;
    buildResource.counterMap = _metricsManager->GetCounterMap();
    buildResource.counterPrefix = counterPrefix;
    buildResource.buildResourceMetrics = std::make_shared<indexlib::util::BuildResourceMetrics>();
    buildResource.buildResourceMetrics->Init();
    buildResource.indexMemoryReclaimer = _memReclaimer.get();
    buildResource.consistentModeBuildThreadPool = _consistentModeBuildThreadPool;
    buildResource.inconsistentModeBuildThreadPool = _inconsistentModeBuildThreadPool;

    auto tabletData = GetTabletData();
    if (tabletData) {
        auto slice = tabletData->CreateSlice();
        for (auto seg : slice) {
            buildResource.segmentDirs.push_back({seg->GetSegmentId(), seg->GetSegmentDirectory()});
        }
    }
    return buildResource;
}

ReadResource Tablet::GenerateReadResource() const
{
    ReadResource readResource;
    readResource.metricsManager = _metricsManager.get();
    readResource.indexMemoryReclaimer = _memReclaimer;
    readResource.rootDirectory = GetRootDirectory();
    readResource.searchCache = _searchCache;
    return readResource;
}

void Tablet::SetRunAfterCloseFunction(const Tablet::RunAfterCloseFunc&& func) { _closeRunFunc = std::move(func); }

std::pair<Status, versionid_t> Tablet::ExecuteTask(const Version& sourceVersion, const std::string& taskType,
                                                   const std::string& taskName,
                                                   const std::map<std::string, std::string>& params)
{
    if (!_versionMerger) {
        RETURN2_IF_STATUS_ERROR(Status::InvalidArgs(), INVALID_VERSIONID, "version merger is nullptr");
    }
    return future_lite::coro::syncAwait(_versionMerger.get()->ExecuteTask(sourceVersion, taskType, taskName, params));
}

Status Tablet::ImportExternalFiles(const std::string& bulkloadId, const std::vector<std::string>& externalFiles,
                                   const std::shared_ptr<ImportExternalFileOptions>& options, Action action,
                                   int64_t eventTimeInSecs)
{
    std::lock_guard<std::mutex> lock(_dataMutex);
    Status status;
    if (bulkloadId.empty()) {
        status = Status::InvalidArgs("bulkload id is empty");
        TABLET_LOG(ERROR, "%s", status.ToString().c_str());
        return status;
    }
    // check ignore bulkload id
    // env IGNORE_BULKLOAD_ID = id1;id2;id3
    std::string ignoreBulkloadIdStr;
    if (autil::EnvUtil::getEnvWithoutDefault("IGNORE_BULKLOAD_ID", ignoreBulkloadIdStr)) {
        std::vector<std::string> ignoreBulkloadIds;
        autil::StringUtil::fromString(ignoreBulkloadIdStr, ignoreBulkloadIds, ";");
        for (auto ignoreBulkloadId : ignoreBulkloadIds) {
            if (bulkloadId == ignoreBulkloadId) {
                status = Status::InvalidArgs("bulkload with ignore bulkload id [%s], all ignore bulkload id [%s]",
                                             bulkloadId.c_str(), ignoreBulkloadIdStr.c_str());
                TABLET_LOG(ERROR, "%s", status.ToString().c_str());
                return status;
            }
        }
    }
    if (action == Action::ADD) {
        // avoid duplicate bulkload call
        const auto& currentOnDiskVersion = GetTabletData()->GetOnDiskVersion();
        auto indexTask = currentOnDiskVersion.GetIndexTaskQueue()->Get(BULKLOAD_TASK_TYPE, bulkloadId);
        if (indexTask != nullptr) {
            return status;
        }
        if (_tabletCommitter->HasIndexTask(BULKLOAD_TASK_TYPE, bulkloadId)) {
            return status;
        }
        RETURN_IF_STATUS_ERROR(FlushUnsafe(), "flush before import external file failed, isLeader[%d]",
                               _tabletOptions->IsLeader());
    }
    if (_tabletOptions->IsLeader()) {
        IndexTaskMetaCreator creator;
        std::map<std::string, std::string> params;
        std::string taskName = "bulkload";
        params[PARAM_BULKLOAD_ID] = bulkloadId;
        params[PARAM_EXTERNAL_FILES] = autil::legacy::ToJsonString(externalFiles);
        if (action == Action::ADD) {
            std::string comment;
            if (externalFiles.empty()) {
                status = Status::InvalidArgs("external file list is empty.");
            } else if (options == nullptr || !options->IsValidMode()) {
                status = Status::InvalidArgs("invalid import external file options.");
            }
            if (!status.IsOK()) {
                action = Action::SUSPEND;
                comment = status.ToString();
                TABLET_LOG(ERROR, "add index task failed, bulkload id is %s, status: %s", bulkloadId.c_str(),
                           status.ToString().c_str());
            } else {
                params[PARAM_IMPORT_EXTERNAL_FILE_OPTIONS] = autil::legacy::ToJsonString(options);
            }
            uint64_t newSegId = _idGenerator->GetNextSegmentId();
            params[PARAM_LAST_SEQUENCE_NUMBER] = autil::StringUtil::toString(newSegId << 24);
            auto meta = creator.TaskType(BULKLOAD_TASK_TYPE)
                            .TaskTraceId(bulkloadId)
                            .TaskName(taskName)
                            .Params(params)
                            .EventTimeInSecs(eventTimeInSecs)
                            .Comment(comment)
                            .Create();
            _tabletCommitter->HandleIndexTask(meta, action);
            _idGenerator->CommitNextSegmentId();
        } else if (action == Action::OVERWRITE) {
            if (externalFiles.empty()) {
                auto status = Status::InvalidArgs("external file list is empty.");
                TABLET_LOG(ERROR, "overwrite index task failed, bulkload id is %s, status: %s", bulkloadId.c_str(),
                           status.ToString().c_str());
                return status;
            }
            if (options == nullptr || !options->IsValidMode()) {
                auto status = Status::InvalidArgs("invalid import external file options.");
                TABLET_LOG(ERROR, "overwrite index task failed, bulkload id is %s, status: %s", bulkloadId.c_str(),
                           status.ToString().c_str());
                return status;
            }
            params[PARAM_IMPORT_EXTERNAL_FILE_OPTIONS] = autil::legacy::ToJsonString(options);
            auto meta = creator.TaskType(BULKLOAD_TASK_TYPE)
                            .TaskTraceId(bulkloadId)
                            .TaskName(taskName)
                            .Params(params)
                            .EventTimeInSecs(eventTimeInSecs)
                            .Create();
            _tabletCommitter->HandleIndexTask(meta, action);
        } else if (action == Action::ABORT) {
            auto meta = creator.TaskType(BULKLOAD_TASK_TYPE).TaskTraceId(bulkloadId).TaskName(taskName).Create();
            _tabletCommitter->HandleIndexTask(meta, action);
        } else if (action == Action::SUSPEND) {
            auto meta = creator.TaskType(BULKLOAD_TASK_TYPE).TaskTraceId(bulkloadId).TaskName(taskName).Create();
            _tabletCommitter->HandleIndexTask(meta, action);
        } else {
            status = Status::InvalidArgs("invalid action, usage action=%s|%s|%s|%s",
                                         ActionConvertUtil::ActionToStr(Action::ADD).c_str(),
                                         ActionConvertUtil::ActionToStr(Action::ABORT).c_str(),
                                         ActionConvertUtil::ActionToStr(Action::OVERWRITE).c_str(),
                                         ActionConvertUtil::ActionToStr(Action::SUSPEND).c_str());
        }
    }
    if (!status.IsOK()) {
        TABLET_LOG(ERROR, "import external file failed, status: %s, bulkload id %s, external files %s, options %s",
                   status.ToString().c_str(), bulkloadId.c_str(),
                   autil::legacy::ToJsonString(externalFiles, /*isCompact=*/true).c_str(),
                   autil::legacy::ToJsonString(options, /*isCompact=*/true).c_str());
        return status;
    }

    TABLET_LOG(INFO, "import external file, isLeader[%d], bulkload id %s, external files %s, options %s",
               _tabletOptions->IsLeader(), bulkloadId.c_str(),
               autil::legacy::ToJsonString(externalFiles, /*isCompact=*/true).c_str(),
               autil::legacy::ToJsonString(options, /*isCompact=*/true).c_str());
    return status;
}

Status Tablet::Import(const std::vector<Version>& versions, const ImportOptions& options)
{
    std::shared_ptr<ITabletImporter> importer = _tabletFactory->CreateTabletImporter(options.GetImportType());
    if (importer == nullptr) {
        TABLET_LOG(ERROR, "Import failed: not support import type %s.", options.GetImportType().c_str());
        return Status::Unimplement();
    }

    std::shared_ptr<TabletData> currentTabletData;
    {
        std::lock_guard<std::mutex> lock(_dataMutex);
        currentTabletData = GetTabletData();
    }
    const auto& baseVersion = currentTabletData->GetOnDiskVersion();
    std::vector<Version> validVersions;
    auto status = importer->Check(versions, &baseVersion, options, &validVersions);
    if (status.IsExist()) {
        TABLET_LOG(WARN, "input versions already exist, no need to be imported");
        return Status::OK();
    }
    RETURN_IF_STATUS_ERROR(status, "import check versions failed");
    _tabletCommitter->Import(validVersions, importer, options);
    return Status::OK();
}

std::shared_ptr<config::ITabletSchema> Tablet::GetReadSchema(const std::shared_ptr<TabletData>& tabletData)
{
    auto readSchema = tabletData->GetOnDiskVersionReadSchema();
    return readSchema ? readSchema : GetTabletSchema();
}

Version Tablet::MakeEmptyVersion(schemaid_t schemaId) const
{
    Version v;
    v.SetSchemaId(schemaId);
    v.SetReadSchemaId(schemaId);
    return v;
}

void Tablet::SetTabletData(std::shared_ptr<TabletData> tabletData)
{
    std::lock_guard<std::mutex> lock(_tabletDataMutex);
    _tabletData = tabletData;
}

std::shared_ptr<TabletData> Tablet::GetTabletData() const
{
    std::lock_guard<std::mutex> lock(_tabletDataMutex);
    return _tabletData;
}

StatusOr<std::shared_ptr<indexlibv2::framework::VersionDeployDescription>>
Tablet::CreateVersionDeployDescription(versionid_t versionId)
{
    if (!_tabletOptions->IsOnline()) {
        return nullptr;
    }
    if (versionId < 0 || (versionId & Version::PRIVATE_VERSION_ID_MASK) ||
        !_tabletOptions->GetOnlineConfig().EnableLocalDeployManifestChecking()) {
        return nullptr;
    }
    auto versionDpDesc = std::make_shared<indexlibv2::framework::VersionDeployDescription>();
    if (!indexlibv2::framework::VersionDeployDescription::LoadDeployDescription(
            _tabletInfos->GetIndexRoot().GetLocalRoot(), versionId, versionDpDesc.get())) {
        TABLET_LOG(ERROR,
                   "load version deploy description failed in CreateVersionDeployDescription, versionId[%d], "
                   "indexRootPath[%s]",
                   static_cast<int>(versionId), _tabletInfos->GetIndexRoot().GetLocalRoot().c_str());
        return Status::Corruption();
    }
    return versionDpDesc;
}

std::string Tablet::GetDiffSegmentDebugString(const Version& targetVersion,
                                              const std::shared_ptr<TabletData>& currentTabletData) const
{
    std::set<segmentid_t> currentSegmentIds;
    std::set<segmentid_t> targetSegmentIds;
    std::set<segmentid_t> allSegmentIds;
    if (currentTabletData) {
        for (auto segment : currentTabletData->CreateSlice()) {
            currentSegmentIds.insert(segment->GetSegmentId());
            allSegmentIds.insert(segment->GetSegmentId());
        }
    }
    for (auto [segmentId, schemaId] : targetVersion) {
        targetSegmentIds.insert(segmentId);
        allSegmentIds.insert(segmentId);
    }

    std::stringstream ss;
    for (auto segmentId : allSegmentIds) {
        if (ss.tellp()) {
            ss << ", ";
        }
        bool isInCurrentSegments = currentSegmentIds.count(segmentId) > 0;
        bool isInTargetSegments = targetSegmentIds.count(segmentId) > 0;
        if (!isInCurrentSegments && isInTargetSegments) {
            ss << "+" << segmentId;
        } else if (isInCurrentSegments && !isInTargetSegments) {
            auto sizeInG = autil::UnitUtil::GiB(currentTabletData->GetSegment(segmentId)->EvaluateCurrentMemUsed());
            ss << "-" << segmentId << ":" << autil::StringUtil::fToString(sizeInG, "%.1f") << "G";
        } else {
            assert(isInCurrentSegments && isInTargetSegments);
            ss << "=" << segmentId;
        }
    }
    return ss.str();
}

void Tablet::CloseWriterUnsafe()
{
    if (_tabletWriter) {
        _tabletWriter->Close();
        _tabletWriter.reset();
    }
}

#undef TABLET_LOG
} // namespace indexlibv2::framework
