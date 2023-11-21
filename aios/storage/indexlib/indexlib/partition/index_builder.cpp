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
#include "indexlib/partition/index_builder.h"

#include <beeper/beeper.h>

#include "autil/EnvUtil.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/document.h"
#include "indexlib/document/document_collector.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/index_base/branch_fs.h"
#include "indexlib/index_base/index_meta/index_format_version.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/index_base/version_committer.h"
#include "indexlib/merger/parallel_partition_data_merger.h"
#include "indexlib/merger/partition_merger_creator.h"
#include "indexlib/partition/builder_branch_hinter.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/partition/index_partition_creator.h"
#include "indexlib/partition/index_partition_resource.h"
#include "indexlib/partition/offline_partition.h"
#include "indexlib/partition/on_disk_index_cleaner.h"
#include "indexlib/partition/partition_writer.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/TaskScheduler.h"
#include "indexlib/util/clock.h"
#include "indexlib/util/counter/CounterMap.h"
#include "indexlib/util/memory_control/MemoryQuotaController.h"
#include "indexlib/util/memory_control/QuotaControl.h"

using namespace std;
using namespace autil;
using namespace indexlib::index;
using namespace indexlib::document;
using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::util;
using namespace indexlib::index_base;
using namespace indexlib::merger;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, IndexBuilder);

IndexBuilder::IndexBuilder(const string& rootDir, const IndexPartitionOptions& options,
                           const IndexPartitionSchemaPtr& schema, const util::QuotaControlPtr& memoryQuotaControl,
                           const BuilderBranchHinter::Option& branchOption, MetricProviderPtr metricProvider,
                           const std::string& indexPluginPath, const PartitionRange& range)
    : mDataLock(NULL)
    , mOptions(options)
    , mSchema(schema)
    , mMemoryQuotaControl(memoryQuotaControl)
    , mMetricProvider(metricProvider)
    , mRootDir(rootDir)
    , mIsLegacyIndexVersion(false)
    , mSupportBuild(true)
    , mStatus(INITIALIZING)
    , mIndexPluginPath(indexPluginPath)
    , mPartitionRange(range)
    , mLastConsumedMessageCount(0)
    , mIsMemoryCtrlRunning(false)
{
    assert(mMemoryQuotaControl);
    IE_LOG(INFO, "memory quota for index builder is %lu MB", mMemoryQuotaControl->GetLeftQuota() / (1024 * 1024));
    BEEPER_FORMAT_REPORT_WITHOUT_TAGS(INDEXLIB_BUILD_INFO_COLLECTOR_NAME, "memory quota for index builder is %lu MB",
                                      mMemoryQuotaControl->GetLeftQuota() / (1024 * 1024));
    RewriteOfflineBuildOptions();
    _clock = std::make_unique<util::Clock>();
    mBranchHinter.reset(new BuilderBranchHinter(branchOption));
}

IndexBuilder::IndexBuilder(const string& rootDir, const ParallelBuildInfo& parallelInfo,
                           const IndexPartitionOptions& options, const IndexPartitionSchemaPtr& schema,
                           const util::QuotaControlPtr& memoryQuotaControl,
                           const BuilderBranchHinter::Option& branchOption, MetricProviderPtr metricProvider,
                           const std::string& indexPluginPath, const PartitionRange& range)
    : mDataLock(NULL)
    , mOptions(options)
    , mSchema(schema)
    , mMemoryQuotaControl(memoryQuotaControl)
    , mMetricProvider(metricProvider)
    , mRootDir(rootDir)
    , mIsLegacyIndexVersion(false)
    , mSupportBuild(true)
    , mStatus(INITIALIZING)
    , mIndexPluginPath(indexPluginPath)
    , mParallelInfo(parallelInfo)
    , mPartitionRange(range)
{
    assert(mMemoryQuotaControl);
    IE_LOG(INFO, "memory quota for index builder is %lu MB", mMemoryQuotaControl->GetLeftQuota() / (1024 * 1024));
    BEEPER_FORMAT_REPORT_WITHOUT_TAGS(INDEXLIB_BUILD_INFO_COLLECTOR_NAME, "memory quota for index builder is %lu MB",
                                      mMemoryQuotaControl->GetLeftQuota() / (1024 * 1024));
    RewriteOfflineBuildOptions();
    _clock = std::make_unique<util::Clock>();
    mBranchHinter.reset(new BuilderBranchHinter(branchOption));
}

void IndexBuilder::RewriteOfflineBuildOptions()
{
    mOptions.SetIsOnline(false);
    mOptions.GetOfflineConfig().enableRecoverIndex = true;
    if (mSchema->GetTableType() == tt_index && mOptions.GetBuildConfig().buildResourceMemoryLimit < 0) {
        int64_t defaultBuildResourceMemoryLimit = mMemoryQuotaControl->GetLeftQuota() / (1024 * 1024) *
                                                  mOptions.GetBuildConfig().GetDefaultBuildResourceMemoryLimitRatio();
        if (defaultBuildResourceMemoryLimit > 0) {
            mOptions.GetBuildConfig().buildResourceMemoryLimit = defaultBuildResourceMemoryLimit;
        }
    }
}

// use existing index partition
IndexBuilder::IndexBuilder(const IndexPartitionPtr& indexPartitionPtr, const util::QuotaControlPtr& memoryQuotaControl,
                           MetricProviderPtr metricProvider, const PartitionRange& range)
    : mDataLock(NULL)
    , mMemoryQuotaControl(memoryQuotaControl)
    , mMetricProvider(metricProvider)
    , mIsLegacyIndexVersion(false)
    , mStatus(INITIALIZING)
    , mPartitionRange(range)
    , mIsMemoryCtrlRunning(false)
{
    assert(mMemoryQuotaControl);
    IE_LOG(INFO, "memory quota for index builder is %lu MB", mMemoryQuotaControl->GetLeftQuota() / (1024 * 1024));
    mIndexPartition = indexPartitionPtr;
    mSupportBuild = SupportOnlineBuild(indexPartitionPtr->GetSchema());
    mIndexPartition->SetBuildMemoryQuotaControler(mMemoryQuotaControl);
    _clock = std::make_unique<util::Clock>();
}

void IndexBuilder::CleanMemoryControlThread()
{
    mIsMemoryCtrlRunning = false;
    if (mMemoryCtrlThread) {
        mMemoryCtrlThread.reset();
    }
}

bool IndexBuilder::CreateMemoryControlThread()
{
    if (mMemoryCtrlThread) {
        IE_LOG(ERROR, "mMemoryControlThread already exists");
        return false;
    }
    mIsMemoryCtrlRunning = true;
    mMemoryCtrlThread =
        autil::Thread::createThread(std::bind(&IndexBuilder::MemoryControlThread, this), "memoryCtrlThread");
    if (!mMemoryCtrlThread) {
        IE_LOG(ERROR, "create thread [memoryCtrlThread] failed");
        return false;
    }
    return true;
}

IndexBuilder::~IndexBuilder()
{
    CleanMemoryControlThread();
    mIndexPartitionWriter.reset();
    mIndexPartition.reset();
}

bool IndexBuilder::Init()
{
    if (!mIndexPartition) {
        assert(mOptions.IsOffline());
        // TODO: give partitionName and totalQuota(unused till now)
        // so ugly, QuotaControl(Build memory), MemoryQuotaController(total), PartitionMemoryQuotaController(partition)
        // TODO: use docker mem-limit as totalQuotaForPartition
        IndexPartitionResource partitionResource;
        partitionResource.memoryQuotaController = std::make_shared<indexlibv2::MemoryQuotaController>(
            /*name*/ "offline_builder", std::numeric_limits<int64_t>::max());
        partitionResource.taskScheduler.reset(new TaskScheduler());
        partitionResource.metricProvider = mMetricProvider;
        partitionResource.indexPluginPath = mIndexPluginPath;
        partitionResource.parallelBuildInfo = mParallelInfo;
        partitionResource.range = mPartitionRange;
        partitionResource.partitionName = "offline";
        partitionResource.branchOption = mBranchHinter->GetOption();

        mIndexPartition = CreateIndexPartition(mSchema, mOptions, partitionResource);
        mIndexPartition->SetBuildMemoryQuotaControler(mMemoryQuotaControl);
        IndexPartition::OpenStatus os = mIndexPartition->Open(mRootDir, "", mSchema, mOptions);
        if (os != IndexPartition::OS_OK) {
            IE_LOG(ERROR, "open partition from [%s] failed, OpenStatue [%d]", mRootDir.c_str(), os);
            ERROR_COLLECTOR_LOG(ERROR, "open partition from [%s] failed, OpenStatue [%d]", mRootDir.c_str(), os);
            mIndexPartition.reset();
            return false;
        }
        if (!CreateInstanceDirectoryForParallelInc()) {
            return false;
        }
    }
    mDataLock = mIndexPartition->GetDataLock();
    ScopedLock lock(*mDataLock);

    mOptions = mIndexPartition->GetOptions();
    mSchema = mIndexPartition->GetSchema();

    mIndexPartitionWriter = mIndexPartition->GetWriter();
    mPartitionMeta = mIndexPartition->GetPartitionMeta();

    mIsLegacyIndexVersion = mIndexPartition->GetIndexFormatVersion().IsLegacyFormat();

    if (mOptions.IsOnline()) {
        const DirectoryPtr& rootDirectory = mIndexPartition->GetRootDirectory();
        assert(rootDirectory);
        mRootDir = rootDirectory->GetOutputPath();
    } else if (mIndexPartitionWriter->EnableAsyncDump()) {
        if (!CreateMemoryControlThread()) {
            CleanMemoryControlThread();
            return false;
        }
    }
    mStatus = mIndexPartitionWriter ? BUILDING : INITIALIZING;
    InitWriter();
    return true;
}

void IndexBuilder::InitWriter()
{
    if (!mIndexPartitionWriter) {
        return;
    }
    PartitionWriter::BuildMode buildMode =
        IsEnableBatchBuild(false) ? PartitionWriter::BUILD_MODE_INCONSISTENT_BATCH : PartitionWriter::BUILD_MODE_STREAM;
    mIndexPartitionWriter->SwitchBuildMode(buildMode);
    _batchStartTimestampUS = _clock->Now();
    if (buildMode == PartitionWriter::BUILD_MODE_CONSISTENT_BATCH ||
        buildMode == PartitionWriter::BUILD_MODE_INCONSISTENT_BATCH) {
        assert(_docCollector == nullptr || _docCollector->Size() == 0);
        _docCollector = std::make_unique<DocumentCollector>(mOptions);
    }
}

void IndexBuilder::TEST_SetClock(std::unique_ptr<util::Clock> clock) { _clock = std::move(clock); }

bool IndexBuilder::CreateInstanceDirectoryForParallelInc()
{
    if (!mParallelInfo.IsParallelBuild()) {
        IE_LOG(INFO, "Init builder in partition path [%s]", mRootDir.c_str());
        return true;
    }

    if (BuildConfig::BP_INC != mOptions.GetBuildConfig(false).buildPhase) {
        IE_LOG(ERROR, "parallel build only support inc, not for [%d]", mOptions.GetBuildConfig(false).buildPhase);
        ERROR_COLLECTOR_LOG(ERROR, "parallel build only support inc, not for [%d]",
                            mOptions.GetBuildConfig(false).buildPhase);
        return false;
    }

    return true;
}

bool IndexBuilder::SupportOnlineBuild(const IndexPartitionSchemaPtr& schema)
{
    if (schema->GetTableType() == tt_customized) {
        return true;
    }
    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    if (!indexSchema->HasPrimaryKeyIndex()) {
        return true;
    }
    return indexSchema->GetPrimaryKeyIndexType() != it_trie;
}

file_system::DirectoryPtr IndexBuilder::TEST_GetRootDirectory() const { return mIndexPartition->GetRootDirectory(); }

IndexPartitionPtr IndexBuilder::CreateIndexPartition(const IndexPartitionSchemaPtr& schema,
                                                     const IndexPartitionOptions& options,
                                                     const IndexPartitionResource& indexPartitionResource)
{
    assert(schema);
    if (schema->GetTableType() == tt_customized) {
        return IndexPartitionCreator(indexPartitionResource).CreateCustom(options);
    }
    return IndexPartitionCreator(indexPartitionResource).CreateNormal(options);
}

bool IndexBuilder::MaybePrepareBuilder()
{
    // TODO(panghai.hj): why check reload here?
    if (mIndexPartition->NeedReload()) {
        INDEXLIB_FATAL_ERROR(InconsistentState, "load table [%s] error, need reload", mSchema->GetSchemaName().c_str());
    }
    if (unlikely(mStatus != BUILDING)) {
        if (unlikely(!mSupportBuild)) {
            IE_LOG(WARN, "Not support build, drop doc");
            ERROR_COLLECTOR_LOG(WARN, "Not support build, drop doc");
            return false;
        }
        if (mStatus != INITIALIZING) {
            INDEXLIB_FATAL_ERROR(InconsistentState, "table [%s] still in status[%d]", mSchema->GetSchemaName().c_str(),
                                 mStatus.load());
        } else {
            assert(mIndexPartitionWriter == nullptr);
            mIndexPartition->ReOpenNewSegment();
            mIndexPartitionWriter = mIndexPartition->GetWriter();
            InitWriter();
            mStatus = BUILDING;
        }
    }
    return true;
}

void IndexBuilder::MaybePrepareDoc(const document::DocumentPtr& doc)
{
    assert(mIndexPartitionWriter);
    if (mIndexPartitionWriter->NeedRewriteDocument(doc)) {
        mIndexPartitionWriter->RewriteDocument(doc);
    }
}

bool IndexBuilder::Build(const DocumentPtr& doc)
{
    ScopedLock lock(*mDataLock);
    if (!MaybePrepareBuilder()) {
        return false;
    }
    MaybePrepareDoc(doc);
    if (mIndexPartitionWriter->GetBuildMode() == PartitionWriter::BUILD_MODE_CONSISTENT_BATCH ||
        mIndexPartitionWriter->GetBuildMode() == PartitionWriter::BUILD_MODE_INCONSISTENT_BATCH) {
        return BatchBuild(doc);
    }
    return BuildSingleDocument(doc);
}

bool IndexBuilder::BuildSingleDocument(const document::DocumentPtr& doc)
{
    if (!DoBuildSingleDocument(doc)) {
        if (mIndexPartitionWriter->GetStatus() == util::NO_SPACE) {
            BEEPER_REPORT(INDEXLIB_BUILD_INFO_COLLECTOR_NAME,
                          "DumpSegment for partition writer reach [util::NO_SPACE] status");
            DoDumpSegment();
            return DoBuildSingleDocument(doc);
        }
        return false;
    }
    return true;
}

// TODO: BatchBuild might trigger GroupedThreadPool core if partition writer is closed and there is still pending build
// work item. Handle this more gracefully.
bool IndexBuilder::BatchBuild(const document::DocumentPtr& doc)
{
    _docCollector->Add(doc);
    uint64_t now = _clock->Now();
    bool shouldTrigger = _docCollector->ShouldTriggerBuild() or
                         mIndexPartitionWriter->NeedDump(mMemoryQuotaControl->GetLeftQuota(), _docCollector.get()) or
                         (now - _batchStartTimestampUS >= mOptions.GetBuildConfig().GetBatchBuildMaxCollectInterval());
    if (!shouldTrigger) {
        return true;
    }
    _batchStartTimestampUS = now;
    return DoBatchBuild();
}

void IndexBuilder::Flush()
{
    ScopedLock lock(*mDataLock);
    MaybeDoBatchBuild();
    _batchStartTimestampUS = _clock->Now();
}

bool IndexBuilder::IsEnableBatchBuild(bool isConsistentMode)
{
    return (mSchema->GetTableType() == tt_index) and mOptions.IsOnline() and
           mSchema->GetIndexSchema()->HasPrimaryKeyIndex() and
           mOptions.GetBuildConfig().IsBatchBuildEnabled(isConsistentMode);
}

void IndexBuilder::SwitchToConsistentMode()
{
    if (unlikely(!mIndexPartitionWriter || !mDataLock)) {
        return;
    }
    ScopedLock lock(*mDataLock);
    if (unlikely(mIndexPartitionWriter->GetBuildMode() == PartitionWriter::BUILD_MODE_STREAM ||
                 mIndexPartitionWriter->GetBuildMode() == PartitionWriter::BUILD_MODE_CONSISTENT_BATCH)) {
        return;
    }
    PartitionWriter::BuildMode targetBuildMode =
        IsEnableBatchBuild(true) ? PartitionWriter::BUILD_MODE_CONSISTENT_BATCH : PartitionWriter::BUILD_MODE_STREAM;

    if (mIndexPartitionWriter->GetBuildMode() == PartitionWriter::BUILD_MODE_INCONSISTENT_BATCH ||
        mIndexPartitionWriter->GetBuildMode() == PartitionWriter::BUILD_MODE_CONSISTENT_BATCH) {
        _batchStartTimestampUS = _clock->Now();
        if (!DoBatchBuild()) {
            IE_LOG(ERROR, "Batch build at BatchBuildStop failed");
        }
    }
    mIndexPartitionWriter->SwitchBuildMode(targetBuildMode);
}

bool IndexBuilder::DoBuildSingleDocument(const DocumentPtr& doc)
{
    bool ret = mIndexPartitionWriter->BuildDocument(doc);
    if (ret && mIndexPartitionWriter->NeedDump(mMemoryQuotaControl->GetLeftQuota())) {
        DoDumpSegment();
    }
    mLastConsumedMessageCount = mIndexPartitionWriter->GetLastConsumedMessageCount();
    return ret;
}

void IndexBuilder::MaybeDoBatchBuild()
{
    // IndexPartitionWriter not initialized
    if (mIndexPartitionWriter == nullptr) {
        return;
    }
    if (mIndexPartitionWriter->GetBuildMode() == PartitionWriter::BUILD_MODE_STREAM) {
        return;
    }
    if (_docCollector == nullptr) {
        return;
    }
    if (!DoBatchBuild()) {
        IE_LOG(ERROR, "Batch build failed");
    }
}

bool IndexBuilder::DoBatchBuild()
{
    if (_docCollector->Size() == 0) {
        return true;
    }
    size_t leftMemoryInBytes = mMemoryQuotaControl->GetLeftQuota();
    if (mIndexPartitionWriter->NeedDump(leftMemoryInBytes)) {
        DoDumpSegment();
    }
    IE_LOG(INFO, "Start batch build with [%lu] docs and [%lu] MB memory left", _docCollector->Size(),
           leftMemoryInBytes / (1024 * 1024));

    // 交出 _docCollector 的所有权，由 PartitionWriter 负责析构。因为它的析构也是一个很耗时的操作 & 形成跨批流水。
    bool ret = mIndexPartitionWriter->BatchBuild(std::move(_docCollector));
    _docCollector = std::make_unique<DocumentCollector>(mOptions);
    if (ret && mIndexPartitionWriter->NeedDump(mMemoryQuotaControl->GetLeftQuota())) {
        DoDumpSegment();
    }
    return ret;
}

BranchFS* IndexBuilder::GetBranchFs() { return mIndexPartition ? mIndexPartition->GetBranchFileSystem() : nullptr; }
void IndexBuilder::TEST_BranchFSMoveToMainRoad()
{
    if (GetBranchFs()) {
        GetBranchFs()->TEST_MoveToMainRoad();
    }
}

void IndexBuilder::EndIndex(int64_t versionTimestamp)
{
    ScopedLock lock(*mDataLock);

    MaybeDoBatchBuild();

    if (mIndexPartitionWriter) {
        mIndexPartitionWriter->EndIndex();
    }

    mStatus = ENDINDEXING;
    if (mOptions.IsOnline()) {
        mIndexPartitionWriter.reset();
    } else {
        DirectoryPtr rootDir = mIndexPartition->GetRootDirectory();
        auto branchFileSystem = mIndexPartition->GetBranchFileSystem();
        ClosePartition();
        Version latestVersion = GetLatestOnDiskVersion();
        if (latestVersion.GetVersionId() == INVALID_VERSIONID) {
            CreateEmptyVersion(rootDir, versionTimestamp);
        } else {
            AlignVersionTimestamp(rootDir, latestVersion, versionTimestamp);
        }
        // actually finished, not just stopped
        if (versionTimestamp != INVALID_TIMESTAMP) {
            branchFileSystem->CommitToDefaultBranch(mBranchHinter.get());
        }
    }
}

void IndexBuilder::ClosePartition()
{
    CleanMemoryControlThread();
    mIndexPartitionWriter.reset();
    mIndexPartition->Close();
}

versionid_t IndexBuilder::DoMerge(const std::string& rootPath, const config::IndexPartitionSchemaPtr& schema,
                                  const IndexPartitionOptions& options, bool optimize, int64_t currentTs)
{
    IndexPartitionOptions mergeOptions = options;
    mergeOptions.SetIsOnline(false);
    mergeOptions.GetOfflineConfig().enableRecoverIndex = true;
    PartitionMergerPtr merger(PartitionMergerCreator::CreateSinglePartitionMerger(
        rootPath, mergeOptions, mMetricProvider, mIndexPluginPath, mPartitionRange, mBranchHinter->GetOption()));
    if (!merger) {
        INDEXLIB_FATAL_ERROR(Runtime, "create merger failed for schema [%s]", schema->GetSchemaName().c_str());
    }
    merger->Merge(optimize, currentTs);
    Version mergedVersion = merger->GetMergedVersion();
    return mergedVersion.GetVersionId();
}

versionid_t IndexBuilder::Merge(const IndexPartitionOptions& options, bool optimize, int64_t currentTs)
{
    ScopedLock lock(*mDataLock);
    mStatus = MERGING;
    auto schema = mIndexPartition->GetSchema();
    DirectoryPtr rootDir = mIndexPartition->GetRootDirectory();
    ClosePartition();

    Version latestVersion = GetLatestOnDiskVersion();

    if (latestVersion.GetVersionId() == INVALID_VERSIONID) {
        CreateEmptyVersion(rootDir, INVALID_TIMESTAMP);
    }

    versionid_t version = DoMerge(mRootDir, schema, options, optimize, currentTs);
    return version;
}

versionid_t IndexBuilder::GetVersion()
{
    ScopedLock lock(*mDataLock);
    Version latestVersion;
    VersionLoader::GetVersion(mIndexPartition->GetRootDirectory(), latestVersion, INVALID_VERSIONID);
    return latestVersion.GetVersionId();
}

void IndexBuilder::StoreBranchCkp()
{
    if (GetBranchFs() != nullptr && !IsEndIndex()) {
        // string checkpoint;
        // GetLastLocator(checkpoint);
        mBranchHinter->StorePorgressIfNeed(mIndexPartition->GetRootPath(), GetBranchFs()->GetBranchName(),
                                           GetLastFlushedLocator());
    }
}

void IndexBuilder::DumpSegment()
{
    {
        ScopedLock lock(*mDataLock);
        MaybeDoBatchBuild();
    }
    DoDumpSegment();
}

void IndexBuilder::DoDumpSegment()
{
    ScopedLock lock(*mDataLock);
    if (unlikely(mStatus != BUILDING)) {
        IE_LOG(WARN, "current status[%d] can not call dump segment", mStatus.load());
        return;
    }
    assert(mIndexPartitionWriter);
    mIndexPartitionWriter->EndIndex();

    mStatus = DUMPING;
    mIndexPartitionWriter->DumpSegment();
    mIndexPartition->ReOpenNewSegment();
    mStatus = BUILDING;
}
bool IndexBuilder::GetLastLocator(std::string& locator) const
{
    locator = GetLastLocator();
    return true;
}

bool IndexBuilder::GetIncVersionTimestamp(int64_t& ts) const
{
    ts = GetIncVersionTimestamp();
    return true;
}

string IndexBuilder::GetLastLocator() const
{
    if (mIsLegacyIndexVersion) {
        return "";
    }

    if (!mIndexPartition) {
        IE_LOG(WARN, "get last locator from index partition failed!");
        return "";
    }
    return mIndexPartition->GetLastLocator();
}

int64_t IndexBuilder::GetIncVersionTimestamp() const
{
    assert(mIndexPartition);
    return mIndexPartition->GetRtSeekTimestampFromIncVersion();
}

string IndexBuilder::GetLastFlushedLocator()
{
    document::Locator locator = mIndexPartition->GetLastFlushedLocator();
    string locatorStr = locator.ToString();
    if (locatorStr.empty() || mIsLegacyIndexVersion) {
        return "";
    }
    return locatorStr;
}

Version IndexBuilder::GetLatestOnDiskVersion()
{
    ScopedLock lock(*mDataLock);
    Version latestVersion;
    VersionLoader::GetVersion(mIndexPartition->GetRootDirectory(), latestVersion, INVALID_VERSIONID);
    return latestVersion;
}

void IndexBuilder::CreateEmptyVersion(const file_system::DirectoryPtr& rootDirectory, int64_t versionTimestamp)
{
    Version emptyVersion;
    indexlibv2::framework::LevelInfo& levelInfo = emptyVersion.GetLevelInfo();
    const BuildConfig& buildConfig = mOptions.GetBuildConfig();
    levelInfo.Init(buildConfig.levelTopology, buildConfig.levelNum, buildConfig.shardingColumnNum);
    emptyVersion.SetVersionId(0);
    emptyVersion.SyncSchemaVersionId(mSchema);
    emptyVersion.SetTimestamp(versionTimestamp);
    emptyVersion.SetDescriptions(mOptions.GetVersionDescriptions());
    emptyVersion.SetUpdateableSchemaStandards(mOptions.GetUpdateableSchemaStandards());

    if (mParallelInfo.IsParallelBuild()) {
        // build version
        emptyVersion.Store(rootDirectory, true);
    } else {
        VersionCommitter committer(rootDirectory, emptyVersion, INVALID_KEEP_VERSION_COUNT);
        committer.Commit();
    }
}

void IndexBuilder::AlignVersionTimestamp(const DirectoryPtr& rootDir, const Version& latestVersion,
                                         int64_t alignedTimestamp)
{
    if (alignedTimestamp == INVALID_TIMESTAMP) {
        return;
    }
    if (latestVersion.GetTimestamp() == alignedTimestamp) {
        return;
    }
    Version newVersion(latestVersion);
    newVersion.SetFormatVersion(Version::CURRENT_FORMAT_VERSION);
    newVersion.IncVersionId();
    newVersion.SetTimestamp(alignedTimestamp);
    newVersion.SetDescriptions(mOptions.GetVersionDescriptions());
    newVersion.SetUpdateableSchemaStandards(mOptions.GetUpdateableSchemaStandards());

    if (rootDir) {
        if (mParallelInfo.IsParallelBuild()) {
            newVersion.Store(rootDir, true);
        } else {
            VersionCommitter committer(rootDir, newVersion, INVALID_KEEP_VERSION_COUNT);
            committer.Commit();
        }
        rootDir->Sync(true);
    } else {
        assert(0);
        INDEXLIB_FATAL_ERROR(Runtime, "unexpected branch, not use currently");
    }
}

string IndexBuilder::GetLocatorInLatestVersion() const
{
    auto rootDirectory = mIndexPartition->GetRootDirectory();
    Version latestVersion;
    VersionLoader::GetVersion(rootDirectory, latestVersion, INVALID_VERSIONID);

    segmentid_t latestSegId = INVALID_SEGMENTID;
    if (latestVersion.GetSegmentCount() == 0) {
        // empty version
        return "";
    }
    latestSegId = latestVersion.GetSegmentVector().back();
    std::string segmentPath = latestVersion.GetSegmentDirName(latestSegId);
    auto segmentDir = rootDirectory->GetDirectory(segmentPath, false);
    if (segmentDir == nullptr) {
        INDEXLIB_FATAL_ERROR(Runtime, "get directory failed, path[%s/%s]", mRootDir.c_str(), segmentPath.c_str());
    }

    SegmentInfo segInfo;
    if (!segInfo.Load(segmentDir)) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "table [%s] load segment info failed for segment : [%d]",
                             mSchema->GetSchemaName().c_str(), latestSegId);
    }
    return segInfo.GetLocator().Serialize();
}

const CounterMapPtr& IndexBuilder::GetCounterMap()
{
    if (mIndexPartition) {
        return mIndexPartition->GetCounterMap();
    }
    static CounterMapPtr counterMap = CounterMapPtr();
    return counterMap;
}

bool IndexBuilder::IsDumping() { return mIndexPartition->HasFlushingLocator() || (mStatus == DUMPING); }
bool IndexBuilder::IsEndIndex() { return mStatus == ENDINDEXING; }

const IndexPartitionPtr& IndexBuilder::GetIndexPartition() const { return mIndexPartition; }

void IndexBuilder::MemoryControlThread()
{
    IE_LOG(INFO, "Memory control thread start");
    int32_t sleepTime = MEMORY_CTRL_THREAD_INTERVAL;
    if (autil::EnvUtil::getEnv("TEST_QUIT_EXIT", false)) {
        sleepTime /= 1000;
    }
    while (mIsMemoryCtrlRunning) {
        if (mIndexPartition) {
            mIndexPartition->ExecuteBuildMemoryControl();
        }
        usleep(sleepTime);
    }
}
}} // namespace indexlib::partition
