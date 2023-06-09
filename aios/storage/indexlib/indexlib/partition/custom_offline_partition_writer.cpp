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
#include "indexlib/partition/custom_offline_partition_writer.h"

#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/document_collector.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/index_base/index_meta/segment_file_list_wrapper.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/segment/building_segment_data.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"
#include "indexlib/index_base/segment/segment_data_base.h"
#include "indexlib/index_base/version_committer.h"
#include "indexlib/partition/custom_partition_data.h"
#include "indexlib/partition/flushed_locator_container.h"
#include "indexlib/partition/segment/dump_segment_queue.h"
#include "indexlib/table/table_factory_wrapper.h"
#include "indexlib/table/table_resource.h"
#include "indexlib/table/table_writer.h"
#include "indexlib/util/memory_control/BlockMemoryQuotaController.h"

using namespace std;
using namespace autil;

using namespace indexlib::config;
using namespace indexlib::common;
using namespace indexlib::file_system;
using namespace indexlib::table;
using namespace indexlib::plugin;
using namespace indexlib::index;
using namespace indexlib::index_base;

using namespace indexlib::util;
namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, CustomOfflinePartitionWriter);

#define IE_LOG_PREFIX mPartitionName.c_str()

CustomOfflinePartitionWriter::CustomOfflinePartitionWriter(const IndexPartitionOptions& options,
                                                           const TableFactoryWrapperPtr& tableFactory,
                                                           const FlushedLocatorContainerPtr& flushedLocatorContainer,
                                                           MetricProviderPtr metricProvider,
                                                           const string& partitionName, const PartitionRange& range)
    : PartitionWriter(options, partitionName)
    , mTableFactory(tableFactory)
    , mFlushedLocatorContainer(flushedLocatorContainer)
    , mMetricProvider(metricProvider)
    , mStatus(util::UNKNOWN)
    , mPartitionRange(range)
    , mEnableAsyncDump(false)
{
}

CustomOfflinePartitionWriter::~CustomOfflinePartitionWriter() {}

void CustomOfflinePartitionWriter::ResetNewSegmentData(const CustomPartitionDataPtr& partitionData)
{
    ScopedLock lock(mBuildingMetaLock);
    mBuildingSegMeta = partitionData->CreateNewSegmentData();
}

NewSegmentMetaPtr CustomOfflinePartitionWriter::GetBuildingSegmentMeta() const
{
    ScopedLock lock(mBuildingMetaLock);
    return mBuildingSegMeta;
}

string CustomOfflinePartitionWriter::GetLastLocator() const
{
    ScopedLock lock(mBuildingMetaLock);
    if (mBuildingSegMeta) {
        return mBuildingSegMeta->segmentInfo.GetLocator().Serialize();
    }
    return string("");
}

void CustomOfflinePartitionWriter::Open(const config::IndexPartitionSchemaPtr& schema,
                                        const PartitionDataPtr& partitionData, const PartitionModifierPtr& modifier)
{
    assert(!modifier);
    mStatus = util::UNKNOWN;
    mSchema = schema;
    mPartitionData = DYNAMIC_POINTER_CAST(CustomPartitionData, partitionData);

    if (!mPartitionData) {
        INDEXLIB_FATAL_ERROR(BadParameter, "cast CustomPartitionData failed for shema[%s]",
                             mSchema->GetSchemaName().c_str());
    }
    mDumpSegmentQueue = mPartitionData->GetDumpSegmentQueue();
    if (!mDumpSegmentQueue) {
        INDEXLIB_FATAL_ERROR(BadParameter, "Get DumpSegmentQueue from partitionData failed for shema[%s]",
                             mSchema->GetSchemaName().c_str());
    }
    if (mOptions.IsOnline()) {
        mEnableAsyncDump =
            (mOptions.GetOnlineConfig().enableAsyncDumpSegment || mOptions.GetBuildConfig(true).EnableAsyncDump());
    } else {
        mEnableAsyncDump = mOptions.GetBuildConfig(false).EnableAsyncDump();
    }

    mRootDir = mPartitionData->GetRootDirectory();
    mFileSystem = mRootDir->GetFileSystem();
    mPartitionMemController = mPartitionData->GetPartitionMemoryQuotaController();
    mTableWriterMemController = mPartitionData->GetTableWriterMemoryQuotaController();
    assert(mPartitionMemController);
    assert(mTableWriterMemController);
    RegisterMetrics(mMetricProvider);
    ReOpenNewSegment(PartitionModifierPtr());
}

DirectoryPtr CustomOfflinePartitionWriter::PrepareNewSegmentDirectory(const SegmentDataBasePtr& newSegmentData,
                                                                      const BuildConfig& buildConfig)
{
    const string& segDirName = newSegmentData->GetSegmentDirName();
    segmentid_t segId = newSegmentData->GetSegmentId();
    const SegmentDirectoryPtr& segmentDirectory = mPartitionData->GetSegmentDirectory();
    DirectoryPtr rootDir = segmentDirectory->GetSegmentParentDirectory(segId);
    DirectoryPtr segDir;
    try {
        segDir = rootDir->MakeDirectory(segDirName, DirectoryOption::PackageMem());
    } catch (const util::ExceptionBase& e) {
        IE_LOG(WARN, "Make PackageMem Directory for [%s] fail : reason [%s]", segDirName.c_str(), e.what());
        return DirectoryPtr();
    }
    mBuildingSegDir = segDir;
    return mBuildingSegDir;
    ;
}

DirectoryPtr CustomOfflinePartitionWriter::PrepareBuildingDirForTableWriter(const DirectoryPtr& segmentDir)
{
    DirectoryPtr buildingDir;
    try {
        buildingDir = segmentDir->MakeDirectory(CUSTOM_DATA_DIR_NAME, DirectoryOption::Mem());
    } catch (const util::ExceptionBase& e) {
        IE_LOG(WARN, "Prepare custom data dir for segment[%s] fail : reason [%s]", segmentDir->DebugString().c_str(),
               e.what());
        return DirectoryPtr();
    }
    return buildingDir;
}

void CustomOfflinePartitionWriter::ReOpenNewSegment(const PartitionModifierPtr& modifier)
{
    ScopedLock lock1(mPartDataLock);
    ScopedLock lock2(mWriterLock);
    assert(!modifier);
    if (!PrepareTableResource(mPartitionData, mTableResource)) {
        INDEXLIB_FATAL_ERROR(BadParameter, "create TableResource failed for shema[%s]",
                             mSchema->GetSchemaName().c_str());
    }

    const BuildConfig& buildConfig = mOptions.GetBuildConfig();
    ResetNewSegmentData(mPartitionData);
    SegmentDataBasePtr& newSegData = mBuildingSegMeta->segmentDataBase;

    DirectoryPtr newSegDir = PrepareNewSegmentDirectory(newSegData, buildConfig);
    if (!newSegDir) {
        INDEXLIB_FATAL_ERROR(Runtime, "prepare new segment directory [%d] failed for schema[%s]",
                             newSegData->GetSegmentId(), mSchema->GetSchemaName().c_str());
    }
    DirectoryPtr customDataDir = PrepareBuildingDirForTableWriter(newSegDir);
    mTableWriter = InitTableWriter(newSegData, customDataDir, mTableResource, mPartitionData->GetPluginManager());
    if (!mTableWriter) {
        INDEXLIB_FATAL_ERROR(Runtime, "InitTableWriter in segment[%d] failed for schema[%s]",
                             newSegData->GetSegmentId(), mSchema->GetSchemaName().c_str());
    }
    ReportPartitionMetrics(mPartitionData, mBuildingSegMeta);
}

void CustomOfflinePartitionWriter::ReportPartitionMetrics(const PartitionDataPtr& partitionData,
                                                          const NewSegmentMetaPtr& buildingSegMeta)
{
    if (partitionData) {
        CustomPartitionDataPtr customPartData = DYNAMIC_POINTER_CAST(CustomPartitionData, partitionData);
        SegmentDataVector segDataVec = customPartData->GetSegmentDatas();
        IE_REPORT_METRIC(segmentCount, (segDataVec.size() + 1));
    }
    if (buildingSegMeta) {
        IE_REPORT_METRIC(partitionDocCount, buildingSegMeta->segmentDataBase->GetBaseDocId());
    }
}

docid_t CustomOfflinePartitionWriter::UpdateBuildingSegmentMeta(const document::DocumentPtr& doc)
{
    mBuildingSegMeta->segmentInfo.Update(doc);
    return mBuildingSegMeta->segmentInfo.docCount;
}

bool CustomOfflinePartitionWriter::BuildDocument(const document::DocumentPtr& doc)
{
    ScopedLock lock(mWriterLock);
    assert(mTableWriter);
    docid_t docCount = UpdateBuildingSegmentMeta(doc);
    typedef TableWriter::BuildResult BuildResult;
    BuildResult result = mTableWriter->Build(docCount, doc);
    mLastConsumedMessageCount = mTableWriter->GetLastConsumedMessageCount();
    mBuildDocumentMetrics->ReportBuildDocumentMetrics(doc, /*successMsgCount=*/mLastConsumedMessageCount);
    mTableWriter->UpdateMemoryUse();
    mBuildingSegMeta->segmentInfo.docCount = mBuildingSegMeta->segmentInfo.docCount + mLastConsumedMessageCount;
    switch (result) {
    case BuildResult::BR_OK:
        mStatus = util::OK;
        return true;
    case BuildResult::BR_SKIP:
        // TODO: metrics
        mStatus = util::OK;
        return false;
    case BuildResult::BR_NO_SPACE:
        mStatus = util::NO_SPACE;
        return false;
    case BuildResult::BR_FAIL:
        mStatus = util::INVALID_ARGUMENT;
        return false;
    case BuildResult::BR_FATAL:
        mStatus = util::FAIL;
        INDEXLIB_FATAL_ERROR(Runtime, "build document [%ld] failed", mBuildingSegMeta->segmentInfo.docCount);
    default:
        INDEXLIB_FATAL_ERROR(Runtime, "build document [%ld] for unknown result [%d]",
                             mBuildingSegMeta->segmentInfo.docCount, static_cast<int>(result));
    }
    return true;
}

bool CustomOfflinePartitionWriter::NeedDump(size_t memoryQuota, const document::DocumentCollector* docCollector) const
{
    ScopedLock lock(mWriterLock);
    if (mBuildingSegMeta->segmentInfo.docCount + (docCollector ? docCollector->Size() : 0) >=
        mOptions.GetBuildConfig().maxDocCount) {
        IE_LOG(INFO,
               "segment docCount reach max-doc-count(%d), "
               "will trigger dump",
               mOptions.GetBuildConfig().maxDocCount);
        return true;
    }

    assert(mTableWriter);
    assert(mTableResource);
    if (mTableWriter->IsFull()) {
        IE_LOG(INFO, "TableWriter [%s] is full, will trigger dump", mSchema->GetSchemaName().c_str());
        return true;
    }
    size_t buildMemUse = EstimateBuildMemoryUse();
    if (!HasResourceMemoryLimit()) {
        buildMemUse += mTableResource->GetCurrentMemoryUse();
    }

    if (mOptions.IsOnline()) {
        if (buildMemUse >= memoryQuota * MEMORY_USE_RATIO) {
            IE_LOG(INFO, "buildMemUse[%lu] reach memoryQuota, will trigger dump", buildMemUse);
            return true;
        }
        return false;
    }
    // offline logic
    if (buildMemUse >= memoryQuota * MEMORY_USE_RATIO) {
        IE_LOG(INFO, "buildMemUse[%zu] reach memoryQuota, will trigger dump", buildMemUse);
        return true;
    }
    size_t fsMemUse = GetFileSystemMemoryUse();
    if (fsMemUse + buildMemUse >= memoryQuota) {
        int64_t latency = 0;
        {
            ScopedTime timer(latency);
            CleanFileSystemCache();
        }
        IE_PREFIX_LOG(DEBUG, "clean resource elapse %ld ms", latency / 1000);
    }
    fsMemUse = GetFileSystemMemoryUse();
    if (fsMemUse >= memoryQuota) {
        INDEXLIB_FATAL_ERROR(OutOfMemory,
                             "[%s] no memory for segment writer, [memory quota %lu MB],"
                             "[FileSystemMemory %lu MB]",
                             IE_LOG_PREFIX, memoryQuota / (1024 * 1024), fsMemUse / (1024 * 1024));
    }
    if (fsMemUse + buildMemUse >= memoryQuota) {
        IE_PREFIX_LOG(INFO,
                      "estimate current segment memory use [%lu] and "
                      "fileSystem memory use [%lu] is greater than memory quota [%lu] ",
                      buildMemUse, fsMemUse, memoryQuota);
        return true;
    }
    return false;
}

size_t CustomOfflinePartitionWriter::EstimateBuildMemoryUse() const
{
    ScopedLock lock(mWriterLock);
    assert(mTableWriter);
    size_t currentSegmentMemUse = mTableWriter->GetCurrentMemoryUse();
    size_t currentDumpMemUse = mTableWriter->EstimateDumpMemoryUse();
    if (mOptions.IsOffline()) {
        return currentSegmentMemUse + currentDumpMemUse + mTableWriter->EstimateDumpFileSize();
    }
    return currentSegmentMemUse + currentDumpMemUse;
}

bool CustomOfflinePartitionWriter::IsDirty() const
{
    ScopedLock lock(mWriterLock);
    if (!mTableWriter) {
        return false;
    }
    if (mTableWriter->IsDirty()) {
        return true;
    }
    return false;
}

void CustomOfflinePartitionWriter::CleanFileSystemCache() const
{
    if (mOptions.IsOnline()) {
        return;
    }
    if (mFileSystem) {
        mFileSystem->Sync(true).GetOrThrow();
        mFileSystem->CleanCache();
    }
}

void CustomOfflinePartitionWriter::StoreSegmentMetas(const SegmentInfoPtr& segmentInfo,
                                                     BuildSegmentDescription& segDescription)
{
    // TODO: store counterMap
    const BuildConfig& buildConfig __attribute__((unused)) = mOptions.GetBuildConfig();
    if (segDescription.useSpecifiedDeployFileList) {
        fslib::FileList customFileList;
        customFileList.reserve(segDescription.deployFileList.size() + 1);
        for (const auto& file : segDescription.deployFileList) {
            customFileList.push_back(util::PathUtil::JoinPath(CUSTOM_DATA_DIR_NAME, file));
        }
        customFileList.push_back(PathUtil::NormalizeDir(CUSTOM_DATA_DIR_NAME));
        SegmentFileListWrapper::Dump(mBuildingSegDir, customFileList, segmentInfo);
    } else {
        SegmentFileListWrapper::Dump(mBuildingSegDir, segmentInfo);
    }
    segmentInfo->Store(mBuildingSegDir);
}

void CustomOfflinePartitionWriter::CommitVersion(segmentid_t segmentId, const SegmentInfo& segmentInfo,
                                                 const BuildSegmentDescription& segDescription)
{
    ScopedLock lock(mPartDataLock);
    const auto& preVersion = mPartitionData->GetVersion();

    mPartitionData->AddDumpedSegment(mBuildingSegMeta->segmentDataBase->GetSegmentId(), segmentInfo,
                                     segDescription.segmentStats);

    // TODO: isEntireDataSet flag is confusing, disable it
    if (segDescription.isEntireDataSet) {
        const auto& preSegList = preVersion.GetSegmentVector();
        if (mOptions.IsOffline()) {
            mPartitionData->RemoveSegments(preSegList);
        } else {
            vector<segmentid_t> preRtSegList;
            for (const auto& segId : preSegList) {
                if (RealtimeSegmentDirectory::IsRtSegmentId(segId)) {
                    preRtSegList.push_back(segId);
                }
            }
            mPartitionData->RemoveSegments(preRtSegList);
        }
    }
    mPartitionData->CommitVersion();
}

void CustomOfflinePartitionWriter::DumpSegment()
{
    ScopedLock lock(mPartDataLock);
    if (!IsDirty()) {
        if (mBuildingSegDir && mBuildingSegMeta) {
            const SegmentDirectoryPtr& segmentDirectory = mPartitionData->GetSegmentDirectory();
            const segmentid_t segId = mBuildingSegMeta->segmentDataBase->GetSegmentId();
            DirectoryPtr rootDir = segmentDirectory->GetSegmentParentDirectory(segId);
            const string& segDirName = mBuildingSegMeta->segmentDataBase->GetSegmentDirName();
            rootDir->RemoveDirectory(segDirName);
            ReportPartitionMetrics(mPartitionData, mBuildingSegMeta);
        }
        CleanResourceAfterDump();
        return;
    }
    CleanFileSystemCache();
    CustomSegmentDumpItemPtr segmentDumpItem(new CustomSegmentDumpItem(mOptions, mSchema, mPartitionName));

    segmentDumpItem->Init(mdumpSegmentLatencyMetric, mRootDir, mModifier, mFlushedLocatorContainer, true,
                          mBuildingSegMeta, mBuildingSegDir, mTableWriter);

    if (mEnableAsyncDump) {
        mDumpSegmentQueue->PushDumpItem(segmentDumpItem);
    } else {
        segmentDumpItem->Dump();
        // // TODO: InMemorySegment::UpdateMemUse
        CommitVersion(segmentDumpItem->GetSegmentId(), *(segmentDumpItem->GetSegmentInfo()),
                      segmentDumpItem->GetSegmentDescription());
    }
    ReportPartitionMetrics(mPartitionData, mBuildingSegMeta);
    CleanResourceAfterDump();
}

void CustomOfflinePartitionWriter::CleanResourceAfterDump()
{
    mStatus = util::UNKNOWN;
    mBuildingSegMeta.reset();
    mBuildingSegDir.reset();

    ScopedLock lock(mWriterLock);
    mTableWriter.reset();
}

void CustomOfflinePartitionWriter::DoClose()
{
    ScopedLock lock(mPartDataLock);
    ReportMetrics();
    DumpSegment();
    CleanResourceAfterDump();
    DirectoryPtr rootDirectory = mPartitionData->GetRootDirectory();
    rootDirectory->Sync(true);
    mPartitionData->CommitVersion();
}

void CustomOfflinePartitionWriter::Release()
{
    ScopedLock lock(mWriterLock);
    mTableWriter.reset();
    mTableResource.reset();
    mPartitionMemController.reset();
}

void CustomOfflinePartitionWriter::Close()
{
    DoClose();
    DirectoryPtr rootDirectory = mPartitionData->GetRootDirectory();
    if (mOptions.IsOnline()) {
        file_system::DirectoryPtr dir = file_system::Directory::GetPhysicalDirectory(rootDirectory->GetOutputPath());
        VersionCommitter::CleanVersions(dir, mOptions.GetBuildConfig().keepVersionCount,
                                        mOptions.GetReservedVersions());
    }
    Release();
}

bool CustomOfflinePartitionWriter::PrepareTableResource(const CustomPartitionDataPtr& partitionData,
                                                        TableResourcePtr& tableResource)
{
    SegmentDataVector segDataVec = partitionData->GetSegmentDatas();
    vector<SegmentMetaPtr> segMetas;
    segMetas.reserve(segDataVec.size());
    for (const auto& segData : segDataVec) {
        SegmentMetaPtr segMeta(new SegmentMeta);
        segMeta->segmentDataBase = SegmentDataBase(segData);
        segMeta->segmentInfo = *segData.GetSegmentInfo();
        segMeta->type = SegmentMeta::SegmentSourceType::INC_BUILD;

        DirectoryPtr segDir = segData.GetDirectory();
        assert(segDir);

        DirectoryPtr dataDir = segDir->GetDirectory(CUSTOM_DATA_DIR_NAME, false);
        if (!dataDir) {
            IE_LOG(ERROR, "init TableResource[%s] failed: data dir [%s] not found in segment[%s]",
                   mSchema->GetSchemaName().c_str(), CUSTOM_DATA_DIR_NAME.c_str(), segDir->DebugString().c_str());
            return false;
        }
        segMeta->segmentDataDirectory = dataDir;
        segMetas.push_back(segMeta);
    }
    if (!tableResource) {
        tableResource = mTableFactory->CreateTableResource();
        if (!CheckMemoryLimit(mTableResource->EstimateInitMemoryUse(segMetas), 0)) {
            return false;
        }
        tableResource->SetMetricProvider(mMetricProvider);

        if (!tableResource->Init(segMetas, mSchema, mOptions)) {
            IE_LOG(ERROR, "init TableResource[%s] failed ", mSchema->GetSchemaName().c_str());
            return false;
        }
        if (!CheckMemoryLimit(tableResource->GetCurrentMemoryUse(), 0)) {
            return false;
        }
        IE_LOG(INFO, "init TableResource[%s] done, metricProvider[%p]", mSchema->GetSchemaName().c_str(),
               mMetricProvider.get());
        return true;
    }

    if (!CheckMemoryLimit(tableResource->EstimateInitMemoryUse(segMetas), 0)) {
        return false;
    }

    if (!tableResource->ReInit(segMetas)) {
        IE_LOG(ERROR, "re-init TableResource[%s] failed ", mSchema->GetSchemaName().c_str());
        return false;
    }
    if (!CheckMemoryLimit(tableResource->GetCurrentMemoryUse(), 0)) {
        return false;
    }

    return true;
}

TableWriterPtr CustomOfflinePartitionWriter::InitTableWriter(const SegmentDataBasePtr& newSegmentData,
                                                             const DirectoryPtr& dataDirectory,
                                                             const TableResourcePtr& tableResource,
                                                             const PluginManagerPtr& pluginManager)
{
    TableWriterPtr tableWriter = mTableFactory->CreateTableWriter();

    TableWriterInitParamPtr initParam(new TableWriterInitParam);
    initParam->schema = mSchema;
    initParam->options = mOptions;
    initParam->rootDirectory = mPartitionData->GetRootDirectory();
    initParam->newSegmentData = newSegmentData;
    initParam->segmentDataDirectory = dataDirectory;
    initParam->tableResource = tableResource;
    initParam->onDiskVersion = mPartitionData->GetOnDiskVersion();
    initParam->partitionRange = mPartitionRange;
    util::UnsafeSimpleMemoryQuotaControllerPtr segMemControl(
        new util::UnsafeSimpleMemoryQuotaController(mTableWriterMemController));

    size_t initMemUse = tableWriter->EstimateInitMemoryUse(initParam);

    if (!CheckMemoryLimit(tableResource->GetCurrentMemoryUse(), initMemUse)) {
        return TableWriterPtr();
    }

    if (mOptions.IsOffline()) {
        // reserve mem to avoid small segments.
        //   rt builder can not recover from error, we just set this limit on offline builder.
        if (!mTableWriterMemController->Reserve(initMemUse)) {
            IE_LOG(ERROR, "reseve writer init mem [%ld] failed, create TableWriter failed", initMemUse);
            return TableWriterPtr();
        }
    }
    segMemControl->Allocate(initMemUse);
    initParam->memoryController = segMemControl;
    if (!tableWriter->Init(initParam)) {
        return TableWriterPtr();
    }
    if (!CheckMemoryLimit(tableResource->GetCurrentMemoryUse(), tableWriter->GetCurrentMemoryUse())) {
        return TableWriterPtr();
    }
    return tableWriter;
}

void CustomOfflinePartitionWriter::ReportMetrics() const
{
    ScopedLock lock(mWriterLock);
    size_t writerMemUse = 0;
    size_t resourceMemUse = 0;
    if (mTableWriter) {
        writerMemUse = mTableWriter->GetCurrentMemoryUse();
    }
    if (mTableResource) {
        resourceMemUse = mTableResource->GetCurrentMemoryUse();
    }

    IE_REPORT_METRIC(TableWriterMemoryUse, writerMemUse);
    IE_REPORT_METRIC(TableResourceMemoryUse, resourceMemUse);
    IE_REPORT_METRIC(AliveTableWriterMemoryUse, mTableWriterMemController->GetUsedQuota());
}

bool CustomOfflinePartitionWriter::CheckMemoryLimit(size_t resourceMemUse, size_t writerMemUse) const
{
    size_t memUseToCheck = writerMemUse;
    if (HasResourceMemoryLimit()) {
        size_t resourceMemLimit = mOptions.GetBuildConfig().buildResourceMemoryLimit * 1024 * 1024;
        if (resourceMemUse > resourceMemLimit) {
            IE_LOG(ERROR,
                   "current resource memory use [%ld] "
                   "exceeds build_resource_memory_limit [%ld]",
                   resourceMemUse, resourceMemLimit);
            return false;
        }
    } else {
        memUseToCheck += resourceMemUse;
    }
    size_t buildTotalMemLimit = mOptions.GetBuildConfig().buildTotalMemory * 1024 * 1024;
    if (memUseToCheck > buildTotalMemLimit) {
        IE_LOG(ERROR,
               "MemoryLimit exceeded! resourceMemUse[%zu], "
               "writerMemUse(%zu), build_total_memory[%zu]",
               resourceMemUse, writerMemUse, buildTotalMemLimit);
        return false;
    }
    return true;
}

uint64_t CustomOfflinePartitionWriter::GetTotalMemoryUse() const
{
    ScopedLock lock(mWriterLock);
    uint64_t memUse = 0u;
    if (mTableWriter) {
        memUse += mTableWriter->GetCurrentMemoryUse();
    }
    if (mTableResource) {
        memUse += mTableResource->GetCurrentMemoryUse();
    }
    return memUse;
}

uint64_t CustomOfflinePartitionWriter::GetFileSystemMemoryUse() const { return mFileSystem->GetFileSystemMemoryUse(); }

uint64_t CustomOfflinePartitionWriter::GetBuildingSegmentDumpExpandSize() const
{
    ScopedLock lock(mWriterLock);
    uint64_t memUse = 0u;
    if (mTableWriter) {
        memUse += mTableWriter->EstimateDumpMemoryUse();
    }
    return memUse;
}

void CustomOfflinePartitionWriter::RegisterMetrics(util::MetricProviderPtr mMetricProvider)
{
    mBuildDocumentMetrics = std::make_unique<BuildDocumentMetrics>(mMetricProvider, mSchema->GetTableType());
    mBuildDocumentMetrics->RegisterMetrics();

#define INIT_BUILD_METRIC(metric, unit)                                                                                \
    IE_INIT_METRIC_GROUP(mMetricProvider, metric, "build/" #metric, kmonitor::GAUGE, unit)

    INIT_BUILD_METRIC(TableWriterMemoryUse, "byte");
    INIT_BUILD_METRIC(TableResourceMemoryUse, "byte");
    INIT_BUILD_METRIC(dumpSegmentLatency, "ms");
    INIT_BUILD_METRIC(AliveTableWriterMemoryUse, "byte");

    IE_INIT_METRIC_GROUP(mMetricProvider, partitionDocCount, "partition/partitionDocCount", kmonitor::STATUS, "count");
    IE_INIT_METRIC_GROUP(mMetricProvider, segmentCount, "partition/segmentCount", kmonitor::STATUS, "count");
#undef INIT_BUILD_METRIC
}

void CustomOfflinePartitionWriter::ResetPartitionData(const PartitionDataPtr& partitionData,
                                                      const TableResourcePtr& tableResource)
{
    ScopedLock lock(mPartDataLock);
    mPartitionData = DYNAMIC_POINTER_CAST(CustomPartitionData, partitionData);
    mDumpSegmentQueue = mPartitionData->GetDumpSegmentQueue();
    mTableResource = tableResource;
}

#undef IE_LOG_PREFIX
}} // namespace indexlib::partition
