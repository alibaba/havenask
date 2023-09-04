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
#include "indexlib/partition/online_partition.h"

#include "autil/EnvUtil.h"
#include "autil/HashAlgorithm.h"
#include "autil/NetUtil.h"
#include "autil/TimeUtility.h"
#include "indexlib/common/executor_scheduler.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/kv_index_config.h"
#include "indexlib/document/index_locator.h"
#include "indexlib/file_system/FileSystemMetricsReporter.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/LifecycleTable.h"
#include "indexlib/file_system/MountOption.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/common/numeric_compress/EncoderProvider.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_plugin_resource.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/index_base/index_meta/index_format_version.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/index_base/lifecycle_strategy.h"
#include "indexlib/index_base/online_join_policy.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/join_segment_directory.h"
#include "indexlib/index_base/segment/partition_segment_iterator.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/index_base/version_committer.h"
#include "indexlib/partition/building_partition_data.h"
#include "indexlib/partition/deploy_index_validator.h"
#include "indexlib/partition/in_mem_virtual_attribute_cleaner.h"
#include "indexlib/partition/in_memory_index_cleaner.h"
#include "indexlib/partition/index_partition_resource.h"
#include "indexlib/partition/local_index_cleaner.h"
#include "indexlib/partition/memory_stat_collector.h"
#include "indexlib/partition/memory_stat_reporter.h"
#include "indexlib/partition/modifier/partition_modifier_creator.h"
#include "indexlib/partition/on_disk_index_cleaner.h"
#include "indexlib/partition/on_disk_partition_data.h"
#include "indexlib/partition/on_disk_realtime_data_calculator.h"
#include "indexlib/partition/online_partition_reader.h"
#include "indexlib/partition/online_partition_writer.h"
#include "indexlib/partition/open_executor/dump_segment_executor.h"
#include "indexlib/partition/open_executor/open_executor_util.h"
#include "indexlib/partition/open_executor/realtime_index_recover_executor.h"
#include "indexlib/partition/operation_queue/operation_iterator.h"
#include "indexlib/partition/operation_queue/operation_replayer.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/partition/partition_reader_cleaner.h"
#include "indexlib/partition/patch_loader.h"
#include "indexlib/partition/segment/async_segment_dumper.h"
#include "indexlib/partition/segment/dump_segment_container.h"
#include "indexlib/partition/segment/in_memory_segment_cleaner.h"
#include "indexlib/partition/segment/in_memory_segment_container.h"
#include "indexlib/partition/segment/normal_segment_dump_item.h"
#include "indexlib/partition/table_reader_container_updater.h"
#include "indexlib/partition/virtual_attribute_container.h"
#include "indexlib/plugin/index_plugin_loader.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/TaskScheduler.h"
#include "indexlib/util/counter/CounterMap.h"
#include "indexlib/util/counter/StateCounter.h"
#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"
#include "indexlib/util/memory_control/MemoryQuotaSynchronizer.h"
#include "indexlib/util/memory_control/MemoryReserver.h"

using namespace std;
using namespace autil;
using namespace indexlib::util;
using namespace indexlib::config;
using namespace indexlib::common;
using namespace indexlib::index;
using namespace indexlib::file_system;
using namespace indexlib::plugin;
using namespace indexlib::document;
using namespace indexlib::index_base;
using namespace indexlibv2::index;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, OnlinePartition);

#define IE_LOG_PREFIX mPartitionName.c_str()

OnlinePartition::OnlinePartition() : OnlinePartition(IndexPartitionResource::Create(IndexPartitionResource::IR_ONLINE))
{
}

OnlinePartition::OnlinePartition(const IndexPartitionResource& partitionResource)
    : IndexPartition(partitionResource)
    , mSearchCache(partitionResource.searchCache)
    , mOnlinePartMetrics(new OnlinePartitionMetrics(partitionResource.metricProvider))
    , mRealtimeIndexSyncThreadPool(partitionResource.realtimeIndexSyncThreadPool)
    , mClosed(true)
    , mIsServiceNormal(false)
    , mIndexPhase(PENDING)
    , mEnableAsyncDump(false)
    , mLatestNeedSkipIncTs(INVALID_TIMESTAMP)
    , mCheckSecondIndexIntervalInMin(EnvUtil::getEnv<int64_t>("SECONDARY_INDEX_CHECK_INTERVAL", -1))
    , mSubscribeSecondIndexIntervalInMin(EnvUtil::getEnv<int64_t>("SECONDARY_INDEX_SUBSCRIBE_INTERVAL", -1))
    , mMissingSegmentCount(0)
    , mFutureExecutor(nullptr)
    , mRecoverMaxTs(-1)
    , mNeedReportTemperature(false)
    , mSrcSignature(partitionResource.srcSignature)
    , mMaxRedoTime(-1)
{
    if (partitionResource.memoryStatReporter) {
        MemoryStatCollectorPtr collector = partitionResource.memoryStatReporter->GetMemoryStatCollector();
        if (collector) {
            collector->AddTableMetrics(partitionResource.partitionGroupName, mOnlinePartMetrics);
        }
    }
    mRealtimeQuotaController = partitionResource.realtimeQuotaController;
    mPartitionIdentifier = autil::HashAlgorithm::hashString64(mPartitionName.c_str(), mPartitionName.length());
    string envStr = autil::EnvUtil::getEnv("ENABLE_TEMPERATUR_ACCESS_METRIC");
    if (envStr == "true") {
        mNeedReportTemperature = true;
    }
    int64_t redoTime = -1;
    if (autil::EnvUtil::getEnvWithoutDefault("INDEXLIB_MAX_REDO_TIME", redoTime)) {
        mMaxRedoTime = redoTime;
    }
}

void OnlinePartition::Reset()
{
    mResourceCalculator.reset();
    mResourceCleaner.reset();
    mLocalIndexCleaner.reset();
    mReaderContainer.reset();
    mReader.reset();
    mWriter.reset();
    mAsyncSegmentDumper.reset();
    mDumpSegmentContainer.reset();
    mPluginManager.reset();
    IndexPartition::Reset();
}

OnlinePartition::~OnlinePartition()
{
    try {
        Close();
    } catch (const ExceptionBase& e) {
        IE_PREFIX_LOG(ERROR, "error occurred in OnlinePartition deconstructor[%s]", e.what());
    }
    Reset();
}

void OnlinePartition::SetEnableReleaseOperationAfterDump(bool releaseOperations)
{
    if (mWriter) {
        mWriter->SetEnableReleaseOperationAfterDump(releaseOperations);
    }
}

IndexPartition::OpenStatus OnlinePartition::Open(const string& primaryDir, const string& secondaryDir,
                                                 const IndexPartitionSchemaPtr& schema,
                                                 const IndexPartitionOptions& options, versionid_t targetVersionId)
{
    int64_t beginTime = TimeUtility::currentTime();
    IE_PREFIX_LOG(INFO, "open partition begin, verison[%d]", targetVersionId);
    IndexPartition::OpenStatus os = OS_FAIL;
    try {
        const std::string& sourceRoot =
            (options.GetOnlineConfig().NeedReadRemoteIndex() && !secondaryDir.empty()) ? secondaryDir : primaryDir;
        os = DoOpen(primaryDir, sourceRoot, schema, options, targetVersionId, OPENING);
        if (os != OS_OK) {
            IE_PREFIX_LOG(ERROR, "open partition failed. Partition is to be closed");
            DoClose();
        }
    } catch (const FileIOException& e) {
        IE_PREFIX_LOG(ERROR, "open caught file io exception: %s", e.what());
        os = OS_FILEIO_EXCEPTION;
    } catch (const ExceptionBase& e) {
        // BadParameterException, InconsistentStateException, ...
        IE_PREFIX_LOG(ERROR, "open caught exception: %s", e.what());
        os = OS_INDEXLIB_EXCEPTION;
    } catch (const exception& e) {
        IE_PREFIX_LOG(ERROR, "open caught std exception: %s", e.what());
        os = OS_UNKNOWN_EXCEPTION;
    } catch (...) {
        IE_PREFIX_LOG(ERROR, "open caught unknown exception");
        os = OS_UNKNOWN_EXCEPTION;
    }
    IE_PREFIX_LOG(INFO, "open partition end, verison[%d], os[%d], used[%.3f]s", targetVersionId, os,
                  (TimeUtility::currentTime() - beginTime) / 1000000.0);
    return os;
}

Version OnlinePartition::LoadOnDiskVersion(const string& workRoot, const string& sourceRoot,
                                           versionid_t targetVersionId)
{
    Version onDiskVersion;
    const std::string& root = sourceRoot;
    VersionLoader::GetVersionS(root, onDiskVersion, targetVersionId);
    auto lifecycleTable = GetLifecycleTableFromVersion(onDiskVersion);
    if (likely(onDiskVersion.GetVersionId() != INVALID_VERSION)) {
        THROW_IF_FS_ERROR(
            mFileSystem->MountVersion(root, onDiskVersion.GetVersionId(), "", FSMT_READ_ONLY, lifecycleTable),
            "mount version failed");
    } else {
        // some ut in ha3 has no version, they only need schema.json...
        THROW_IF_FS_ERROR(mFileSystem->MountFile(root, SCHEMA_FILE_NAME, SCHEMA_FILE_NAME, FSMT_READ_ONLY, -1, false),
                          "mount schema file failed");
        THROW_IF_FS_ERROR(mFileSystem->MountFile(root, INDEX_FORMAT_VERSION_FILE_NAME, INDEX_FORMAT_VERSION_FILE_NAME,
                                                 FSMT_READ_ONLY, -1, false),
                          "mount version file failed");
    }
    return onDiskVersion;
}

IndexPartition::OpenStatus OnlinePartition::DoOpen(const string& workRoot, const string& sourceRoot,
                                                   const IndexPartitionSchemaPtr& schema,
                                                   const IndexPartitionOptions& options, versionid_t targetVersionId,
                                                   IndexPartition::IndexPhase indexPhase)
{
    IE_PREFIX_LOG(INFO, "open index partition: work root [%s], source root [%s], version[%d]", workRoot.c_str(),
                  sourceRoot.c_str(), targetVersionId);
    IndexPhaseGuard phaseGuard(mIndexPhase, indexPhase);
    IndexPartition::OpenStatus openStatus =
        IndexPartition::Open(workRoot, sourceRoot, schema, options, targetVersionId);
    assert(openStatus == OS_OK);
    (void)openStatus;

    Version onDiskVersion = LoadOnDiskVersion(workRoot, sourceRoot, targetVersionId);
    const string& remoteRealtimeSyncDir = options.GetOnlineConfig().GetRealtimeIndexSyncDir();
    if (!remoteRealtimeSyncDir.empty()) {
        mRealtimeIndexSynchronizer.reset(new RealtimeIndexSynchronizer(RealtimeIndexSynchronizer::READ_WRITE));
        if (!mRealtimeIndexSynchronizer->Init(remoteRealtimeSyncDir, mPartitionMemController)) {
            IE_LOG(ERROR, "realtime index sync init failed");
            return OS_FAIL;
        }
        DirectoryPtr localRtDirectory =
            GetFileSystemRootDirectory()->MakeDirectory(RT_INDEX_PARTITION_DIR_NAME, DirectoryOption::Mem());
        if (!mRealtimeIndexSynchronizer->SyncFromRemote(localRtDirectory, onDiskVersion)) {
            IE_LOG(ERROR, "realtime index sync from remote path [%s] to local path [%s]failed",
                   remoteRealtimeSyncDir.c_str(), localRtDirectory->DebugString().c_str());
            return OS_FAIL;
        }
    }
    mEnableAsyncDump =
        (options.GetOnlineConfig().enableAsyncDumpSegment || options.GetBuildConfig(true).EnableAsyncDump());
    mOperationLogCatchUpThreshold = options.GetOnlineConfig().GetOperationLogCatchUpThreshold();
    mReaderContainer.reset(new ReaderContainer);
    if (!mDumpSegmentContainer) {
        // mDumpSegmentContainer should be reset in Close()
        mDumpSegmentContainer.reset(new DumpSegmentContainer());
    }
    if (!mRealtimeQuotaController) {
        mRealtimeQuotaController.reset(
            new MemoryQuotaController(mOptions.GetOnlineConfig().maxRealtimeMemSize * 1024 * 1024));
    }
    mRtMemQuotaSynchronizer = CreateRealtimeMemoryQuotaSynchronizer(mRealtimeQuotaController);

    mClosed = true;

    ScopedLock lock(mDataLock);

    RewriteSchemaAndOptions(schema, onDiskVersion);

    FileSystemMetricsReporter* reporter = mFileSystem->GetFileSystemMetricsReporter();
    if (reporter) {
        // update reporter tags
        util::KeyValueMap tagMap;
        tagMap["schema_name"] = mSchema->GetSchemaName();
        string generationStr;
        if (onDiskVersion.GetDescription("generation", generationStr)) {
            tagMap["generation"] = generationStr;
        }
        reporter->UpdateGlobalTags(tagMap);
    }

    mOnlinePartMetrics->RegisterMetrics(mSchema->GetTableType());
    mCounterMap.reset(new CounterMap());
    if (mOptions.GetOnlineConfig().loadRemainFlushRealtimeIndex && !mOptions.TEST_mReadOnly) {
        // first open will recover rt index
        mOptions.GetOnlineConfig().enableRecoverIndex = true;
    }

    mPluginManager = IndexPluginLoader::Load(mIndexPluginPath, mSchema->GetIndexSchema(), mOptions);
    if (!mPluginManager) {
        IE_PREFIX_LOG(ERROR, "load index plugin failed for schema [%s]", mSchema->GetSchemaName().c_str());
        return OS_FAIL;
    }
    // mDumpSegmentContainer should only exists for test
    BuildingPartitionParam param(mOptions, mRtSchema, mPartitionMemController, mDumpSegmentContainer, mCounterMap,
                                 mPluginManager, mMetricProvider, mSrcSignature);
    mPartitionDataHolder.Reset(
        PartitionDataCreator::CreateBuildingPartitionData(param, mFileSystem, onDiskVersion, "", InMemorySegmentPtr()));
    ResetCounter();
    PartitionMeta partitionMeta = mPartitionDataHolder.Get()->GetPartitionMeta();
    PluginResourcePtr pluginResource(
        new IndexPluginResource(mSchema, mOptions, mCounterMap, partitionMeta, mIndexPluginPath, mMetricProvider));
    mPluginManager->SetPluginResource(pluginResource);

    // only recover rt index once
    mOptions.GetOnlineConfig().enableRecoverIndex = false;

    if (mOptions.GetOnlineConfig().IsValidateIndexEnabled()) {
        DeployIndexValidator::ValidateDeploySegments(mPartitionDataHolder.Get()->GetRootDirectory(), onDiskVersion);
    }

    CheckPartitionMeta(mSchema, partitionMeta);
    assert(!mSchema->GetVirtualAttributeSchema());
    assert(!mRtSchema->GetVirtualAttributeSchema());

    mFileSystem->SwitchLoadSpeedLimit(
        mOptions.GetOnlineConfig().enableLoadSpeedControlForOpen); // true for on, false for off

    if (mOptions.GetOnlineConfig().disableSsePforDeltaOptimize) {
        EncoderProvider::GetInstance()->DisableSseOptimize();
    }

    if (!InitAccessCounters(mCounterMap, mSchema)) {
        return OS_FAIL;
    }
    InitResourceCalculator();

    if (mOptions.GetOnlineConfig().loadRemainFlushRealtimeIndex && !mOptions.TEST_mReadOnly && HasFlushRtIndex()) {
        // ReclaimRemainFlushRtIndex(mPartitionDataHolder.Get());
        if (!RecoverFlushRtIndex()) {
            IE_LOG(ERROR, "recover flush rt index failed");
            return OS_FAIL;
        }
        IndexPartition::OpenStatus status = LoadIncWithRt(onDiskVersion);
        if (status != OS_OK) {
            return status;
        }
    } else {
        OpenExecutorChainCreatorPtr creator = CreateChainCreator();
        OpenExecutorChainPtr chain = creator->CreateReopenPartitionReaderChain(false);
        ExecutorResource resource = CreateExecutorResource(onDiskVersion, false);
        if (!chain->Execute(resource)) {
            Close();
            return OS_LACK_OF_MEMORY;
        }
    }

    MEMORY_BARRIER();
    mFileSystem->SwitchLoadSpeedLimit(true);
    mReader->EnableAccessCountors(mNeedReportTemperature);

    InitResourceCleaner();
    if (NeedCheckSecondIndex()) {
        CheckSecondIndex();
    }
    if (NeedSubscribeSecondIndex()) {
        SubscribeSecondIndex();
    }
    ReportMetrics();
    mLoadedIncVersion = mPartitionDataHolder.Get()->GetOnDiskVersion();

    mClosed = false;
    if (!mReaderUpdater) {
        mReaderUpdater.reset(new SearchReaderUpdater(mSchema->GetSchemaName()));
    }

    if (!mOptions.TEST_mReadOnly && !PrepareIntervalTask()) {
        return OS_FAIL;
    }
    mNeedReload = false;
    mIsServiceNormal = true;
    IE_PREFIX_LOG(INFO, "open index partition end, version[%d]", targetVersionId);
    return OS_OK;
}

bool OnlinePartition::RecoverFlushRtIndex()
{
    IE_LOG(INFO, "begin recover flush rt index");
    auto onDiskVersion = mPartitionDataHolder.Get()->GetOnDiskVersion();
    ExecutorResource resource = CreateExecutorResource(onDiskVersion, false);
    RealtimeIndexRecoverExecutor executor;
    bool ret = executor.Execute(resource);
    IE_LOG(INFO, "end recover flush rt index");
    for (auto iter = mPartitionDataHolder.Get()->Begin(); iter != mPartitionDataHolder.Get()->End(); iter++) {
        mRecoverMaxTs = max(mRecoverMaxTs, iter->GetSegmentInfo()->timestamp);
    }
    return ret;
}

IFileSystemPtr OnlinePartition::CreateFileSystem(const string& primaryDir, const string& secondaryDir)
{
    if (mOptions.TEST_mReadOnly) {
        return IndexPartition::CreateFileSystem(primaryDir, secondaryDir);
    }

    auto fileSystem = IndexPartition::CreateFileSystem(primaryDir, secondaryDir);
    ResetRtAndJoinDirPath(Directory::Get(fileSystem));
    return fileSystem;
}

void OnlinePartition::RewriteSchemaAndOptions(const IndexPartitionSchemaPtr& schema, const Version& onDiskVersion)
{
    ScopedLock schemaLock(mSchemaLock);
    mOptions.SetNeedRewriteFieldType(false);
    mOptions.SetOngoingModifyOperationIds(onDiskVersion.GetOngoingModifyOperations());
    mOptions.SetUpdateableSchemaStandards(onDiskVersion.GetUpdateableSchemaStandards());
    mSchema = SchemaAdapter::LoadAndRewritePartitionSchema(GetFileSystemRootDirectory(), mOptions,
                                                           onDiskVersion.GetSchemaVersionId());

    CheckParam(mSchema, mOptions);

    if (schema) {
        mSchema->AssertCompatible(*schema);
    }
    mRtSchema = CreateRtSchema(mOptions);
}

bool OnlinePartition::AddOneIntervalTask(const std::string& taskName, int32_t interval,
                                         OnlinePartitionTaskItem::TaskType taskType)
{
    TaskItemPtr taskItem(new OnlinePartitionTaskItem(this, taskType));
    if (!mTaskScheduler->DeclareTaskGroup(taskName, interval)) {
        IE_PREFIX_LOG(ERROR, "declare task[%s] failed!", taskName.c_str());
        return false;
    }
    int32_t taskId = mTaskScheduler->AddTask(taskName, taskItem);
    if (taskId == TaskScheduler::INVALID_TASK_ID) {
        IE_PREFIX_LOG(ERROR, "add task[%s:%d] failed!", taskName.c_str(), taskId);
        return false;
    }
    mTaskIds.push_back(make_pair(taskName, taskId));
    return true;
}

bool OnlinePartition::PrepareIntervalTask()
{
    for (const pair<string, int32_t>& oldTaskId : mTaskIds) {
        mTaskScheduler->DeleteTask(oldTaskId.second);
    }
    mTaskIds.clear();
    int32_t sleepTime = REPORT_METRICS_INTERVAL;
    if (unlikely(autil::EnvUtil::getEnv("TEST_QUICK_EXIT", false))) {
        sleepTime /= 1000;
    }

    if (mOptions.GetOnlineConfig().enableAsyncCleanResource) {
        if (!AddOneIntervalTask("clean_resource", sleepTime, OnlinePartitionTaskItem::TT_CLEAN_RESOURCE)) {
            IE_PREFIX_LOG(ERROR, "Prepare clean_resource task failed!");
            return false;
        }
    }
    // if (!AddOneIntervalTask(
    //         "calculate_metrics", sleepTime, OnlinePartitionTaskItem::TT_CALCULATE_METRICS)) {
    //     IE_PREFIX_LOG(ERROR, "Prepare calculate_metrics task failed!");
    //     return false;
    // }
    if (!AddOneIntervalTask("report_metrics", sleepTime, OnlinePartitionTaskItem::TT_REPORT_METRICS)) {
        IE_PREFIX_LOG(ERROR, "Prepare report_metrics task failed!");
        return false;
    }
    if (!AddOneIntervalTask("interval_dump", sleepTime, OnlinePartitionTaskItem::TT_INTERVAL_DUMP)) {
        IE_PREFIX_LOG(ERROR, "Prepare interval_dump task failed!");
        return false;
    }
    if (mEnableAsyncDump) {
        mAsyncSegmentDumper.reset(new AsyncSegmentDumper(this, mCleanerLock));
        if (!AddOneIntervalTask("async_dump", sleepTime, OnlinePartitionTaskItem::TT_TRIGGER_ASYNC_DUMP)) {
            IE_PREFIX_LOG(ERROR, "Prepare async_dump task failed!");
            return false;
        }
    }
    if (NeedCheckSecondIndex()) {
        if (!AddOneIntervalTask("check_index", mCheckSecondIndexIntervalInMin * 60 * 1000 * 1000,
                                OnlinePartitionTaskItem::TT_CHECK_SECONDARY_INDEX)) {
            IE_PREFIX_LOG(ERROR, "Prepare check_index task failed!");
            return false;
        }
    }
    if (NeedSubscribeSecondIndex()) {
        if (!AddOneIntervalTask("subscribe_index", mSubscribeSecondIndexIntervalInMin * 60 * 1000 * 1000,
                                OnlinePartitionTaskItem::TT_SUBSCRIBE_SECONDARY_INDEX)) {
            IE_PREFIX_LOG(ERROR, "Prepare subscribe_index task failed!");
            return false;
        }
    }
    if (!mOptions.GetOnlineConfig().GetRealtimeIndexSyncDir().empty()) {
        if (!AddOneIntervalTask("sync_realtime_index", 1 * 1000 * 1000,
                                OnlinePartitionTaskItem::TT_SYNC_REALTIME_INDEX)) {
            IE_PREFIX_LOG(ERROR, "Prepare sync_realtime_index task failed!");
            return false;
        }
        IE_LOG(INFO, "declare sync realtime index task");
    }
    return true;
}

void OnlinePartition::CheckParam(const IndexPartitionSchemaPtr& schema, const IndexPartitionOptions& options) const
{
    if (!schema) {
        INDEXLIB_FATAL_ERROR(BadParameter, "[%s] no schema in index", IE_LOG_PREFIX);
    }

    if (!options.IsOnline()) {
        INDEXLIB_FATAL_ERROR(BadParameter, "[%s] OnlinePartition only support online option!", IE_LOG_PREFIX);
    }

    options.Check();
    schema->Check();

    // check maxReopenMemoryUse with memoryUseLimit
    int64_t maxReopenMemoryUse;
    int64_t totalMemUse = mPartitionMemController->GetFreeQuota() / (1024 * 1024);
    if (options.GetOnlineConfig().GetMaxReopenMemoryUse(maxReopenMemoryUse) && maxReopenMemoryUse >= totalMemUse) {
        INDEXLIB_FATAL_ERROR(BadParameter,
                             "[%s] maxReopenMemoryUse [%ld]MB should less than current free memory [%ld]MB!",
                             IE_LOG_PREFIX, maxReopenMemoryUse, totalMemUse);
    }
}

void OnlinePartition::InitReader()
{
    auto partData = mPartitionDataHolder.Get();
    Version version = partData->GetVersion();
    IE_PREFIX_LOG(INFO, "begin open index partition reader, version[%d]", version.GetVersionId());

    MemoryReserverPtr memReserver = mFileSystem->CreateMemoryReserver("switch_flush_rt_segment");
    segmentid_t lastValidFlushRtSegId = GetLatestLinkRtSegId(partData, memReserver);
    DoInitReader(lastValidFlushRtSegId);
    IE_PREFIX_LOG(INFO, "end open index partition reader, version[%d]", version.GetVersionId());
}

void OnlinePartition::DoInitReader(segmentid_t flushRtSegmentId)
{
    OnlinePartitionReaderPtr reader(new partition::OnlinePartitionReader(mOptions, mSchema, mSearchCache,
                                                                         mOnlinePartMetrics.get(), flushRtSegmentId,
                                                                         mPartitionName, mLatestNeedSkipIncTs));

    PartitionDataPtr clonedPartitionData(mPartitionDataHolder.Get()->Clone());

    AttributeMetrics* attributeMetrics = mOnlinePartMetrics->GetAttributeMetrics();
    assert(attributeMetrics);
    attributeMetrics->ResetMetricsForNewReader();
    partition::OnlinePartitionReaderPtr hintReader = mReader;
    reader->Open(clonedPartitionData, hintReader.get());

    LoadReaderPatch(clonedPartitionData, reader);

    SwitchReader(reader);

    if (mWriter) {
        PartitionModifierPtr modifier = CreateReaderModifier(reader);
        mWriter->ReOpenNewSegment(modifier);
    }
}

/* @param: isAboutToSwitchMaster 是否是切换到主路前的最后一次redo，可能需要执行一些特殊的逻辑 */
void OnlinePartition::GetRedoParam(PartitionDataPtr& partData, IndexPartitionSchemaPtr& schema,
                                   SimpleMemoryQuotaControllerPtr& memController, PartitionModifierPtr& modifier,
                                   OnlinePartitionReaderPtr& hintReader, bool isAboutToSwitchMaster)
{
    PartitionMemoryQuotaControllerPtr partMemController;
    OnlinePartitionReaderPtr reader;
    {
        int64_t beginTime = TimeUtility::currentTime();
        ScopedLock dataLockGuard(mDataLock);
        if (isAboutToSwitchMaster) {
            // 保证 partitionInfo 准确
            partData.reset(mPartitionDataHolder.Get()->Clone());
        } else {
            partData.reset(mPartitionDataHolder.Get()->Snapshot(&mDataLock));
        }
        partData->UpdateDumppedSegment();
        partMemController = mPartitionMemController;
        MemoryReserverPtr memReserver = mFileSystem->CreateMemoryReserver("dump_redo_opertion");
        segmentid_t lastValidFlushRtSegId = GetLatestLinkRtSegId(mPartitionDataHolder.Get(), memReserver);
        schema = GetSchema();
        reader.reset(new partition::OnlinePartitionReader(mOptions, schema, mSearchCache, mOnlinePartMetrics.get(),
                                                          lastValidFlushRtSegId, mPartitionName, mLatestNeedSkipIncTs));
        if (isAboutToSwitchMaster) {
            AttributeMetrics* attributeMetrics = mOnlinePartMetrics->GetAttributeMetrics();
            assert(attributeMetrics);
            attributeMetrics->ResetMetricsForNewReader();
        }
        int64_t endTime = TimeUtility::currentTime();
        mOnlinePartMetrics->IncreaseRedoOperationLockRealtimeTimeusedValue((endTime - beginTime) / 1000);
    }
    reader->Open(partData, hintReader.get());
    modifier = CreateReaderModifier(reader);
    BlockMemoryQuotaControllerPtr blockMemController(
        new BlockMemoryQuotaController(partMemController, "dump_replay_op_log"));
    memController.reset(new SimpleMemoryQuotaController(blockMemController));
    hintReader = reader;
}

uint64_t OnlinePartition::BatchRedoOpertions(const OperationRedoStrategyPtr& dumpRedoStrategy,
                                             OperationCursor& lastCursor, OnlinePartitionReaderPtr& reader,
                                             bool isAboutToSwtchMaster)
{
    PartitionDataPtr partData;
    PartitionModifierPtr modifier;
    IndexPartitionSchemaPtr schema;
    util::SimpleMemoryQuotaControllerPtr memController;
    PartitionDataPtr currentPartData;

    GetRedoParam(currentPartData, schema, memController, modifier, reader, isAboutToSwtchMaster);
    auto onDiskVersion = currentPartData->GetOnDiskVersion();
    OperationReplayer opReplayer(currentPartData, schema, memController);
    opReplayer.Seek(lastCursor);
    opReplayer.RedoOperations(modifier, onDiskVersion, dumpRedoStrategy);
    lastCursor = opReplayer.GetCursor();
    return OperationIterator::GetTotalBuildingOperationCntFromCursor(currentPartData, lastCursor);
}

// redo segment's operation log include building segment and waiting_to_dump segment
void OnlinePartition::RedoOperations()
{
    IE_PREFIX_LOG(INFO, "Begin redo operation log.");
    util::ScopeLatencyReporter scopeTime(mOnlinePartMetrics->GetRedoOperationLogTimeusedMetric().get());
    mOnlinePartMetrics->SetRedoOperationLockRealtimeTimeusedValue(0);
    PartitionDataPtr currentPartData;
    OnlinePartitionReaderPtr reader;
    {
        ScopedLock dataLockGuard(mDataLock);
        currentPartData.reset(mPartitionDataHolder.Get()->Snapshot(&mDataLock));
        currentPartData->UpdateDumppedSegment();
        ScopedLock readerLockGuard(mReaderLock);
        reader = mReader;
    }

    // no need to redo current dumpped segment, so +1
    segmentid_t redoOperationBeginSegId = currentPartData->CreateSegmentIterator()->GetLastBuiltSegmentId();
    OperationCursor lastCursor(redoOperationBeginSegId + 1);
    OperationRedoStrategyPtr dumpRedoStrategy(new DumpOperationRedoStrategy(redoOperationBeginSegId));

    static constexpr uint32_t maxCatchUpCnt = 10;
    uint32_t catchUpCnt = 0;
    uint64_t leftOperationLogCnt = 0;
    do {
        leftOperationLogCnt = BatchRedoOpertions(dumpRedoStrategy, lastCursor, reader, false);
        if (++catchUpCnt > maxCatchUpCnt) {
            IE_PREFIX_LOG(INFO, "End batch replay, cause of retry count.");
            break;
        }
    } while (leftOperationLogCnt > mOperationLogCatchUpThreshold);

    IE_PREFIX_LOG(INFO, "End while redo count %lu. segId[%d]", leftOperationLogCnt, redoOperationBeginSegId);

    int64_t beginTime = TimeUtility::currentTime();
    ScopedLock dataLockGuard(mDataLock);
    mPartitionDataHolder.Get()->CommitVersion();
    BatchRedoOpertions(dumpRedoStrategy, lastCursor, reader, true);
    LoadReaderPatch(mPartitionDataHolder.Get(), reader);
    SwitchReader(reader);
    PartitionModifierPtr modifier = CreateReaderModifier(reader);
    assert(mWriter != nullptr);
    mWriter->ReOpenNewSegment(modifier);
    mReader->EnableAccessCountors(mNeedReportTemperature);
    if (mIndexPhase != SWITCH_FLUSH_RT && mReaderUpdater != nullptr) {
        mReaderUpdater->Update(mReader);
    }
    int64_t lockRtTimeUsed = (TimeUtility::currentTime() - beginTime) / 1000;
    mOnlinePartMetrics->IncreaseRedoOperationLockRealtimeTimeusedValue(lockRtTimeUsed);
    ReportMetrics();

    IE_PREFIX_LOG(INFO, "End redo operation log, segId[%d]. lock RT timeUsed[%ld]", redoOperationBeginSegId,
                  lockRtTimeUsed);
}

void OnlinePartition::InitWriter(const PartitionDataPtr& partData)
{
    assert(partData->GetInMemorySegment());
    assert(!mWriter);
    mWriter.reset(new OnlinePartitionWriter(mOptions, mDumpSegmentContainer, mOnlinePartMetrics.get(), mPartitionName,
                                            mSrcSignature));
    mResourceCalculator->SetWriter(mWriter);
    PartitionModifierPtr modifier = CreateReaderModifier(mReader);
    mWriter->Open(mRtSchema, partData, modifier);
}

void OnlinePartition::InitResourceCleaner()
{
    mResourceCleaner.reset(new ExecutorScheduler);
    ExecutorPtr readerCleaner(new PartitionReaderCleaner(mReaderContainer, mFileSystem, mDataLock, mPartitionName));
    mResourceCleaner->Add(readerCleaner, ExecutorScheduler::ST_REPEATEDLY);

    // inMemorySegCleaner should be after readerCleaner : reader will release inMemorySegment
    ExecutorPtr inMemorySegCleaner(new InMemorySegmentCleaner(mDumpSegmentContainer->GetInMemSegmentContainer()));
    mResourceCleaner->Add(inMemorySegCleaner, ExecutorScheduler::ST_REPEATEDLY);

    // inMemoryIndexCleaner should after readerCleaner & inMemorySegCleaner
    // kv table will hold key file in inMemorySegment
    ExecutorPtr inMemoryIndexCleaner(
        new InMemoryIndexCleaner(mReaderContainer, mPartitionDataHolder.Get()->GetRootDirectory()));
    mResourceCleaner->Add(inMemoryIndexCleaner, ExecutorScheduler::ST_REPEATEDLY);
    AddOnDiskIndexCleaner();
    CleanResource();
}

void OnlinePartition::InitResourceCalculator()
{
    mResourceCalculator = CreateResourceCalculator(mOptions, mPartitionDataHolder.Get(), mWriter, mPluginManager);
}

PartitionResourceCalculatorPtr
OnlinePartition::CreateResourceCalculator(const IndexPartitionOptions& options, const PartitionDataPtr& partitionData,
                                          const PartitionWriterPtr& writer,
                                          const plugin::PluginManagerPtr& pluginManager) const
{
    PartitionResourceCalculatorPtr calculator(new PartitionResourceCalculator(options.GetOnlineConfig()));
    calculator->Init(partitionData->GetRootDirectory(), writer, mDumpSegmentContainer->GetInMemSegmentContainer(),
                     pluginManager);
    return calculator;
}

IndexPartition::OpenStatus OnlinePartition::SwitchFlushRtSegments()
{
    IE_PREFIX_LOG(INFO, "lock dumpLock in reopen thread.");
    ScopedLock dumpLockGuard(mDumpLock);

    IndexPhaseGuard phaseGuard(mIndexPhase, SWITCH_FLUSH_RT);
    IE_PREFIX_LOG(INFO, "lock dataLock in reopen thread.");
    ScopedLock lock(mDataLock);

    util::ScopeLatencyReporter scopeTime(mOnlinePartMetrics->GetSwitchFlushRtSegmentLatencyMetric().get());

    MemoryReserverPtr memReserver = mFileSystem->CreateMemoryReserver("switch_flush_rt_segment");
    segmentid_t lastValidFlushRtSegId = GetLatestLinkRtSegId(mPartitionDataHolder.Get(), memReserver);
    assert(mReader);
    segmentid_t lastReaderFlushRtSegId = mReader->GetPartitionVersion()->GetLastLinkRtSegmentId();

    if (lastValidFlushRtSegId <= lastReaderFlushRtSegId) {
        IE_PREFIX_LOG(WARN, "skip switch reader for flushed rt segments!");
        return OS_OK;
    }

    IE_PREFIX_LOG(INFO, "switch reader for flushed rt segments begin");
    DoInitReader(lastValidFlushRtSegId);
    mReader->EnableAccessCountors(mNeedReportTemperature);
    ReportMetrics();

    IE_PREFIX_LOG(INFO, "switch reader for flushed rt segments end");
    return OS_OK;
}

IndexPartition::OpenStatus OnlinePartition::ReclaimReaderMem()
{
    IndexPhaseGuard phaseGuard(mIndexPhase, RECLAIM_READER_MEM);
    IE_PREFIX_LOG(INFO, "lock dataLock in reopen thread.");
    ScopedLock lock(mDataLock);

    util::ScopeLatencyReporter scopeTime(mOnlinePartMetrics->GetReclaimReaderMemLatencyMetric().get());

    IE_PREFIX_LOG(INFO, "reclaim reader memory begin");

    // TODO: skip dump, just open reader
    if (mWriter) {
        mWriter->DumpSegment();
        mPartitionDataHolder.Get()->CreateNewSegment();
    }

    InitReader();
    mReader->EnableAccessCountors(mNeedReportTemperature);
    ReportMetrics();
    IE_PREFIX_LOG(INFO, "reclaim reader memory end");
    return OS_OK;
}

IndexPartition::OpenStatus OnlinePartition::ReOpen(bool forceReopen, versionid_t targetVersionId)
{
    int64_t beginTime = TimeUtility::currentTime();
    IE_PREFIX_LOG(INFO, "reopen partition begin, force[%d], verison[%d]", forceReopen, targetVersionId);
    OpenStatus os;
    try {
        if (mClosed) {
            IE_PREFIX_LOG(WARN, "partition is closed. try open instead");
            os = DoOpen(mOpenIndexPrimaryDir, mOpenIndexSecondaryDir, mOpenSchema, mOpenOptions, targetVersionId,
                        FORCE_OPEN);
            IE_PREFIX_LOG(INFO, "reopen partition end, verison[%d], os[%d], used[%.3f]s", targetVersionId, os,
                          (TimeUtility::currentTime() - beginTime) / 1000000.0);
            return os;
        }
        // scopelock will forbidden cleanResource when process reopen
        ScopedLock lock(mCleanerLock);
        CleanResource();

        os = DoReopen(forceReopen, targetVersionId);
        ResetCounter();
        IE_PREFIX_LOG(INFO, "reopen partition end, verison[%d], os[%d], used[%.3f]s", targetVersionId, os,
                      (TimeUtility::currentTime() - beginTime) / 1000000.0);
        return os;
    } catch (const FileIOException& e) {
        IE_PREFIX_LOG(ERROR, "reopen caught file io exception: %s", e.what());
        os = OS_FILEIO_EXCEPTION;
    } catch (const ExceptionBase& e) {
        IE_PREFIX_LOG(ERROR, "reopen caught exception: %s", e.what());
        os = OS_INDEXLIB_EXCEPTION;
    } catch (const exception& e) {
        IE_PREFIX_LOG(ERROR, "reopen caught std exception: %s", e.what());
        os = OS_UNKNOWN_EXCEPTION;
    } catch (...) {
        IE_PREFIX_LOG(ERROR, "reopen caught unknown exception");
        os = OS_UNKNOWN_EXCEPTION;
    }
    IE_PREFIX_LOG(INFO, "reopen partition end, verison[%d], os[%d], used[%.3f]s", targetVersionId, os,
                  (TimeUtility::currentTime() - beginTime) / 1000000.0);
    return os;
}

IndexPartition::OpenStatus OnlinePartition::LoadIncWithRt(Version& onDiskVersion)
{
    IndexPhaseGuard phaseGuard(mIndexPhase, FORCE_REOPEN);
    IE_PREFIX_LOG(INFO, "force reopen begin, version[%d]", onDiskVersion.GetVersionId());

    mFileSystem->SwitchLoadSpeedLimit(false);
    OpenExecutorChainCreatorPtr creator = CreateChainCreator();
    OpenExecutorChainPtr chain = creator->CreateForceReopenExecutorChain(
        mSchema, mFileSystem, mPartitionMemController, mPartitionDataHolder, mLoadedIncVersion, onDiskVersion,
        mDataLock, mOptions, mOnlinePartMetrics, mEnableAsyncDump);
    ExecutorResource&& resource = CreateExecutorResource(onDiskVersion, true);
    if (!chain->Execute(resource)) {
        IE_PREFIX_LOG(INFO, "force reopen fail, version[%d]", onDiskVersion.GetVersionId());
        mIsServiceNormal = false;
        return OS_FAIL;
    }
    mLatestNeedSkipIncTs = resource.mLatestNeedSkipIncTs;

    MEMORY_BARRIER();
    ReportMetrics();
    AddOnDiskIndexCleaner();
    IE_PREFIX_LOG(INFO, "force reopen success, version[%d]", onDiskVersion.GetVersionId());
    mFileSystem->SwitchLoadSpeedLimit(true);
    // mFileSystem->CleanCache();        clean cache to make memory metric right
    mIsServiceNormal = true;
    return OS_OK;
}

void OnlinePartition::SwitchReader(const OnlinePartitionReaderPtr& reader)
{
    {
        autil::ScopedLock lock(mReaderLock);
        mReader = reader;
        MEMORY_BARRIER();
    }
    mReaderContainer->AddReader(reader);
    mResourceCalculator->CalculateCurrentIndexSize(mPartitionDataHolder.Get(), mSchema);
}

void OnlinePartition::AddOnDiskIndexCleaner()
{
    // TODO: In future, suez will in controll of local index clean
    if (!mResourceCleaner) {
        return;
    }
    string envStr = autil::EnvUtil::getEnv("INDEXLIB_ONLINE_CLEAN_ON_DISK_INDEX");
    if (envStr == "false") {
        return;
    }
    if (!autil::EnvUtil::hasEnv("INDEXLIB_ONLINE_NOT_AUTO_DISABLE_CLEAN_INDEX")) {
        if (!mOptions.GetOnlineConfig().NeedDeployIndex()) {
            IE_PREFIX_LOG(INFO, "do not need clean on disk index when NeedDeployIndex=false");
            return;
        }
    }
    ExecutorPtr onDiskIndexCleaner(new OnDiskIndexCleaner(mOptions.GetOnlineConfig().onlineKeepVersionCount,
                                                          mReaderContainer, GetRootDirectory()));
    mResourceCleaner->Add(onDiskIndexCleaner, ExecutorScheduler::ST_ONCE);
}

void OnlinePartition::AddVirtualAttributeDataCleaner(const IndexPartitionSchemaPtr& newSchema)
{
    vector<pair<string, bool>> attrNames;
    CollectVirtualAttributes(newSchema->GetVirtualAttributeSchema(), attrNames, false);

    const IndexPartitionSchemaPtr& newSubSchema = newSchema->GetSubIndexPartitionSchema();
    if (newSubSchema) {
        CollectVirtualAttributes(newSubSchema->GetVirtualAttributeSchema(), attrNames, true);
    }
    if (!mVirtualAttrContainer) {
        mVirtualAttrContainer.reset(new VirtualAttributeContainer);
        ExecutorPtr cleaner(
            new InMemVirtualAttributeCleaner(mReaderContainer, mVirtualAttrContainer, GetRootDirectory()));
        mResourceCleaner->Add(cleaner, ExecutorScheduler::ST_REPEATEDLY);
    }
    mVirtualAttrContainer->UpdateUsingAttributes(attrNames);
}

void OnlinePartition::CollectVirtualAttributes(const AttributeSchemaPtr& newSchema,
                                               vector<pair<string, bool>>& attrNames, bool isSub)
{
    if (!newSchema) {
        return;
    }
    AttributeSchema::Iterator iter = newSchema->Begin();
    for (; iter != newSchema->End(); iter++) {
        const string& attrName = (*iter)->GetAttrName();
        attrNames.emplace_back(attrName, isSub);
    }
}

ReopenDecider::ReopenType OnlinePartition::MakeReopenDecision(bool forceReopen, versionid_t targetVersionId,
                                                              Version& onDiskVersion)
{
    util::ScopeLatencyReporter scopeTime(mOnlinePartMetrics->GetdecisionLatencyMetric().get());
    ReopenDecider reopenDecider(mOptions.GetOnlineConfig(), forceReopen);
    const std::string& root = mOpenIndexSecondaryDir.empty() ? mOpenIndexPrimaryDir : mOpenIndexSecondaryDir;
    assert(mPartitionDataHolder.Get());
    reopenDecider.Init(mPartitionDataHolder.Get(), root, mOnlinePartMetrics->GetAttributeMetrics(), mRecoverMaxTs,
                       targetVersionId, mReader);
    Version version = reopenDecider.GetReopenIncVersion();
    ReopenDecider::ReopenType reopenType = reopenDecider.GetReopenType();
    if (reopenType == ReopenDecider::NORMAL_REOPEN || reopenType == ReopenDecider::FORCE_REOPEN) {
        VersionLoader::GetVersionS(root, onDiskVersion, version.GetVersionId());
        if (unlikely(onDiskVersion.GetVersionId() == INVALID_VERSION)) {
            INDEXLIB_FATAL_ERROR(InconsistentState, "get invalid version");
        }
        /*/ TODO: modify onDiskVersion's segmentTemperature
        1. read version.x.done file, and get deploy time
        2. create LifecyleStrategy
        3. modify onDiskVersion's segmentTemperatures
        */
        // TODO: new file should override old file with same name
        auto lifecycleTable = GetLifecycleTableFromVersion(onDiskVersion);
        THROW_IF_FS_ERROR(
            mFileSystem->MountVersion(root, onDiskVersion.GetVersionId(), "", FSMT_READ_ONLY, lifecycleTable),
            "mount version failed");
        reopenDecider.ValidateDeploySegments(mPartitionDataHolder.Get(), version);
    }
    onDiskVersion = version;
    return reopenType;
}

IndexPartition::OpenStatus OnlinePartition::DoReopen(bool forceReopen, versionid_t targetVersionId)
{
    IE_PREFIX_LOG(DEBUG, "reopen begin");
    // lambda for scope
    SetEnableReleaseOperationAfterDump(false);
    shared_ptr<OnlinePartition> selfPartition(this,
                                              [](OnlinePartition* p) { p->SetEnableReleaseOperationAfterDump(true); });

    util::ScopeLatencyReporter scopeTime(mOnlinePartMetrics->GetreopenIncLatencyMetric().get());

    ReportMetrics();

    Version onDiskVersion;
    ReopenDecider::ReopenType reopenType = MakeReopenDecision(forceReopen, targetVersionId, onDiskVersion);
    OpenStatus ret = OS_FAIL;
    switch (reopenType) {
    case ReopenDecider::NORMAL_REOPEN:
        ret = NormalReopen(onDiskVersion);
        break;
    case ReopenDecider::FORCE_REOPEN: {
        ret = LoadIncWithRt(onDiskVersion);
        if ((mOptions.GetOnlineConfig().enableForceOpen == false) || (ret == OS_OK)) {
            break;
        }
        IE_PREFIX_LOG(ERROR,
                      "force reopen failed, version[%d]; "
                      "abandon realtime data and switch to force *OPEN*",
                      targetVersionId);
        auto primaryDir = mOpenIndexPrimaryDir;
        auto secondaryDir = mOpenIndexSecondaryDir;
        auto schema = mOpenSchema;
        auto options = mOpenOptions;
        Close();
        Reset();
        ret = Open(primaryDir, secondaryDir, schema, options, targetVersionId);
        break;
    }
    case ReopenDecider::RECLAIM_READER_REOPEN:
        ret = ReclaimReaderMem();
        break;
    case ReopenDecider::SWITCH_RT_SEGMENT_REOPEN:
        ret = SwitchFlushRtSegments();
        break;
    case ReopenDecider::NO_NEED_REOPEN:
        ret = IndexPartition::OS_OK;
        break;
    case ReopenDecider::NEED_FORCE_REOPEN:
        ret = IndexPartition::OS_FORCE_REOPEN;
        break;
    case ReopenDecider::UNABLE_NORMAL_REOPEN:
        ret = IndexPartition::OS_LACK_OF_MEMORY;
        break;
    case ReopenDecider::UNABLE_REOPEN:
        ret = IndexPartition::OS_FAIL;
        break;
    case ReopenDecider::UNABLE_FORCE_REOPEN:
        ret = IndexPartition::OS_LACK_OF_MEMORY;
        break;
    case ReopenDecider::INCONSISTENT_SCHEMA_REOPEN:
        ret = IndexPartition::OS_INCONSISTENT_SCHEMA;
        break;
    default:
        IE_PREFIX_LOG(WARN, "unsupported reopenType : [%d]", reopenType);
        assert(false);
    }

    ScopedLock lockGuard(mDataLock);
    if (ret == OS_OK && mReaderUpdater) {
        if (reopenType == ReopenDecider::NO_NEED_REOPEN && !mReaderUpdater->NeedUpdateReader()) {
            return ret;
        }
        if (!mReaderUpdater->Update(mReader)) {
            ret = OS_FAIL;
        }
    }
    return ret;
}

void OnlinePartition::ReportMetrics()
{
    UpdateSwitchRtSegmentSize();
    UpdateOnlinePartitionMetrics();
    mOnlinePartMetrics->ReportMetrics();
    mOnlinePartMetrics->PrintMetrics(mOptions.GetOnlineConfig(), mPartitionName);
    mFileSystem->ReportMetrics();
    if (mWriter) {
        mWriter->ReportMetrics();
    }
}

void OnlinePartition::CleanResource()
{
    IE_PREFIX_LOG(DEBUG, "resource clean begin");
    ScopedLock lock(mCleanerLock);
    mResourceCleaner->Execute();
    UpdateSwitchRtSegmentSize();
    IE_PREFIX_LOG(DEBUG, "resource clean end");
}

void OnlinePartition::UpdateOnlinePartitionMetrics()
{
    if (!mResourceCalculator) {
        return;
    }

    size_t builtRtIndexMemoryUse = mResourceCalculator->GetRtIndexMemoryUse();
    size_t writerMemoryUse = mResourceCalculator->GetWriterMemoryUse();
    size_t dumpingMemoryUse = mDumpSegmentContainer->GetDumpingSegmentsSize();
    size_t rtIndexMemoryUse = builtRtIndexMemoryUse + writerMemoryUse + dumpingMemoryUse;

    if (mRtMemQuotaSynchronizer) {
        mRtMemQuotaSynchronizer->SyncMemoryQuota(rtIndexMemoryUse);
    }

    IndexPartition::MemoryStatus memoryStatus = CheckMemoryStatus();
    mOnlinePartMetrics->SetdumpingSegmentCountValue((int64_t)mDumpSegmentContainer->GetDumpItemSize());
    mOnlinePartMetrics->SetmemoryStatusValue((int64_t)memoryStatus);
    mOnlinePartMetrics->SetindexPhaseValue((int64_t)mIndexPhase);
    mOnlinePartMetrics->SetpartitionIndexSizeValue(mResourceCalculator->GetCurrentIndexSize());
    mOnlinePartMetrics->SetpartitionMemoryUseValue(mResourceCalculator->GetCurrentMemoryUse());
    mOnlinePartMetrics->SetincIndexMemoryUseValue(mResourceCalculator->GetIncIndexMemoryUse());
    mOnlinePartMetrics->SetbuiltRtIndexMemoryUseValue(builtRtIndexMemoryUse);
    mOnlinePartMetrics->SetbuildingSegmentMemoryUseValue(writerMemoryUse);
    mOnlinePartMetrics->SetrtIndexMemoryUseValue(rtIndexMemoryUse);
    const OnlineConfig& onlineConfig = mOptions.GetOnlineConfig();
    int64_t maxRtIndexSizeInMB = onlineConfig.maxRealtimeMemSize;
    double rtIndexMemoryRatio = (double)rtIndexMemoryUse / (maxRtIndexSizeInMB * 1024 * 1024);
    mOnlinePartMetrics->SetrtIndexMemoryUseRatioValue(rtIndexMemoryRatio);
    mOnlinePartMetrics->SetoldInMemorySegmentMemoryUseValue(
        mDumpSegmentContainer->GetInMemSegmentContainer()->GetTotalMemoryUse());

    int64_t partMemQuotaUse = mPartitionMemController->GetUsedQuota();
    mOnlinePartMetrics->SetpartitionMemoryQuotaUseValue(partMemQuotaUse);
    int64_t totalMemQuotaLimit = mPartitionMemController->GetTotalQuotaLimit();
    mOnlinePartMetrics->SettotalMemoryQuotaLimitValue(totalMemQuotaLimit);
    int64_t freeMemQuota = mPartitionMemController->GetFreeQuota();
    mOnlinePartMetrics->SetfreeMemoryQuotaValue(freeMemQuota);

    double partitionMemQuotaUseRatio = 1.0;
    if (partMemQuotaUse < totalMemQuotaLimit) {
        partitionMemQuotaUseRatio = (double)partMemQuotaUse / totalMemQuotaLimit;
    }
    mOnlinePartMetrics->SetpartitionMemoryQuotaUseRatioValue(partitionMemQuotaUseRatio);

    double freeMemQuotaRatio = 1.0;
    if (freeMemQuota < totalMemQuotaLimit) {
        freeMemQuotaRatio = (double)freeMemQuota / totalMemQuotaLimit;
    }
    mOnlinePartMetrics->SetfreeMemoryQuotaRatioValue(freeMemQuotaRatio);

    mOnlinePartMetrics->SetmissingSegmentCountValue(mMissingSegmentCount.load());

    // update ReaderContainer related metrics
    versionid_t oldestReaderVersion = mReaderContainer->GetOldestReaderVersion();
    versionid_t latestReaderVersion = mReaderContainer->GetLatestReaderVersion();
    int64_t readerCount = mReaderContainer->Size();

    mOnlinePartMetrics->SetpartitionReaderVersionCountValue(readerCount);
    mOnlinePartMetrics->SetoldestReaderVersionIdValue(oldestReaderVersion);
    mOnlinePartMetrics->SetlatestReaderVersionIdValue(latestReaderVersion);

    int64_t latestIncVersionTs = mReaderContainer->GetLatestReaderIncVersionTimestamp();
    int64_t currentTs = TimeUtility::currentTime();
    if (latestIncVersionTs != INVALID_TIMESTAMP && currentTs > latestIncVersionTs) {
        mOnlinePartMetrics->SetincVersionFreshnessValue((currentTs - latestIncVersionTs) / 1000);
    }
}

void OnlinePartition::LoadReaderPatch(const PartitionDataPtr& partitionData, OnlinePartitionReaderPtr& reader)
{
    if (mSchema->GetTableType() == tt_kv || mSchema->GetTableType() == tt_kkv) {
        return;
    }
    util::ScopeLatencyReporter scopeTime(mOnlinePartMetrics->GetLoadReaderPatchLatencyMetric().get());
    IE_PREFIX_LOG(INFO, "load reader patch begin");

    PartitionModifierPtr modifier = CreateReaderModifier(reader);
    if (modifier) {
        bool loadPatchForOpen = false;
        if (mIndexPhase == IndexPartition::OPENING || mIndexPhase == IndexPartition::FORCE_OPEN ||
            mIndexPhase == IndexPartition::FORCE_REOPEN) {
            loadPatchForOpen = true;
        }

        PatchLoader patchLoader(mRtSchema, mOptions.GetOnlineConfig());
        segmentid_t startLoadSegment = mLoadedIncVersion.GetLastSegment() + 1;
        bool isIncConsistentWithRt = (mLoadedIncVersion != index_base::Version(INVALID_VERSION)) &&
                                     mOptions.GetOnlineConfig().isIncConsistentWithRealtime;

        Version onDiskIncVersion = partitionData->GetOnDiskVersion();
        string locatorStr = partitionData->GetLastLocator();
        IndexLocator indexLocator;
        if (!indexLocator.fromString(locatorStr)) {
            isIncConsistentWithRt = false;
        } else {
            bool fromInc = false;
            OnlineJoinPolicy joinPolicy(onDiskIncVersion, mSchema->GetTableType(), partitionData->GetSrcSignature());
            joinPolicy.GetRtSeekTimestamp(indexLocator, fromInc);
            if (fromInc) {
                isIncConsistentWithRt = false;
            }
        }
        IE_PREFIX_LOG(INFO, "load reader patch with isIncConsistentWithRt[%d", isIncConsistentWithRt);
        patchLoader.Init(partitionData, isIncConsistentWithRt, mLoadedIncVersion, startLoadSegment, loadPatchForOpen);
        patchLoader.Load(mLoadedIncVersion, modifier);
    }
    IE_PREFIX_LOG(INFO, "load reader patch end");
}

void OnlinePartition::ReOpenNewSegment()
{
    IndexPhaseGuard phaseGuard(mIndexPhase, REOPEN_RT);
    ScopedLock lock(mDataLock);

    PartitionDataPtr partData = mPartitionDataHolder.Get();
    std::vector<InMemorySegmentPtr> dumpingSegments;
    std::set<segmentid_t> dumpingSegmentIds;
    if (mDumpSegmentContainer != nullptr) {
        mDumpSegmentContainer->GetDumpingSegments(dumpingSegments);
        for (const auto& seg : dumpingSegments) {
            dumpingSegmentIds.insert(seg->GetSegmentId());
        }
    }
    IE_PREFIX_LOG(INFO, "reopen new segment begin");
    util::ScopeLatencyReporter scopeTime(mOnlinePartMetrics->GetreopenRealtimeLatencyMetric().get());
    partData->CreateNewSegment();
    segmentid_t lastValidLinkRtSegId = partData->GetLastValidRtSegmentInLinkDirectory();
    InMemorySegmentPtr inMemSeg = partData->GetInMemorySegment();
    segmentid_t inMemSegmentId = inMemSeg ? inMemSeg->GetSegmentId() : INVALID_SEGMENTID;
    PartitionVersion partitionVersion(partData->GetVersion(), inMemSegmentId, lastValidLinkRtSegId, dumpingSegmentIds);
    if (partitionVersion.GetHashId() == mReader->GetPartitionVersionHashId() && mWriter) {
        return;
    }
    InitReader();

    if (!mWriter) {
        InitWriter(partData);
    } else {
        PartitionModifierPtr modifier = CreateReaderModifier(mReader);
        mWriter->ReOpenNewSegment(modifier);
    }

    mReader->EnableAccessCountors(mNeedReportTemperature);
    ReportMetrics();
    if (mReaderUpdater) {
        mReaderUpdater->Update(mReader);
    }

    IE_PREFIX_LOG(INFO, "reopen new segment end");
}

void OnlinePartition::Close()
{
    if (mClosed) {
        IE_PREFIX_LOG(INFO, "close index partition repeatedly, version[%d]", mLoadedIncVersion.GetVersionId());
        return;
    }

    try {
        DoClose();
    } catch (const ExceptionBase& e) {
        throw;
    }
}

void OnlinePartition::DoClose()
{
    IE_PREFIX_LOG(INFO, "close index partition begin, version[%d]", mLoadedIncVersion.GetVersionId());
    mClosed = true;
    mIsServiceNormal = false;
    // should not lock mDataLock, avoid dead lock
    for (const pair<string, int32_t>& oldTaskId : mTaskIds) {
        mTaskScheduler->DeleteTask(oldTaskId.second);
    }
    mTaskIds.clear();
    mAsyncSegmentDumper.reset();
    ScopedLock lock(mDataLock);

    if (mReaderContainer) {
        while (mReaderContainer->Size() > 2) {
            auto unReleasedReader = mReaderContainer->PopOldestReader();
            if (unReleasedReader && unReleasedReader.use_count() > 2) {
                IE_PREFIX_LOG(ERROR,
                              "unreleased reader [version: %d] [use_count:%lu] exists"
                              " when IndexPartition is to be closed",
                              unReleasedReader->GetVersion().GetVersionId(), unReleasedReader.use_count());
            }
        }

        mReaderContainer->Close();
    }
    {
        autil::ScopedLock lock(mReaderLock);
        if (mReader.use_count() > 2) {
            INDEXLIB_FATAL_ERROR(InconsistentState,
                                 "[%s] IndexPartitionReader is still "
                                 "used when the IndexPartition is to be closed",
                                 IE_LOG_PREFIX);
        }
        mReader.reset();
    }
    if (mFileSystem) {
        mFileSystem->CleanCache();
    }
    bool canDiscardData = !mOptions.GetOnlineConfig().loadRemainFlushRealtimeIndex;
    if (mWriter) {
        if (canDiscardData) {
            mWriter->CloseDiscardData();
        } else {
            mWriter->Close();
        }
    }
    mWriter.reset();
    if (mDumpSegmentContainer && mDumpSegmentContainer->GetLastSegment() && !canDiscardData) {
        FlushDumpSegmentInContainer();
    }
    if (mDumpSegmentContainer) {
        while (mDumpSegmentContainer->GetInMemSegmentContainer()->Size() > 0) {
            mDumpSegmentContainer->GetInMemSegmentContainer()->EvictOldestInMemSegment();
        }
    }
    mDumpSegmentContainer.reset();
    mPartitionDataHolder.Reset();
    if (mFileSystem) {
        mFileSystem->Sync(true).GetOrThrow();
    }
    mFileSystem.reset();
    mRtMemQuotaSynchronizer.reset();
    IndexPartition::Close();
    IE_PREFIX_LOG(INFO, "close index partition end, version[%d]", mLoadedIncVersion.GetVersionId());
}

PartitionWriterPtr OnlinePartition::GetWriter() const
{
    ScopedLock lock(mDataLock);
    return mWriter;
}

IndexPartitionReaderPtr OnlinePartition::GetReader() const
{
    ScopedLock lock(mReaderLock);
    return mReader;
}

int64_t OnlinePartition::GetRtSeekTimestampFromIncVersion() const
{
    // NOTE : no need lock, because getTimestamp() is atomic
    // ScopedLock lock(mDataLock);
    OnlineJoinPolicy joinPolicy(mLoadedIncVersion, mSchema->GetTableType(), mSrcSignature);
    return joinPolicy.GetRtSeekTimestampFromIncVersion();
}

IndexPartition::MemoryStatus OnlinePartition::CheckMemoryStatus() const
{
    if (!mResourceCalculator) {
        return MS_OK;
    }

    const OnlineConfig& onlineConfig = mOptions.GetOnlineConfig();
    // for reach max rt index memory use
    int64_t rtIndexMemUseInMB =
        (mResourceCalculator->GetRtIndexMemoryUse() + mDumpSegmentContainer->GetDumpingSegmentsSize() +
         mResourceCalculator->GetWriterMemoryUse()) /
        (1024 * 1024);

    int64_t maxRtIndexSizeInMB = onlineConfig.maxRealtimeMemSize;
    if (rtIndexMemUseInMB >= maxRtIndexSizeInMB) {
        IE_PREFIX_LOG(WARN,
                      "reach max Rt index size[%ld MB], "
                      "current rtIndexSize is %ld MB",
                      maxRtIndexSizeInMB, rtIndexMemUseInMB);
        return MS_REACH_MAX_RT_INDEX_SIZE;
    }

    if (mRtMemQuotaSynchronizer && mRtMemQuotaSynchronizer->GetFreeQuota() <= 0) {
        IE_PREFIX_LOG(WARN, "reach realtime quota control limit, current free quota[%lu]",
                      mRtMemQuotaSynchronizer->GetFreeQuota());
        return MS_REACH_MAX_RT_INDEX_SIZE;
    }

    // TODO: when reopen, control building mem not exceed BUILD_RESERVE_MEM

    // for free mem not enough for max reopen size
    int64_t availableMemoryUse = mPartitionMemController->GetFreeQuota() / (1024 * 1024);
    int64_t maxReopenMemoryUse;
    if (!onlineConfig.GetMaxReopenMemoryUse(maxReopenMemoryUse)) {
        maxReopenMemoryUse = 0;
    }

    uint64_t buildingSegmentDumpExpandMemUse = mResourceCalculator->GetBuildingSegmentDumpExpandSize() / (1024 * 1024);
    uint64_t dumpingSegmentDumpExpandMemUse = mDumpSegmentContainer->GetMaxDumpingSegmentExpandMemUse() / (1024 * 1024);
    uint64_t dumpExpandMemUse = max(buildingSegmentDumpExpandMemUse, dumpingSegmentDumpExpandMemUse);
    if (availableMemoryUse - maxReopenMemoryUse <= (int64_t)dumpExpandMemUse) {
        int64_t currentMemUse = mResourceCalculator->GetCurrentMemoryUse() / (1024 * 1024);
        IE_PREFIX_LOG(WARN,
                      "reach memory use limit free mem [%ld MB], "
                      "current total memory is %ld MB, maxReopenMemoryUse [%ld MB] "
                      "dumpExpandMemUse [%lu MB]",
                      availableMemoryUse, currentMemUse, maxReopenMemoryUse, dumpExpandMemUse);
        return MS_REACH_TOTAL_MEM_LIMIT;
    }
    return MS_OK;
}

util::MemoryReserverPtr
OnlinePartition::ReserveVirtualAttributesResource(const config::AttributeConfigVector& mainVirtualAttrConfigs,
                                                  const config::AttributeConfigVector& subVirtualAttrConfigs)
{
    ScopedLock lock(mDataLock);
    PartitionInfoPtr partitionInfo = mReader->GetPartitionInfo();
    size_t docCount = partitionInfo->GetTotalDocCount();
    AttributeSchemaPtr virtualAttrSchema = mRtSchema->GetVirtualAttributeSchema();
    size_t expandSize = EstimateAttributeExpandSize(virtualAttrSchema, mainVirtualAttrConfigs, docCount);
    if (subVirtualAttrConfigs.size() != 0) {
        PartitionInfoPtr subPartitionInfo = partitionInfo->GetSubPartitionInfo();
        size_t subDocCount = subPartitionInfo->GetTotalDocCount();
        AttributeSchemaPtr subVirtualAttrSchema = mRtSchema->GetSubIndexPartitionSchema()->GetVirtualAttributeSchema();
        expandSize += EstimateAttributeExpandSize(subVirtualAttrSchema, subVirtualAttrConfigs, subDocCount);
    }
    MemoryReserverPtr memReserver = mFileSystem->CreateMemoryReserver("virtual_attributes");
    if (memReserver->Reserve(expandSize)) {
        return memReserver;
    }

    return MemoryReserverPtr();
}

size_t OnlinePartition::EstimateAttributeExpandSize(const AttributeSchemaPtr& virtualAttrSchema,
                                                    const config::AttributeConfigVector& attributeConfigs,
                                                    size_t docCount)
{
    size_t expandSize = 0;
    for (size_t i = 0; i < attributeConfigs.size(); i++) {
        if (virtualAttrSchema && virtualAttrSchema->IsInAttribute(attributeConfigs[i]->GetAttrName())) {
            continue;
        }
        expandSize += attributeConfigs[i]->EstimateAttributeExpandSize(docCount);
    }
    return expandSize;
}

void OnlinePartition::AddVirtualAttributes(const AttributeConfigVector& mainAttrConfigs,
                                           const AttributeConfigVector& subAttrConfigs)
{
    ScopedLock lock(mDataLock);
    AttributeConfigVector mainVirtualAttrConfigs = mainAttrConfigs;
    AttributeConfigVector subVirtualAttrConfigs = subAttrConfigs;
    mReaderUpdater->FillVirtualAttributeConfigs(mainVirtualAttrConfigs, subVirtualAttrConfigs);
    IndexPartitionSchemaPtr newSchema = CreateNewSchema(mSchema, mainVirtualAttrConfigs, subVirtualAttrConfigs);
    if (!newSchema) {
        return;
    }

    AddVirtualAttributeDataCleaner(newSchema);

    IndexPartitionSchemaPtr newRtSchema = CreateRtSchema(mOptions);
    newRtSchema->CloneVirtualAttributes(*newSchema);

    if (mWriter) {
        mWriter->DumpSegment();
    }

    {
        ScopedLock schemaLock(mSchemaLock);
        mSchema = newSchema;
        mRtSchema = newRtSchema;
    }
    OpenExecutorChainCreatorPtr creator = CreateChainCreator();
    OpenExecutorChainPtr chain = creator->CreateReopenPartitionReaderChain(true);
    ResetCounter();
    ExecutorResource resource = CreateExecutorResource(mLoadedIncVersion, false);

    // Schema changed, create new inMemorySegment
    resource.mNeedInheritInMemorySegment = false;
    chain->Execute(resource);
    ReportMetrics();
}

bool OnlinePartition::CleanIndexFiles(const vector<versionid_t>& keepVersionIds)
{
    if (!mLocalIndexCleaner) {
        mLocalIndexCleaner.reset(new LocalIndexCleaner(mOpenIndexPrimaryDir, mOpenIndexSecondaryDir,
                                                       mOptions.GetOnlineConfig().onlineKeepVersionCount,
                                                       mReaderContainer));
    }
    return mLocalIndexCleaner->Clean(keepVersionIds);
}

bool OnlinePartition::CleanUnreferencedIndexFiles(const set<string>& toKeepFiles)
{
    // to be supported after OnlinePartition is migrated to tablet
    return false;
}

IndexPartitionSchemaPtr OnlinePartition::CreateNewSchema(const IndexPartitionSchemaPtr& schema,
                                                         const AttributeConfigVector& mainVirtualAttrConfigs,
                                                         const AttributeConfigVector& subVirtualAttrConfigs)
{
    IndexPartitionSchemaPtr newSchema = SchemaAdapter::LoadAndRewritePartitionSchema(
        GetFileSystemRootDirectory(), mOptions, schema->GetSchemaVersionId());
    // newSchema->CloneVirtualAttributes(*schema);
    newSchema->AddVirtualAttributeConfigs(mainVirtualAttrConfigs);
    const IndexPartitionSchemaPtr& newSubSchema = newSchema->GetSubIndexPartitionSchema();
    if (newSubSchema) {
        newSubSchema->AddVirtualAttributeConfigs(subVirtualAttrConfigs);
    }
    if (HasSameVirtualAttrSchema(newSchema, schema) &&
        HasSameVirtualAttrSchema(newSubSchema, schema->GetSubIndexPartitionSchema())) {
        return IndexPartitionSchemaPtr();
    }
    return newSchema;
}

bool OnlinePartition::HasSameVirtualAttrSchema(const IndexPartitionSchemaPtr& newSchema,
                                               const IndexPartitionSchemaPtr& oldSchema)
{
    if (!newSchema && !oldSchema) {
        return true;
    }
    AttributeSchemaPtr newAttrSchema = newSchema->GetVirtualAttributeSchema();
    AttributeSchemaPtr oldAttrSchema = oldSchema->GetVirtualAttributeSchema();
    if (!newAttrSchema) {
        return oldAttrSchema == AttributeSchemaPtr();
    }
    return newAttrSchema->HasSameAttributeConfigs(oldAttrSchema);
}

IndexPartitionSchemaPtr OnlinePartition::CreateRtSchema(const IndexPartitionOptions& options)
{
    IndexPartitionOptions clonedOptions = options;
    clonedOptions.SetNeedRewriteFieldType(true);
    IndexPartitionSchemaPtr rtSchema = SchemaAdapter::LoadAndRewritePartitionSchema(
        GetFileSystemRootDirectory(), clonedOptions, mSchema->GetSchemaVersionId());
    if (rtSchema->GetTableType() != tt_kkv && rtSchema->GetTableType() != tt_kv) {
        SchemaAdapter::RewriteToRtSchema(rtSchema);
    }

    if (clonedOptions.IsOnline() && rtSchema->GetTableType() == tt_kv) {
        if (rtSchema->GetRegionCount() > 1) {
            // rt schema not use global param, avoid rt compress
            autil::legacy::json::JsonMap emptyMap;
            rtSchema->SetGlobalRegionIndexPreference(emptyMap);
        }
        const IndexSchemaPtr& indexSchema = rtSchema->GetIndexSchema(DEFAULT_REGIONID);
        KVIndexConfigPtr kvConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
        const KVIndexPreference::ValueParam valueParam = kvConfig->GetIndexPreference().GetValueParam();
        // online kv table will disable value compress
        KVIndexPreference::ValueParam rewriteValueParam(valueParam.IsEncode(), "");
        if (valueParam.IsFixLenAutoInline()) {
            rewriteValueParam.EnableFixLenAutoInline();
        }
        kvConfig->GetIndexPreference().SetValueParam(rewriteValueParam);
    }
    return rtSchema;
}

void OnlinePartition::ResetRtAndJoinDirPath(const file_system::DirectoryPtr& dir)
{
    assert(dir);
    indexlib::file_system::RemoveOption removeOption = indexlib::file_system::RemoveOption::MayNonExist();
    dir->RemoveDirectory(JOIN_INDEX_PARTITION_DIR_NAME, removeOption);
    if (!mOptions.GetOnlineConfig().loadRemainFlushRealtimeIndex) {
        dir->RemoveDirectory(RT_INDEX_PARTITION_DIR_NAME, removeOption);
    }
}

bool OnlinePartition::HasFlushRtIndex()
{
    DirectoryPtr localRtDir =
        GetFileSystemRootDirectory()->MakeDirectory(RT_INDEX_PARTITION_DIR_NAME, DirectoryOption::Mem());
    OnDiskPartitionDataPtr localPartitionData =
        OnDiskPartitionData::CreateRealtimePartitionData(localRtDir, true, plugin::PluginManagerPtr());
    assert(localPartitionData);
    auto version = localPartitionData->GetVersion();
    return version.GetSegmentCount() > 0;
}

PartitionModifierPtr OnlinePartition::CreateReaderModifier(const IndexPartitionReaderPtr& reader)
{
    assert(reader);
    ScopedLock schemaLockGuard(mSchemaLock);
    PartitionModifierPtr modifier = PartitionModifierCreator::CreateInplaceModifier(mSchema, reader);
    return modifier;
}

bool OnlinePartition::InitAccessCounters(const CounterMapPtr& counterMap, const IndexPartitionSchemaPtr& schema)
{
    vector<string> temperatures = {".HOT", ".WARM", ".COLD"};
    assert(counterMap);
    IndexMetrics* indexMetrics = mOnlinePartMetrics->GetIndexMetrics();
    AttributeMetrics* attrMetrics = mOnlinePartMetrics->GetAttributeMetrics();
    if (indexMetrics) {
        const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
        assert(indexSchema);
        string counterNodePrefix = "online.access.index.";
        auto indexConfigs = indexSchema->CreateIterator(true);
        auto iter = indexConfigs->Begin();
        for (; iter != indexConfigs->End(); iter++) {
            const IndexConfigPtr& indexConfig = *iter;
            const string& indexName = indexConfig->GetIndexName();
            string counterNodePath = counterNodePrefix + indexName;
            const auto& counter = counterMap->GetAccCounter(counterNodePath);
            if (!counter) {
                IE_PREFIX_LOG(ERROR, "get counter[%s] failed]", counterNodePath.c_str());
                return false;
            }
            indexMetrics->AddAccessCounter(indexName, counter);
        }
    }
    if (attrMetrics) {
        const AttributeSchemaPtr& attrSchema = schema->GetAttributeSchema();
        string counterNodePrefix = "online.access.attribute.";
        if (attrSchema) {
            auto attrConfigs = attrSchema->CreateIterator();
            auto iter = attrConfigs->Begin();
            for (; iter != attrConfigs->End(); iter++) {
                const AttributeConfigPtr& attrConfig = *iter;
                const string& attrName = attrConfig->GetAttrName();
                if (mNeedReportTemperature && mSchema->EnableTemperatureLayer()) {
                    for (size_t i = 0; i < temperatures.size(); i++) {
                        string counterNodePath = counterNodePrefix + attrName + temperatures[i];
                        const auto& counter = counterMap->GetAccCounter(counterNodePath);
                        if (!counter) {
                            IE_PREFIX_LOG(ERROR, "get counter[%s] failed", counterNodePath.c_str());
                            return false;
                        }
                        string accName = attrName + temperatures[i];
                        attrMetrics->AddAccessCounter(accName, counter);
                    }
                } else {
                    string counterNodePath = counterNodePrefix + attrName;
                    const auto& counter = counterMap->GetAccCounter(counterNodePath);
                    if (!counter) {
                        IE_PREFIX_LOG(ERROR, "get counter[%s] failed", counterNodePath.c_str());
                        return false;
                    }
                    attrMetrics->AddAccessCounter(attrName, counter);
                }
            }
        }
    }
    const IndexPartitionSchemaPtr& subSchema = schema->GetSubIndexPartitionSchema();
    if (subSchema) {
        InitAccessCounters(mCounterMap, subSchema);
    }
    return true;
}

bool OnlinePartition::ExecuteTask(OnlinePartitionTaskItem::TaskType taskType)
{
    switch (taskType) {
    case OnlinePartitionTaskItem::TT_REPORT_METRICS: {
        ReportMetrics();
        return true;
    }
    case OnlinePartitionTaskItem::TT_CLEAN_RESOURCE: {
        if (mCleanerLock.trylock() != 0) {
            return false;
        }

        versionid_t loadedVersion = INVALID_VERSION;
        if (mDataLock.trylock() != 0) {
            mCleanerLock.unlock();
            return false;
        } else {
            loadedVersion = mLoadedIncVersion.GetVersionId();
            mDataLock.unlock();
        }

        ExecuteCleanResourceTask(loadedVersion);
        mCleanerLock.unlock();
        return true;
    }
    case OnlinePartitionTaskItem::TT_INTERVAL_DUMP: {
        DumpSegmentOverInterval();
        return true;
    }
    case OnlinePartitionTaskItem::TT_TRIGGER_ASYNC_DUMP: {
        if (mAsyncSegmentDumper) {
            mAsyncSegmentDumper->TriggerDumpIfNecessary();
        }
        return true;
    }
    case OnlinePartitionTaskItem::TT_CHECK_SECONDARY_INDEX: {
        CheckSecondIndex();
        return true;
    }
    case OnlinePartitionTaskItem::TT_SUBSCRIBE_SECONDARY_INDEX: {
        SubscribeSecondIndex();
        return true;
    }
    case OnlinePartitionTaskItem::TT_SYNC_REALTIME_INDEX: {
        if (mRealtimeIndexSynchronizer) {
            mRealtimeIndexSynchronizer->SyncToRemote(mPartitionDataHolder.Get(), mRealtimeIndexSyncThreadPool);
        }
    }
    case OnlinePartitionTaskItem::TT_CALCULATE_METRICS: {
        CalculateMetrics();
    }
    default:
        break;
    }
    return false;
}

void OnlinePartition::ExecuteCleanResourceTask(versionid_t loadedVersion)
{
    if (mClosed) {
        return;
    }
    try {
        ScopedLock lock(mCleanerLock);
        if (!mIsServiceNormal || !mPartitionDataHolder.Get()) {
            return;
        }
        CleanResource();
        DoReopen(false, loadedVersion);
    } catch (ExceptionBase& e) {
        IE_PREFIX_LOG(ERROR, "clean resource exception [%s]!!", e.what());
    } catch (...) {
        IE_PREFIX_LOG(ERROR, "clean resource exception!!");
    }
}

IndexPartition::OpenStatus OnlinePartition::NormalReopen(Version& onDiskVersion)
{
    ScopedLock scopedDumpLock(mDumpLock);
    ExecutorResource&& estimateResource = CreateExecutorResource(onDiskVersion, false, true);
    size_t incExpandMemSizeInBytes = OpenExecutorUtil::EstimateReopenMemoryUse(estimateResource, mPartitionName);
    int64_t maxReopenMemoryUseInMB = 0;
    if (mOptions.GetOnlineConfig().GetMaxReopenMemoryUse(maxReopenMemoryUseInMB) &&
        incExpandMemSizeInBytes >= (size_t)maxReopenMemoryUseInMB * 1024 * 1024) {
        IE_PREFIX_LOG(WARN, "incExpandMemSizeInMB [%lu] over maxReopenMemoryUse [%ld]",
                      incExpandMemSizeInBytes / (1024 * 1024), maxReopenMemoryUseInMB);
        return OS_FORCE_REOPEN;
    }
    ScopedLockPtr scopedLock(new ScopedLock(mDataLock));
    IE_PREFIX_LOG(INFO, "normal reopen begin, version from [%d] to [%d]", mLoadedIncVersion.GetVersionId(),
                  onDiskVersion.GetVersionId());
    IndexPhaseGuard phaseGuard(mIndexPhase, NORMAL_REOPEN);
    OpenExecutorChainCreatorPtr creator = CreateChainCreator();
    OpenExecutorChainPtr chain = creator->CreateReopenExecutorChain(
        mSchema, mFileSystem, mPartitionMemController, mPartitionDataHolder, mLoadedIncVersion, onDiskVersion,
        scopedLock, mDataLock, mDumpLock, mOptions, mOnlinePartMetrics, mEnableAsyncDump, mMaxRedoTime);

    ExecutorResource resource = CreateExecutorResource(onDiskVersion, false, true);
    if (!chain->Execute(resource)) {
        IE_PREFIX_LOG(INFO, "normal reopen fail, version[%d]", onDiskVersion.GetVersionId());
        auto lifecycleTable = GetLifecycleTableFromVersion(mLoadedIncVersion);
        IE_LOG(INFO, "rollback lifecycle for version [%d] of table[%s]", mLoadedIncVersion.GetVersionId(),
               mSchema->GetSchemaName().c_str());
        RefreshLifecycleProperty(lifecycleTable);
        return OS_FAIL;
    }
    mLatestNeedSkipIncTs = resource.mLatestNeedSkipIncTs;

    MEMORY_BARRIER();
    ReportMetrics();
    AddOnDiskIndexCleaner();
    IE_PREFIX_LOG(INFO, "normal reopen success, version[%d]", onDiskVersion.GetVersionId());
    return OS_OK;
}

ExecutorResource OnlinePartition::CreateExecutorResource(const Version& onDiskVersion, bool forceReopen,
                                                         bool needSnapshot)
{
    ScopedLockPtr scopedLock(new ScopedLock(mDataLock));
    ExecutorResource resource(mLoadedIncVersion, mReader, mReaderLock, mWriter, mPartitionDataHolder,
                              *mOnlinePartMetrics, mNeedReload, mOptions, GetLatestNeedSkipIncTs(onDiskVersion));

    resource.mIncVersion = onDiskVersion;
    resource.mFileSystem = mFileSystem;
    {
        ScopedLock schemaLock(mSchemaLock);
        resource.mSchema = mSchema;
        resource.mRtSchema = mRtSchema;
    }
    resource.mReaderContainer = mReaderContainer;
    resource.mDumpSegmentContainer = mDumpSegmentContainer;
    resource.mCleanerLock = &mCleanerLock;
    resource.mResourceCleaner = mResourceCleaner;
    resource.mResourceCalculator = mResourceCalculator;
    resource.mPartitionMemController = mPartitionMemController;
    resource.mSearchCache = mSearchCache;
    resource.mCounterMap = mCounterMap;
    resource.mForceReopen = forceReopen;
    resource.mPluginManager = mPluginManager;
    resource.mIndexPhase = mIndexPhase;
    resource.mNeedReportTemperatureAccess = mNeedReportTemperature;
    resource.mSrcSignature = mSrcSignature;

    if (needSnapshot) {
        // on force reopen, we need to release all partition data and file, can not clone
        // also, it'snapshot partition data is useless in force repoen
        resource.mSnapshotPartitionData.reset(mPartitionDataHolder.Get()->Snapshot(&mDataLock));
    }
    return resource;
}

segmentid_t OnlinePartition::GetLatestLinkRtSegId(const PartitionDataPtr& partData,
                                                  const MemoryReserverPtr& memReserver) const
{
    assert(partData);
    assert(memReserver);

    segmentid_t lastReaderLinkRtSegId =
        !mReader ? INVALID_SEGMENTID : mReader->GetPartitionVersion()->GetLastLinkRtSegmentId();
    segmentid_t lastValidLinkRtSegId = partData->GetLastValidRtSegmentInLinkDirectory();
    if (lastValidLinkRtSegId == INVALID_SEGMENTID) {
        return INVALID_SEGMENTID;
    }

    OnDiskRealtimeDataCalculator rtCalculator;
    rtCalculator.Init(mSchema, partData, mPluginManager);
    size_t switchExpandMemSize = rtCalculator.CalculateExpandSize(lastReaderLinkRtSegId, lastValidLinkRtSegId);
    if (switchExpandMemSize > 0) {
        if (!memReserver->Reserve(switchExpandMemSize)) {
            IE_PREFIX_LOG(WARN,
                          "switch to latest flush rt segments [%d] fail."
                          "mem need [%ld], mem left [%ld]",
                          lastValidLinkRtSegId, switchExpandMemSize, memReserver->GetFreeQuota());
            return lastReaderLinkRtSegId;
        }
    }
    return lastValidLinkRtSegId;
}

void OnlinePartition::UpdateSwitchRtSegmentSize()
{
    if (mResourceCalculator && mOptions.GetOnlineConfig().onDiskFlushRealtimeIndex) {
        // clean thread may cause rt segment be removed
        // update switch rt segment may throw exception for no segment_info file
        if (mCleanerLock.trylock() != 0) {
            return;
        }

        try {
            vector<segmentid_t> switchRtSegmentIds;
            mReaderContainer->GetSwitchRtSegments(switchRtSegmentIds);
            mResourceCalculator->UpdateSwitchRtSegments(mRtSchema, switchRtSegmentIds);
        } catch (const ExceptionBase& e) {
            IE_PREFIX_LOG(ERROR,
                          "error occurred in calculate size"
                          " for UpdateSwitchRtSegments, [%s]",
                          e.what());
        } catch (...) {
            IE_PREFIX_LOG(ERROR, "error occurred in calculate size"
                                 " for UpdateSwitchRtSegments");
        }
        mCleanerLock.unlock();
    }
}

int64_t OnlinePartition::GetLatestNeedSkipIncTs(const Version& incVersion) const
{
    string locatorStr = GetLastLocator();

    IndexLocator locator;
    if (!locatorStr.empty() && !locator.fromString(locatorStr)) {
        IE_PREFIX_LOG(WARN, "deserialze rt locator fail!");
        return mLatestNeedSkipIncTs;
    }

    if (locator == INVALID_INDEX_LOCATOR) {
        return mLatestNeedSkipIncTs;
    }
    OnlineJoinPolicy joinPolicy(incVersion, mSchema->GetTableType(), mSrcSignature);

    bool fromInc;
    int64_t rtSeekTs = joinPolicy.GetRtSeekTimestamp(locator, fromInc);
    if (fromInc) {
        return rtSeekTs;
    }
    return mLatestNeedSkipIncTs;
}

void OnlinePartition::DumpSegmentOverInterval()
{
    if (mDataLock.trylock() != 0) {
        return;
    }

    if (!mWriter) {
        mDataLock.unlock();
        return;
    }

    int64_t maxDumpInterval = mOptions.GetOnlineConfig().maxRealtimeDumpInterval;
    int64_t currentTimeInSeconds = TimeUtility::currentTimeInSeconds();
    if (currentTimeInSeconds - mWriter->GetLastDumpTimestamp() < maxDumpInterval) {
        mDataLock.unlock();
        return;
    }

    IE_PREFIX_LOG(INFO, "trigger realtime dumpsegment by dump interval [%ld]", maxDumpInterval);
    OpenExecutorPtr dumpSegmentExecutor = OpenExecutorPtr(new DumpSegmentExecutor(mPartitionName, true));
    ExecutorResource resource = CreateExecutorResource(mLoadedIncVersion, false);
    try {
        dumpSegmentExecutor->Execute(resource);
    } catch (const ExceptionBase& e) {
        IE_PREFIX_LOG(ERROR, "error occurred in onlinepartition DumpSegmentOverInterval[%s]", e.what());
    }
    if (mReaderUpdater) {
        mReaderUpdater->Update(mReader);
    }

    mDataLock.unlock();
}

void OnlinePartition::FlushDumpSegmentInContainer()
{
    IE_PREFIX_LOG(INFO, "flush dump segment begin.");
    segmentid_t dumpSegId = INVALID_SEGMENTID;
    ScopedLock dumpLockGuard(mDumpLock);
    if (mWriter || !mClosed || (mDumpSegmentContainer && mDumpSegmentContainer->GetLastSegment())) {
        bool hasDumpedSegment = false;
        {
            ScopedLock lock(mCleanerLock);
            int64_t newIncVersionTimestamp = INVALID_TIMESTAMP;
            {
                ScopedLock dataLockGuard(mDataLock);
                Version onDiskNewIncVersion = mPartitionDataHolder.Get()->GetOnDiskVersion();
                newIncVersionTimestamp = onDiskNewIncVersion.GetTimestamp();
                if (newIncVersionTimestamp > 0) {
                    mDumpSegmentContainer->SetReclaimTimestamp(newIncVersionTimestamp);
                    mDumpSegmentContainer->TrimObsoleteDumpingSegment(mDumpSegmentContainer->GetReclaimTimestamp());
                }
            }

            NormalSegmentDumpItemPtr item = mDumpSegmentContainer->GetOneValidSegmentItemToDump();
            if (item != nullptr) {
                dumpSegId = item->GetSegmentId();
                try {
                    item->Dump();
                } catch (const util::ExceptionBase& e) {
                    IE_PREFIX_LOG(ERROR,
                                  "flush dump segment end. "
                                  "because catch exception:[%s].",
                                  e.what());
                    return;
                }
                hasDumpedSegment = true;
            }
            if (!mClosed && hasDumpedSegment) {
                try {
                    if (mSchema->GetTableType() == tt_kv || mSchema->GetTableType() == tt_kkv) {
                        mPartitionDataHolder.Get()->CommitVersion();
                        ReOpenNewSegment();
                    } else {
                        RedoOperations();
                    }
                    CleanResource();
                } catch (const util::ExceptionBase& e) {
                    IE_PREFIX_LOG(ERROR,
                                  "async dump segment thread end. "
                                  "because reopen new segment catch exception:[%s].",
                                  e.what());
                    return;
                }
            }
        }
    }
    IE_PREFIX_LOG(INFO, "flush dump segment end. dumpSegId[%d]", dumpSegId);
}

MemoryQuotaSynchronizerPtr
OnlinePartition::CreateRealtimeMemoryQuotaSynchronizer(const MemoryQuotaControllerPtr& realtimeQuotaController)
{
    assert(realtimeQuotaController);
    PartitionMemoryQuotaControllerPtr rtPartitionMemoryQuotaControl(
        new PartitionMemoryQuotaController(realtimeQuotaController, "realtime_partition_controller"));

    BlockMemoryQuotaControllerPtr blockMemQuotaControl(
        new BlockMemoryQuotaController(rtPartitionMemoryQuotaControl, "block_realtime_partition_controller"));

    MemoryQuotaSynchronizerPtr memQuotaSync(new MemoryQuotaSynchronizer(blockMemQuotaControl));
    return memQuotaSync;
}

namespace {
bool TryLock(autil::ThreadMutex& lock, size_t times, int64_t intervalInUs)
{
    size_t trys = 0;
    while (trys++ < times) {
        if (lock.trylock() == 0) {
            return true;
        }
        usleep(intervalInUs);
    }
    return false;
}
} // namespace

void OnlinePartition::CheckSecondIndex()
{
    index_base::Version loadedIncVersion;
    if (!TryLock(mDataLock, 5, 1000)) {
        return;
    }

    loadedIncVersion = mLoadedIncVersion;
    mDataLock.unlock();

    int64_t missingSegmentCount = 0;
    string versionPath = loadedIncVersion.GetVersionFileName();
    bool hasException = false;
    try {
        file_system::ErrorCode ec = mFileSystem->Validate(versionPath);
        if (ec == FSEC_NOENT) {
            missingSegmentCount = loadedIncVersion.GetSegmentCount();
            IE_PREFIX_LOG(ERROR, "segment version [%s] not exist", versionPath.c_str());
        } else if (ec == FSEC_OK) {
            const auto& segmentVec = loadedIncVersion.GetSegmentVector();
            for (const auto& segmentId : segmentVec) {
                string segmentPath = loadedIncVersion.GetSegmentDirName(segmentId);
                string segInfoPath = util::PathUtil::JoinPath(segmentPath, SEGMENT_INFO_FILE_NAME);
                file_system::ErrorCode ec = mFileSystem->Validate(segInfoPath);
                if (ec == FSEC_OK) {
                    // pass
                } else if (ec == FSEC_NOENT) {
                    ++missingSegmentCount;
                    IE_PREFIX_LOG(ERROR, "segment_info [%s] not exist", segInfoPath.c_str());
                } else {
                    hasException = true;
                }
            }
        } else {
            hasException = true;
        }
    } catch (const ExceptionBase& e) {
        hasException = true;
        IE_PREFIX_LOG(ERROR, "caught exception %s", e.GetMessage().c_str());
    } catch (...) {
        hasException = true;
        auto eptr = std::current_exception();
        INDEXLIB_HANDLE_EPTR(eptr);
    }

    if (missingSegmentCount > 0 || (missingSegmentCount == 0 && !hasException)) {
        if (!TryLock(mDataLock, 5, 1000)) {
            return;
        }
        if (loadedIncVersion.GetVersionId() == mLoadedIncVersion.GetVersionId()) {
            mMissingSegmentCount = missingSegmentCount;
        }
        mDataLock.unlock();
    }
}

void OnlinePartition::SubscribeSecondIndex()
{
    if (mOpenIndexSecondaryDir.empty()) {
        IE_LOG(WARN, "secondary path is empty, do not need subsrcibe second index");
        return;
    }

    versionid_t loadedIncVersionId = mLoadedIncVersion.GetVersionId();
    std::string lockPath =
        PathUtil::JoinPath(mOpenIndexSecondaryDir, DeployIndexWrapper::GetDeployMetaLockFileName(loadedIncVersionId));
    std::string ipAddr;
    if (!NetUtil::GetDefaultIp(ipAddr)) {
        IE_PREFIX_LOG(ERROR, "Get IpAddr failed!");
        return;
    }
    if (FslibWrapper::UpdatePanguInlineFile(lockPath, ipAddr).Code() != FSEC_OK) {
        IE_PREFIX_LOG(ERROR, "Update deploy meta lock file [%s] failed!", lockPath.c_str());
    } else {
        IE_PREFIX_LOG(INFO, "Update deploy meta lock file [%s] successfully, ipAddr [%s]", lockPath.c_str(),
                      ipAddr.c_str());
    }
}

bool OnlinePartition::NeedSubscribeSecondIndex() const
{
    return mSubscribeSecondIndexIntervalInMin > 0 && mOptions.GetOnlineConfig().NeedReadRemoteIndex() &&
           !mOpenIndexSecondaryDir.empty() && PathUtil::SupportPanguInlineFile(mOpenIndexSecondaryDir);
}

void OnlinePartition::ResetCounter()
{
    if (!mPartitionDataHolder.Get()) {
        return;
    }
    PartitionDataPtr clonedPartData(mPartitionDataHolder.Get()->Clone());
    clonedPartData->ResetCounters();
}

void OnlinePartition::CalculateMetrics() const
{
    PartitionDataPtr currentPartData;
    {
        std::vector<InMemorySegmentPtr> dumpingSegments;
        std::set<segmentid_t> dumpingSegmentIds;
        ScopedLock dataLockGuard(mDataLock);
        if (mDumpSegmentContainer != nullptr) {
            mDumpSegmentContainer->GetDumpingSegments(dumpingSegments);
            for (const auto& seg : dumpingSegments) {
                dumpingSegmentIds.insert(seg->GetSegmentId());
            }
        }

        IndexPartitionReaderPtr reader = GetReader();
        auto partData = mPartitionDataHolder.Get();
        if (reader == nullptr || partData == nullptr) {
            return;
        }

        segmentid_t lastValidLinkRtSegId = partData->GetLastValidRtSegmentInLinkDirectory();
        InMemorySegmentPtr inMemSeg = partData->GetInMemorySegment();
        segmentid_t inMemSegmentId = inMemSeg ? inMemSeg->GetSegmentId() : INVALID_SEGMENTID;
        PartitionVersion partitionVersion(partData->GetVersion(), inMemSegmentId, lastValidLinkRtSegId,
                                          dumpingSegmentIds);
        if (partitionVersion.GetHashId() == reader->GetPartitionVersionHashId()) {
            return;
        }
        currentPartData.reset(partData->Clone());
    }

    mResourceCalculator->CalculateCurrentIndexSize(currentPartData, mSchema);
}

shared_ptr<file_system::LifecycleTable> OnlinePartition::GetLifecycleTableFromVersion(const Version& version)
{
    auto lifecycleTable = std::make_shared<LifecycleTable>();
    auto lifecycleConfig = mOptions.GetOnlineConfig().GetLifecycleConfig();
    auto lifecycleStrategy = indexlib::index_base::LifecycleStrategyFactory::CreateStrategy(lifecycleConfig, {});
    std::vector<std::pair<segmentid_t, std::string>> segLifecycles = lifecycleStrategy->GetSegmentLifecycles(version);
    for (const auto& kv : segLifecycles) {
        std::string segmentDir = version.GetSegmentDirName(kv.first);
        lifecycleTable->AddDirectory(segmentDir, kv.second);
    }
    return lifecycleTable;
}

void OnlinePartition::RefreshLifecycleProperty(const shared_ptr<file_system::LifecycleTable>& lifecycleTable)
{
    if (lifecycleTable == nullptr) {
        return;
    }
    for (auto it = lifecycleTable->Begin(); it != lifecycleTable->End(); it++) {
        mFileSystem->SetDirLifecycle(it->first, it->second);
    }
}

#undef IE_LOG_PREFIX
}} // namespace indexlib::partition
