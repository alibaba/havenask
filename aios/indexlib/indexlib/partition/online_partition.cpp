#include <autil/HashAlgorithm.h>
#include <autil/TimeUtility.h>
#include "indexlib/partition/online_partition.h"
#include "indexlib/partition/online_partition_writer.h"
#include "indexlib/partition/online_partition_reader.h"
#include "indexlib/partition/on_disk_partition_data.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/partition/index_partition_resource.h"
#include "indexlib/partition/memory_stat_collector.h"
#include "indexlib/partition/memory_stat_reporter.h"
#include "indexlib/partition/table_reader_container_updater.h"
#include "indexlib/partition/patch_loader.h"
#include "indexlib/partition/modifier/partition_modifier_creator.h"
#include "indexlib/partition/on_disk_realtime_data_calculator.h"
#include "indexlib/partition/on_disk_index_cleaner.h"
#include "indexlib/partition/partition_reader_cleaner.h"
#include "indexlib/partition/local_index_cleaner.h"
#include "indexlib/partition/virtual_attribute_container.h"
#include "indexlib/partition/in_memory_index_cleaner.h"
#include "indexlib/partition/in_mem_virtual_attribute_cleaner.h"
#include "indexlib/partition/segment/in_memory_segment_cleaner.h"
#include "indexlib/partition/segment/async_segment_dumper.h"
#include "indexlib/partition/segment/in_memory_segment_container.h"
#include "indexlib/partition/segment/normal_segment_dump_item.h"
#include "indexlib/partition/join_segment_writer.h"
#include "indexlib/partition/deploy_index_validator.h"
#include "indexlib/partition/open_executor/lock_executor.h"
#include "indexlib/partition/open_executor/preload_executor.h"
#include "indexlib/partition/open_executor/prejoin_executor.h"
#include "indexlib/partition/open_executor/reclaim_rt_index_executor.h"
#include "indexlib/partition/open_executor/reclaim_rt_segments_executor.h"
#include "indexlib/partition/open_executor/generate_join_segment_executor.h"
#include "indexlib/partition/open_executor/reopen_partition_reader_executor.h"
#include "indexlib/partition/open_executor/release_partition_reader_executor.h"
#include "indexlib/partition/open_executor/dump_segment_executor.h"
#include "indexlib/partition/open_executor/dump_container_flush_executor.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/index_meta/index_format_version.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/index_base/version_committer.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/join_segment_directory.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"
#include "indexlib/common/index_locator.h"
#include "indexlib/common/executor_scheduler.h"
#include "indexlib/common/numeric_compress/encoder_provider.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/kv_index_config.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_plugin_resource.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/index/online_join_policy.h"
#include "indexlib/misc/exception.h"
#include "indexlib/util/path_util.h"
#include "indexlib/util/memory_control/memory_quota_controller_creator.h"
#include "indexlib/util/memory_control/memory_quota_synchronizer.h"
#include "indexlib/util/memory_control/memory_reserver.h"
#include "indexlib/util/counter/counter_map.h"
#include "indexlib/util/task_scheduler.h"
#include "indexlib/util/net_util.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/plugin/index_plugin_loader.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/file_system/indexlib_file_system.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(plugin);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, OnlinePartition);

#define IE_LOG_PREFIX mPartitionName.c_str()

OnlinePartition::OnlinePartition(
    const string& partitionName, const IndexPartitionResource& partitionResource)
    : IndexPartition(partitionName, partitionResource)
    , mSearchCache(partitionResource.searchCache)
    , mOnlinePartMetrics(new OnlinePartitionMetrics(partitionResource.metricProvider))
    , mClosed(true)
    , mIsServiceNormal(false)
    , mIndexPhase(PENDING)
    , mCleanResourceTaskId(TaskScheduler::INVALID_TASK_ID)
    , mReportMetricsTaskId(TaskScheduler::INVALID_TASK_ID)
    , mIntervalDumpTaskId(TaskScheduler::INVALID_TASK_ID)
    , mAsyncDumpTaskId(TaskScheduler::INVALID_TASK_ID)
    , mLatestNeedSkipIncTs(INVALID_TIMESTAMP)
    , mCheckIndexTaskId(TaskScheduler::INVALID_TASK_ID)
    , mCheckSecondIndexIntervalInMin(partitionResource.checkSecondIndexIntervalInMin)
    , mSubscribeIndexTaskId(TaskScheduler::INVALID_TASK_ID)
    , mSubscribeSecondIndexIntervalInMin(partitionResource.subscribeSecondIndexIntervalInMin)
    , mMissingSegmentCount(0)
    , mFutureExecutor(nullptr)
{
    if (partitionResource.memStatReporter)
    {
        MemoryStatCollectorPtr collector =
            partitionResource.memStatReporter->GetMemoryStatCollector();
        if (collector)
        {
            collector->AddTableMetrics(partitionResource.partitionGroupName, mOnlinePartMetrics);
        }
    }
    mRealtimeQuotaController = partitionResource.realtimeQuotaController;
    mPartitionIdentifier = autil::HashAlgorithm::hashString64(
            mPartitionName.c_str(), mPartitionName.length());
}

OnlinePartition::OnlinePartition(const string& partitionName,
                                 const MemoryQuotaControllerPtr& memQuotaController,
                                 MetricProviderPtr metricProvider)
    : IndexPartition(memQuotaController, partitionName)
    , mOnlinePartMetrics(new OnlinePartitionMetrics(metricProvider))
    , mClosed(true)
    , mIsServiceNormal(false)
    , mIndexPhase(PENDING)
    , mCleanResourceTaskId(TaskScheduler::INVALID_TASK_ID)
    , mReportMetricsTaskId(TaskScheduler::INVALID_TASK_ID)
    , mIntervalDumpTaskId(TaskScheduler::INVALID_TASK_ID)
    , mAsyncDumpTaskId(TaskScheduler::INVALID_TASK_ID)
    , mLatestNeedSkipIncTs(INVALID_TIMESTAMP)
    , mCheckIndexTaskId(TaskScheduler::INVALID_TASK_ID)
    , mCheckSecondIndexIntervalInMin(-1)
    , mSubscribeIndexTaskId(TaskScheduler::INVALID_TASK_ID)
    , mSubscribeSecondIndexIntervalInMin(-1)
    , mMissingSegmentCount(0)
    , mFutureExecutor(nullptr)
{
    mMetricProvider = metricProvider;
    mTaskScheduler.reset(new TaskScheduler());
    mPartitionIdentifier = autil::HashAlgorithm::hashString64(
            mPartitionName.c_str(), mPartitionName.length());
}

OnlinePartition::OnlinePartition()
    : mOnlinePartMetrics(new OnlinePartitionMetrics(NULL))
    , mClosed(true)
    , mIsServiceNormal(false)
    , mIndexPhase(PENDING)
    , mCleanResourceTaskId(TaskScheduler::INVALID_TASK_ID)
    , mReportMetricsTaskId(TaskScheduler::INVALID_TASK_ID)
    , mIntervalDumpTaskId(TaskScheduler::INVALID_TASK_ID)
    , mAsyncDumpTaskId(TaskScheduler::INVALID_TASK_ID)
    , mLatestNeedSkipIncTs(INVALID_TIMESTAMP)
    , mPartitionIdentifier(0)
    , mCheckIndexTaskId(TaskScheduler::INVALID_TASK_ID)
    , mCheckSecondIndexIntervalInMin(-1)
    , mSubscribeIndexTaskId(TaskScheduler::INVALID_TASK_ID)
    , mSubscribeSecondIndexIntervalInMin(-1)
    , mMissingSegmentCount(0)
    , mFutureExecutor(nullptr)
{
    mMetricProvider = NULL;
    mMemQuotaController = util::MemoryQuotaControllerCreator::CreateMemoryQuotaController();
    mTaskScheduler.reset(new TaskScheduler());
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
    try
    {
        Close();
    }
    catch(const ExceptionBase& e)
    {
        IE_PREFIX_LOG(FATAL, "error occurred in OnlinePartition deconstructor[%s]", e.what());
    }
    Reset();
}

void OnlinePartition::SetEnableReleaseOperationAfterDump(bool releaseOperations)
{
    if (mWriter)
    {
        mWriter->SetEnableReleaseOperationAfterDump(releaseOperations);
    }
}


IndexPartition::OpenStatus OnlinePartition::Open(
        const string& primaryDir, const string& secondaryDir,
        const IndexPartitionSchemaPtr& schema,
        const IndexPartitionOptions& options, versionid_t targetVersionId)
{
    int64_t beginTime = TimeUtility::currentTime();
    IE_PREFIX_LOG(INFO, "open partition begin, verison[%d]", targetVersionId);
    IndexPartition::OpenStatus os = OS_FAIL;
    try
    {
        os = DoOpen(primaryDir, secondaryDir, schema, options, targetVersionId, OPENING);
        if (os != OS_OK)
        {
            IE_PREFIX_LOG(ERROR, "open partition failed. Partition is to be closed");
            DoClose();
        }
    }
    catch (const FileIOException& e)
    {
        IE_PREFIX_LOG(ERROR, "open caught file io exception: %s", e.what());
        os = OS_FILEIO_EXCEPTION;
    }
    catch (const ExceptionBase& e)
    {
        // BadParameterException, InconsistentStateException, ...
        IE_PREFIX_LOG(ERROR, "open caught exception: %s", e.what());
        os = OS_INDEXLIB_EXCEPTION;
    }
    catch (const exception& e)
    {
        IE_PREFIX_LOG(ERROR, "open caught std exception: %s", e.what());
        os = OS_UNKNOWN_EXCEPTION;
    }
    catch (...)
    {
        IE_PREFIX_LOG(ERROR, "open caught unknown exception");
        os = OS_UNKNOWN_EXCEPTION;
    }
    IE_PREFIX_LOG(INFO, "open partition end, verison[%d], os[%d], used[%.3f]s",
                  targetVersionId, os, (TimeUtility::currentTime() - beginTime) / 1000000.0);
    return os;
}

IndexPartition::OpenStatus OnlinePartition::DoOpen(
        const string& primaryDir, const string& secondaryDir,
        const IndexPartitionSchemaPtr& schema,
        const IndexPartitionOptions& options, versionid_t targetVersionId,
        IndexPartition::IndexPhase indexPhase)
{
    IE_PREFIX_LOG(INFO, "open index partition: primary[%s], secondary[%s], version[%d]",
                  primaryDir.c_str(), secondaryDir.c_str(), targetVersionId);
    IndexPhaseGuard phaseGuard(mIndexPhase, indexPhase);
    IndexPartition::OpenStatus openStatus = IndexPartition::Open(
            primaryDir, secondaryDir, schema, options, targetVersionId);
    assert(openStatus == OS_OK);
    (void)openStatus;

    mReaderContainer.reset(new ReaderContainer);
    if (!mDumpSegmentContainer)
    {
        // mDumpSegmentContainer should be reset in Close()
        mDumpSegmentContainer.reset(new DumpSegmentContainer());
    }
    if (!mRealtimeQuotaController)
    {
        mRealtimeQuotaController.reset(new MemoryQuotaController(
                        mOptions.GetOnlineConfig().maxRealtimeMemSize * 1024 * 1024));
    }
    mRtMemQuotaSynchronizer = CreateRealtimeMemoryQuotaSynchronizer(mRealtimeQuotaController);

    mClosed = true;

    Version onDiskVersion;
    VersionLoader::GetVersion(GetFileSystemRootDirectory(), onDiskVersion, targetVersionId);
    InitPathMetaCache(GetFileSystemRootDirectory(), onDiskVersion);

    ScopedLock lock(mDataLock);

    RewriteSchemaAndOptions(schema, onDiskVersion);

    mOnlinePartMetrics->RegisterMetrics(mSchema->GetTableType());

    mCounterMap.reset(new CounterMap());
    if (mOptions.GetOnlineConfig().loadRemainFlushRealtimeIndex
        && !mOptions.TEST_mReadOnly)
    {
        // first open will recover rt index
        mOptions.GetOnlineConfig().enableRecoverIndex = true;
    }

    mPluginManager = IndexPluginLoader::Load(mIndexPluginPath,
            mSchema->GetIndexSchema(), mOptions);
    if (!mPluginManager)
    {
        IE_PREFIX_LOG(ERROR, "load index plugin failed for schema [%s]",
                      mSchema->GetSchemaName().c_str());
        return OS_FAIL;
    }
    // mDumpSegmentContainer should only exists for test

    mPartitionDataHolder.Reset(
            PartitionDataCreator::CreateBuildingPartitionData(
                    mFileSystem, mRtSchema, mOptions,
                    mPartitionMemController, mDumpSegmentContainer, 
                    onDiskVersion, mMetricProvider, "",
                    InMemorySegmentPtr(), mCounterMap, mPluginManager));

    PartitionMeta partitionMeta = mPartitionDataHolder.Get()->GetPartitionMeta();
    PluginResourcePtr pluginResource(
            new IndexPluginResource(mSchema, mOptions, mCounterMap, partitionMeta, mIndexPluginPath, mMetricProvider));
    mPluginManager->SetPluginResource(pluginResource);

    // only recover rt index once
    mOptions.GetOnlineConfig().enableRecoverIndex = false;

    if (mOptions.GetOnlineConfig().IsValidateIndexEnabled())
    {
        DeployIndexValidator::ValidateDeploySegments(
                mPartitionDataHolder.Get()->GetRootDirectory(), onDiskVersion);
    }
    
    CheckPartitionMeta(mSchema, partitionMeta);
    assert(!mSchema->GetVirtualAttributeSchema());
    assert(!mRtSchema->GetVirtualAttributeSchema());
    if (mOptions.GetOnlineConfig().enableLoadSpeedControlForOpen)
    {
        mFileSystem->EnableLoadSpeedSwitch();
    }
    else
    {
        mFileSystem->DisableLoadSpeedSwitch();
    }
    
    if (mOptions.GetOnlineConfig().disableSsePforDeltaOptimize)
    {
        EncoderProvider::GetInstance()->DisableSseOptimize();
    }
    
    if (!InitAccessCounters(mCounterMap, mSchema))
    {
        return OS_FAIL;
    }

    if (mOptions.GetOnlineConfig().loadRemainFlushRealtimeIndex
        && !mOptions.TEST_mReadOnly)
    {
        ReclaimRemainFlushRtIndex(mPartitionDataHolder.Get());
    }

    InitResourceCalculator();
    OpenExecutorPtr executor = CreateReopenPartitionReaderExecutor(false);
    ExecutorResource resource = CreateExecutorResource(onDiskVersion, false);
    if (!executor->Execute(resource)) {
        Close();
        return OS_LACK_OF_MEMORY;
    }

    MEMORY_BARRIER();
    SetReaderHashId(mReader ? mReader->GetPartitionVersionHashId() : 0);

    mFileSystem->EnableLoadSpeedSwitch();
    mReader->EnableAccessCountors();
    
    InitResourceCleaner();
    if (NeedCheckSecondIndex())
    {
        CheckSecondIndex();
    }
    if (NeedSubscribeSecondIndex())
    {
        SubscribeSecondIndex();
    }
    ReportMetrics();
    mLoadedIncVersion = mPartitionDataHolder.Get()->GetOnDiskVersion();

    mClosed = false;
    mIsServiceNormal = true;
    if (!mReaderUpdater)
    {
        mReaderUpdater.reset(new SearchReaderUpdater(mSchema->GetSchemaName()));
    }

    if (!mOptions.TEST_mReadOnly && !PrepareIntervalTask())
    {
        return OS_FAIL;
    }
    mNeedReload = false;
    IE_PREFIX_LOG(INFO, "open index partition end, version[%d]", targetVersionId);
    return OS_OK;
}

IndexlibFileSystemPtr OnlinePartition::CreateFileSystem(
        const string& primaryDir, const string& secondaryDir)
{
    if (mOptions.TEST_mReadOnly)
    {
        return IndexPartition::CreateFileSystem(primaryDir, secondaryDir);
    }

    IndexlibFileSystem::DeleteRootLinks(primaryDir);
    auto fileSystem = IndexPartition::CreateFileSystem(primaryDir, secondaryDir);
    ResetRtAndJoinDirPath(fileSystem);
    return fileSystem;
}

void OnlinePartition::InitPathMetaCache(const DirectoryPtr& deployMetaDir,
                                        const Version& version)
{
    if (!mOptions.GetBuildConfig(true).usePathMetaCache)
    {
        return;
    }
    IndexFileList deployMeta;
    const string& deployMetaFileName = DeployIndexWrapper::GetDeployMetaFileName(version.GetVersionId());
    if (!deployMeta.Load(deployMetaDir, deployMetaFileName))
    {
        return;
    }
    deployMeta.Sort();
    assert(deployMeta.finalDeployFileMetas.size() == 1);

    auto it = deployMeta.deployFileMetas.begin();
    while (it != deployMeta.deployFileMetas.end())
    {
        FileInfo& fileInfo = *it;
        const string& filePath = fileInfo.filePath;
        if (fileInfo.fileLength < 0 && filePath == deployMetaFileName)
        {
            it ++;
            continue;
        }
        if (filePath.find('/') == string::npos)
        {
            // eg. index_format_version, schema.json
            IE_PREFIX_LOG(INFO, "file meta cache add file [%s/%s]",
                          deployMetaDir->GetPath().c_str(), filePath.c_str());
            deployMetaDir->AddPathFileInfo(fileInfo);
            it ++;
            continue;
        }
        
        if (fileInfo.isDirectory() && filePath.find('/') == filePath.size() - 1)
        {
            // eg. segment_3_level_0/, truncate_meta/
            if (mFileSystem->MatchSolidPath(PathUtil::JoinPath(deployMetaDir->GetPath(), filePath)))
            {
                for (; it != deployMeta.deployFileMetas.end() &&
                         StringUtil::startsWith(it->filePath, filePath); it ++);
                continue;
            }

            IE_PREFIX_LOG(INFO, "file meta cache add file [%s/%s]",
                          deployMetaDir->GetPath().c_str(), filePath.c_str());
            deployMetaDir->AddPathFileInfo(fileInfo);

            auto innerIt = it + 1;
            for (; innerIt != deployMeta.deployFileMetas.end() &&
                     StringUtil::startsWith(innerIt->filePath, filePath); innerIt ++)
            {
                // eg. segment_3_level_0/segment_info, truncate_meta/price
                innerIt->filePath = innerIt->filePath.substr(filePath.size());
            }
            DirectoryPtr dir = deployMetaDir->GetDirectory(filePath, true);
            assert(dir);
            dir->AddSolidPathFileInfos(it + 1, innerIt);
            IE_PREFIX_LOG(INFO, "file meta cache add solid path [%s] with [%ld] inner files", dir->GetPath().c_str(), innerIt - it);
            it = innerIt;
            continue;
        }
        
        assert(false);
        IE_PREFIX_LOG(ERROR, "Unexpected file [%s] found in [%s]", filePath.c_str(),
                      DeployIndexWrapper::GetDeployMetaFileName(version.GetVersionId()).c_str());
        it ++;
    }
}

void OnlinePartition::RewriteSchemaAndOptions(const IndexPartitionSchemaPtr& schema,
        const Version& onDiskVersion)
{
    ScopedLock schemaLock(mSchemaLock);

    // rt build disable package file
    mOptions.GetBuildConfig(true).enablePackageFile = false;
    mOptions.SetNeedRewriteFieldType(false);
    mOptions.SetOngoingModifyOperationIds(onDiskVersion.GetOngoingModifyOperations());
    mSchema = SchemaAdapter::LoadAndRewritePartitionSchema(
            GetFileSystemRootDirectory(), mOptions, onDiskVersion.GetSchemaVersionId());

    CheckParam(mSchema, mOptions);

    if (schema)
    {
        mSchema->AssertCompatible(*schema);
    }
    mRtSchema = CreateRtSchema(mOptions);
}

bool OnlinePartition::PrepareIntervalTask()
{
    int32_t sleepTime = INDEXLIB_REPORT_METRICS_INTERVAL;
    if (unlikely(mOptions.TEST_mQuickExit))
    {
        sleepTime /= 1000;
    }

    if (mOptions.GetOnlineConfig().enableAsyncCleanResource)
    {
        TaskItemPtr cleanResourceTask(new OnlinePartitionTaskItem(
                        this, OnlinePartitionTaskItem::TT_CLEAN_RESOURCE));
        if (!mTaskScheduler->DeclareTaskGroup("clean_resource", sleepTime))
        {
            IE_PREFIX_LOG(ERROR, "declare clean resource task failed!");
            return false;
        }
        mCleanResourceTaskId = mTaskScheduler->AddTask("clean_resource", cleanResourceTask);
        if (mCleanResourceTaskId == TaskScheduler::INVALID_TASK_ID)
        {
            IE_PREFIX_LOG(ERROR, "add clean resource task failed!");
            return false;
        }
    }

    TaskItemPtr reportMetricsTask(new OnlinePartitionTaskItem(
                    this, OnlinePartitionTaskItem::TT_REPORT_METRICS));
    if (!mTaskScheduler->DeclareTaskGroup("report_metrics", sleepTime))
    {
        IE_PREFIX_LOG(ERROR, "declare report metrics task failed!");
        return false;
    }
    mReportMetricsTaskId = mTaskScheduler->AddTask("report_metrics", reportMetricsTask);
    if (mReportMetricsTaskId == TaskScheduler::INVALID_TASK_ID)
    {
        IE_PREFIX_LOG(ERROR, "add report metrics task failed!");
        return false;
    }

    TaskItemPtr intervalDumpTask(new OnlinePartitionTaskItem(
                    this, OnlinePartitionTaskItem::TT_INTERVAL_DUMP));
    if (!mTaskScheduler->DeclareTaskGroup("interval_dump", sleepTime))
    {
        IE_PREFIX_LOG(ERROR, "declare interval_dump task failed!");
        return false;
    }
    mIntervalDumpTaskId = mTaskScheduler->AddTask("interval_dump", intervalDumpTask);
    if (mIntervalDumpTaskId == TaskScheduler::INVALID_TASK_ID)
    {
        IE_PREFIX_LOG(ERROR, "add interval_dump task failed!");
        return false;
    }

    if (mOptions.GetOnlineConfig().enableAsyncDumpSegment)
    {
        mAsyncSegmentDumper.reset(new AsyncSegmentDumper(this, mCleanerLock));
        TaskItemPtr asyncDumpTask(new OnlinePartitionTaskItem(
                        this, OnlinePartitionTaskItem::TT_TRIGGER_ASYNC_DUMP));
        if (!mTaskScheduler->DeclareTaskGroup("async_dump", sleepTime))
        {
            IE_PREFIX_LOG(ERROR, "declare async_dump task failed!");
            return false;
        }
        mAsyncDumpTaskId = mTaskScheduler->AddTask("async_dump", asyncDumpTask);
        if (mAsyncDumpTaskId == TaskScheduler::INVALID_TASK_ID)
        {
            IE_PREFIX_LOG(ERROR, "add async_dump task failed!");
            return false;
        }
    }

    if (NeedCheckSecondIndex())
    {
        TaskItemPtr checkSecondaryIndexTask(new OnlinePartitionTaskItem(
                this, OnlinePartitionTaskItem::TT_CHECK_SECONDARY_INDEX));
        if (!mTaskScheduler->DeclareTaskGroup("check_index",
                mCheckSecondIndexIntervalInMin * 60 * 1000 * 1000))
        {
            IE_PREFIX_LOG(ERROR, "declare check_index task failed!");
            return false;
        }
        mCheckIndexTaskId = mTaskScheduler->AddTask("check_index", checkSecondaryIndexTask);
        if (mCheckIndexTaskId == TaskScheduler::INVALID_TASK_ID)
        {
            IE_PREFIX_LOG(ERROR, "add check_index task failed!");
            return false;
        }
    }
    
    if (NeedSubscribeSecondIndex())
    {
        TaskItemPtr subscribeSecondIndexTask(new OnlinePartitionTaskItem(
                this, OnlinePartitionTaskItem::TT_SUBSCRIBE_SECONDARY_INDEX));
        if (!mTaskScheduler->DeclareTaskGroup("subscribe_index",
                mSubscribeSecondIndexIntervalInMin * 60 * 1000 * 1000))
        {
            IE_PREFIX_LOG(ERROR, "declare subscribe_index task failed!");
            return false;
        }
        mSubscribeIndexTaskId = mTaskScheduler->AddTask("subscribe_index", subscribeSecondIndexTask);
        if (mSubscribeIndexTaskId == TaskScheduler::INVALID_TASK_ID)
        {
            IE_PREFIX_LOG(ERROR, "add subscribe_index task failed!");
            return false;
        }
    }
    return true;
}

void OnlinePartition::CheckParam(const IndexPartitionSchemaPtr& schema,
                                 const IndexPartitionOptions& options) const
{
    if (!schema)
    {
        INDEXLIB_FATAL_ERROR(BadParameter,
                             "[%s] no schema in index",
                             IE_LOG_PREFIX);
    }

    if (!options.IsOnline())
    {
        INDEXLIB_FATAL_ERROR(BadParameter,
                             "[%s] OnlinePartition only support online option!",
                             IE_LOG_PREFIX);
    }

    options.Check();
    schema->Check();

    // check maxReopenMemoryUse with memoryUseLimit
    int64_t maxReopenMemoryUse;
    int64_t totalMemUse = mPartitionMemController->GetFreeQuota() / (1024 * 1024);
    if (options.GetOnlineConfig().GetMaxReopenMemoryUse(maxReopenMemoryUse)
        && maxReopenMemoryUse >= totalMemUse)
    {
        INDEXLIB_FATAL_ERROR(BadParameter,
                             "[%s] maxReopenMemoryUse [%ld]MB should less than current free memory [%ld]MB!",
                             IE_LOG_PREFIX, maxReopenMemoryUse, totalMemUse);
    }
    
    if (options.GetOnlineConfig().loadRemainFlushRealtimeIndex &&
        (schema->GetTableType() != tt_kv && schema->GetTableType() != tt_kkv))
    {
        INDEXLIB_FATAL_ERROR(BadParameter,
                             "[%s] only kv or kkv table support load_remain_flush_realtime_index = true!",
                             IE_LOG_PREFIX);
    }
}

void OnlinePartition::InitReader(const PartitionDataPtr& partData)
{
    Version version = partData->GetVersion();
    IE_PREFIX_LOG(INFO, "begin open index partition reader, version[%d]", 
                  version.GetVersionId());

    MemoryReserverPtr memReserver = mFileSystem->CreateMemoryReserver("switch_flush_rt_segment");
    segmentid_t lastValidFlushRtSegId = GetLatestLinkRtSegId(partData, memReserver);
    DoInitReader(partData, lastValidFlushRtSegId);
    
    IE_PREFIX_LOG(INFO, "end open index partition reader, version[%d]",
                  version.GetVersionId());
}

void OnlinePartition::DoInitReader(const PartitionDataPtr& partData,
                                   segmentid_t flushRtSegmentId)
{
    OnlinePartitionReaderPtr reader(
        new partition::OnlinePartitionReader(mOptions, mSchema, mSearchCache,
                                             mOnlinePartMetrics.get(), flushRtSegmentId,
                                             mPartitionName, mLatestNeedSkipIncTs));

    PartitionDataPtr clonedPartitionData(partData->Clone());

    AttributeMetrics* attributeMetrics = mOnlinePartMetrics->GetAttributeMetrics();
    assert(attributeMetrics);
    attributeMetrics->ResetMetricsForNewReader();

    reader->Open(clonedPartitionData);

    LoadReaderPatch(clonedPartitionData, reader);

    SwitchReader(reader);
}

void OnlinePartition::InitWriter(const PartitionDataPtr& partData)
{
    assert(partData->GetInMemorySegment());
    assert(!mWriter);
    mWriter.reset(new OnlinePartitionWriter(mOptions, mDumpSegmentContainer, 
                    mOnlinePartMetrics.get(), mPartitionName));
    mResourceCalculator->SetWriter(mWriter);
    PartitionModifierPtr modifier = CreateReaderModifier(mReader);
    mWriter->Open(mRtSchema, partData, modifier);
}

void OnlinePartition::InitResourceCleaner()
{
    mResourceCleaner.reset(new ExecutorScheduler);
    ExecutorPtr readerCleaner(new PartitionReaderCleaner(mReaderContainer,
                    mFileSystem, mDataLock, mPartitionName));
    mResourceCleaner->Add(readerCleaner, ExecutorScheduler::ST_REPEATEDLY);

    // inMemorySegCleaner should be after readerCleaner : reader will release inMemorySegment 
    ExecutorPtr inMemorySegCleaner(new InMemorySegmentCleaner(mDumpSegmentContainer->GetInMemSegmentContainer()));
    mResourceCleaner->Add(inMemorySegCleaner, ExecutorScheduler::ST_REPEATEDLY);

    // inMemoryIndexCleaner should after readerCleaner & inMemorySegCleaner
    // kv table will hold key file in inMemorySegment
    ExecutorPtr inMemoryIndexCleaner(new InMemoryIndexCleaner(mReaderContainer,
                    mPartitionDataHolder.Get()->GetRootDirectory()));
    mResourceCleaner->Add(inMemoryIndexCleaner, ExecutorScheduler::ST_REPEATEDLY);
    AddOnDiskIndexCleaner();
    CleanResource();
}

void OnlinePartition::InitResourceCalculator()
{
    mResourceCalculator = CreateResourceCalculator(
            mOptions, mPartitionDataHolder.Get(), mWriter, mPluginManager);
}

PartitionResourceCalculatorPtr OnlinePartition::CreateResourceCalculator(
        const IndexPartitionOptions& options,
        const PartitionDataPtr& partitionData,
        const PartitionWriterPtr& writer,
        const plugin::PluginManagerPtr& pluginManager) const
{
    PartitionResourceCalculatorPtr calculator(
            new PartitionResourceCalculator(options.GetOnlineConfig()));
    calculator->Init(partitionData->GetRootDirectory(), writer,
                     mDumpSegmentContainer->GetInMemSegmentContainer(),
                     pluginManager);
    return calculator;
}

IndexPartition::OpenStatus OnlinePartition::SwitchFlushRtSegments()
{
    IndexPhaseGuard phaseGuard(mIndexPhase, SWITCH_FLUSH_RT);
    ScopedLock lock(mDataLock);

    misc::ScopeLatencyReporter scopeTime(
            mOnlinePartMetrics->GetSwitchFlushRtSegmentLatencyMetric().get());

    MemoryReserverPtr memReserver = mFileSystem->CreateMemoryReserver("switch_flush_rt_segment");
    segmentid_t lastValidFlushRtSegId = GetLatestLinkRtSegId(mPartitionDataHolder.Get(), memReserver);
    assert(mReader);
    segmentid_t lastReaderFlushRtSegId = mReader->GetPartitionVersion().GetLastLinkRtSegmentId();

    if (lastValidFlushRtSegId <= lastReaderFlushRtSegId)
    {
        IE_PREFIX_LOG(WARN, "skip switch reader for flushed rt segments!");
        return OS_OK;
    }

    IE_PREFIX_LOG(INFO, "switch reader for flushed rt segments begin");
    DoInitReader(mPartitionDataHolder.Get(), lastValidFlushRtSegId);
    PartitionModifierPtr modifier = CreateReaderModifier(mReader);
    if (mWriter)
    {
        mWriter->ReOpenNewSegment(modifier);
    }

    mReader->EnableAccessCountors();
    ReportMetrics();
    IE_PREFIX_LOG(INFO, "switch reader for flushed rt segments end");
    return OS_OK;
}

IndexPartition::OpenStatus OnlinePartition::ReclaimReaderMem()
{
    IndexPhaseGuard phaseGuard(mIndexPhase, RECLAIM_READER_MEM);
    ScopedLock lock(mDataLock);

    misc::ScopeLatencyReporter scopeTime(
            mOnlinePartMetrics->GetReclaimReaderMemLatencyMetric().get());

    IE_PREFIX_LOG(INFO, "reclaim reader memory begin");

    PartitionDataPtr partData = mPartitionDataHolder.Get();
    // TODO: skip dump, just open reader
    if (mWriter)
    {
        mWriter->DumpSegment();
        partData->CreateNewSegment();
    }

    InitReader(partData);
    PartitionModifierPtr modifier = CreateReaderModifier(mReader);
    if (mWriter)
    {
        mWriter->ReOpenNewSegment(modifier);
    }

    mReader->EnableAccessCountors();
    ReportMetrics();
    IE_PREFIX_LOG(INFO, "reclaim reader memory end");
    return OS_OK;
}

IndexPartition::OpenStatus OnlinePartition::ReOpen(
        bool forceReopen, versionid_t targetVersionId)
{
    int64_t beginTime = TimeUtility::currentTime();
    IE_PREFIX_LOG(INFO, "reopen partition begin, force[%d], verison[%d]", forceReopen, targetVersionId);
    OpenStatus os;
    try
    {
        if (mClosed)
        {
            IE_PREFIX_LOG(WARN, "partition is closed. try open instead");
            os = DoOpen(mOpenIndexPrimaryDir, mOpenIndexSecondaryDir,
                        mOpenSchema, mOpenOptions, targetVersionId, FORCE_OPEN);
            IE_PREFIX_LOG(INFO, "reopen partition end, verison[%d], os[%d], used[%.3f]s",
                          targetVersionId, os, (TimeUtility::currentTime() - beginTime) / 1000000.0);
            return os;
        }
        // scopelock will forbidden cleanResource when process reopen
        ScopedLock lock(mCleanerLock);
        CleanResource();

        os = DoReopen(forceReopen, targetVersionId);
        IE_PREFIX_LOG(INFO, "reopen partition end, verison[%d], os[%d], used[%.3f]s",
                      targetVersionId, os, (TimeUtility::currentTime() - beginTime) / 1000000.0);
        return os;
    }
    catch (const FileIOException& e)
    {
        IE_PREFIX_LOG(ERROR, "reopen caught file io exception: %s", e.what());
        os = OS_FILEIO_EXCEPTION;
    }
    catch (const ExceptionBase& e)
    {
        IE_PREFIX_LOG(ERROR, "reopen caught exception: %s", e.what());
        os = OS_INDEXLIB_EXCEPTION;
    }
    catch (const exception& e)
    {
        IE_PREFIX_LOG(ERROR, "reopen caught std exception: %s", e.what());
        os = OS_UNKNOWN_EXCEPTION;
    }
    catch (...)
    {
        IE_PREFIX_LOG(ERROR, "reopen caught unknown exception");
        os = OS_UNKNOWN_EXCEPTION;
    }
    IE_PREFIX_LOG(INFO, "reopen partition end, verison[%d], os[%d], used[%.3f]s",
                  targetVersionId, os, (TimeUtility::currentTime() - beginTime) / 1000000.0);
    return os;
}

JoinSegmentWriterPtr OnlinePartition::CreateJoinSegmentWriter(
    const Version& onDiskVersion, bool isForceReopen) const
{
    misc::ScopeLatencyReporter scopeTime(
            mOnlinePartMetrics->GetcreateJoinSegmentWriterLatencyMetric().get());

    bool enableRedoSpeedup =
        mOptions.GetOnlineConfig().IsRedoSpeedupEnabled() && !isForceReopen;

    IE_LOG(INFO, "enableRedoSpeedup flag = [%d] on version[%d] with isForceReopen = [%d]",
           enableRedoSpeedup, onDiskVersion.GetVersionId(), isForceReopen);

    IndexPartitionOptions options = mOptions;
    options.GetOnlineConfig().SetEnableRedoSpeedup(enableRedoSpeedup);
    
    JoinSegmentWriterPtr joinWriter(new JoinSegmentWriter(
                    mSchema, options, mFileSystem, mPartitionMemController));
    joinWriter->Init(mPartitionDataHolder.Get(), onDiskVersion, mLoadedIncVersion);
    return joinWriter;
}

IndexPartition::OpenStatus OnlinePartition::ForceReopen(
        Version& onDiskVersion)
{
    IndexPhaseGuard phaseGuard(mIndexPhase, FORCE_REOPEN);
    IE_PREFIX_LOG(INFO, "force reopen begin, version[%d]", onDiskVersion.GetVersionId());

    mFileSystem->DisableLoadSpeedSwitch();

    OpenExecutorChainPtr chain = CreateForceReopenExecutorChain(onDiskVersion);
    ExecutorResource&& resource = CreateExecutorResource(onDiskVersion, true);
    if (!chain->Execute(resource))
    {
        IE_PREFIX_LOG(INFO, "force reopen fail, version[%d]",
                      onDiskVersion.GetVersionId());
        mIsServiceNormal = false;
        return OS_FAIL;
    }
    mLatestNeedSkipIncTs = resource.mLatestNeedSkipIncTs;
    
    MEMORY_BARRIER();
    SetReaderHashId(mReader ? mReader->GetPartitionVersionHashId() : 0);

    ReportMetrics();
    AddOnDiskIndexCleaner();
    IE_PREFIX_LOG(INFO, "force reopen success, version[%d]",
                  onDiskVersion.GetVersionId());
    mFileSystem->EnableLoadSpeedSwitch();
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
        SetReaderHashId(mReader ? mReader->GetPartitionVersionHashId() : 0);
    }
    mReaderContainer->AddReader(reader);
    mResourceCalculator->CalculateCurrentIndexSize(mPartitionDataHolder.Get(), mSchema);
}

void OnlinePartition::AddOnDiskIndexCleaner()
{
    // TODO: In future, suez will in controll of local index clean
    char* envStr = getenv("INDEXLIB_ONLINE_CLEAN_ON_DISK_INDEX");
    if (envStr && std::string(envStr) == "false")
    {
        return;
    }
    if (!getenv("INDEXLIB_ONLINE_NOT_AUTO_DISABLE_CLEAN_INDEX"))
    {
        if (!mOptions.GetOnlineConfig().NeedDeployIndex())
        {
            IE_PREFIX_LOG(INFO, "do not need clean on disk index when NeedDeployIndex=false");
            return;
        }
    }
    ExecutorPtr onDiskIndexCleaner(new OnDiskIndexCleaner(
                    mOptions.GetOnlineConfig().onlineKeepVersionCount,
                    mReaderContainer, GetRootDirectory()));
    mResourceCleaner->Add(onDiskIndexCleaner, ExecutorScheduler::ST_ONCE);
}

void OnlinePartition::AddVirtualAttributeDataCleaner(
        const IndexPartitionSchemaPtr &newSchema)
{
    vector<pair<string, bool>> attrNames;
    CollectVirtualAttributes(newSchema->GetVirtualAttributeSchema(),
                             attrNames, false);

    const IndexPartitionSchemaPtr &newSubSchema = newSchema->GetSubIndexPartitionSchema();
    if (newSubSchema)
    {
        CollectVirtualAttributes(newSubSchema->GetVirtualAttributeSchema(),
                attrNames, true);
    }
    if (!mVirtualAttrContainer)
    {
        mVirtualAttrContainer.reset(new VirtualAttributeContainer);
        ExecutorPtr cleaner(new InMemVirtualAttributeCleaner(
                        mReaderContainer, mVirtualAttrContainer,
                        GetRootDirectory()));
        mResourceCleaner->Add(cleaner, ExecutorScheduler::ST_REPEATEDLY);
    }
    mVirtualAttrContainer->UpdateUsingAttributes(attrNames);

}

void OnlinePartition::CollectVirtualAttributes(
        const AttributeSchemaPtr& newSchema,
        vector<pair<string, bool>>& attrNames,
        bool isSub)
{
    if (!newSchema)
    {
        return;
    }
    AttributeSchema::Iterator iter = newSchema->Begin();
    for (; iter != newSchema->End(); iter++)
    {
        const string& attrName = (*iter)->GetAttrName();
        attrNames.emplace_back(attrName, isSub);
    }
}

ReopenDecider::ReopenType OnlinePartition::MakeReopenDecision(
        bool forceReopen, versionid_t targetVersionId, Version& onDiskVersion)
{
    misc::ScopeLatencyReporter scopeTime(
            mOnlinePartMetrics->GetdecisionLatencyMetric().get());
    ReopenDecider reopenDecider(mOptions.GetOnlineConfig(),
                                forceReopen);
    reopenDecider.Init(mPartitionDataHolder.Get(), 
                       mOnlinePartMetrics->GetAttributeMetrics(),
                       targetVersionId, mReader);
    onDiskVersion = reopenDecider.GetReopenIncVersion();
    return reopenDecider.GetReopenType();
}

IndexPartition::OpenStatus OnlinePartition::DoReopen(
        bool forceReopen, versionid_t targetVersionId)
{
    ScopedLockPtr scopedLock(new ScopedLock(mDataLock));
    IE_PREFIX_LOG(DEBUG, "reopen begin");
    // lambda for scope
    SetEnableReleaseOperationAfterDump(false);
    shared_ptr<OnlinePartition> selfPartition(this,
            [](OnlinePartition* p) { p->SetEnableReleaseOperationAfterDump(true); });

    misc::ScopeLatencyReporter scopeTime(
            mOnlinePartMetrics->GetreopenIncLatencyMetric().get());

    ReportMetrics();

    Version onDiskVersion;
    ReopenDecider::ReopenType reopenType = MakeReopenDecision(
            forceReopen, targetVersionId, onDiskVersion);
    OpenStatus ret = OS_FAIL;
    switch (reopenType)
    {
    case ReopenDecider::NORMAL_REOPEN:
        InitPathMetaCache(GetFileSystemRootDirectory(), onDiskVersion);
        ret = NormalReopen(scopedLock, onDiskVersion);
        break;
    case ReopenDecider::FORCE_REOPEN:
    {
        InitPathMetaCache(GetFileSystemRootDirectory(), onDiskVersion);
        ret = ForceReopen(onDiskVersion);
        if ((mOptions.GetOnlineConfig().enableForceOpen == false) || (ret == OS_OK))
        {
            break;
        }
        IE_PREFIX_LOG(ERROR, "force reopen failed, version[%d]; "
               "abandon realtime data and switch to force *OPEN*", targetVersionId);
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

    if (ret == OS_OK && mReaderUpdater)
    {
        if (!mReaderUpdater->Update(mReader))
        {
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
    if (mWriter)
    {
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
    if (!mResourceCalculator)
    {
        return;
    }

    size_t builtRtIndexMemoryUse = mResourceCalculator->GetRtIndexMemoryUse();
    size_t writerMemoryUse = mResourceCalculator->GetWriterMemoryUse();
    size_t dumpingMemoryUse = mDumpSegmentContainer->GetDumpingSegmentsSize();
    size_t rtIndexMemoryUse = builtRtIndexMemoryUse + writerMemoryUse + dumpingMemoryUse;

    if (mRtMemQuotaSynchronizer)
    {
        mRtMemQuotaSynchronizer->SyncMemoryQuota(rtIndexMemoryUse);
    }

    IndexPartition::MemoryStatus memoryStatus = CheckMemoryStatus();
    mOnlinePartMetrics->SetdumpingSegmentCountValue((int64_t)mDumpSegmentContainer->GetDumpItemSize());
    mOnlinePartMetrics->SetmemoryStatusValue((int64_t)memoryStatus);
    mOnlinePartMetrics->SetindexPhaseValue((int64_t)mIndexPhase);
    mOnlinePartMetrics->SetpartitionIndexSizeValue(
            mResourceCalculator->GetCurrentIndexSize());
    mOnlinePartMetrics->SetpartitionMemoryUseValue(
            mResourceCalculator->GetCurrentMemoryUse());
    mOnlinePartMetrics->SetincIndexMemoryUseValue(
            mResourceCalculator->GetIncIndexMemoryUse());
    mOnlinePartMetrics->SetbuiltRtIndexMemoryUseValue(builtRtIndexMemoryUse);
    mOnlinePartMetrics->SetbuildingSegmentMemoryUseValue(writerMemoryUse);
    mOnlinePartMetrics->SetrtIndexMemoryUseValue(rtIndexMemoryUse);
    const OnlineConfig& onlineConfig = mOptions.GetOnlineConfig();
    int64_t maxRtIndexSizeInMB = onlineConfig.maxRealtimeMemSize;
    double rtIndexMemoryRatio =
        (double)rtIndexMemoryUse / (maxRtIndexSizeInMB * 1024 * 1024);
    mOnlinePartMetrics->SetrtIndexMemoryUseRatioValue(rtIndexMemoryRatio);
    mOnlinePartMetrics->SetoldInMemorySegmentMemoryUseValue(
            mDumpSegmentContainer->GetInMemSegmentContainer()->GetTotalMemoryUse());
    mOnlinePartMetrics->SetpartitionMemoryQuotaUseValue(
            mPartitionMemController->GetUsedQuota());
    mOnlinePartMetrics->SetmissingSegmentCountValue(mMissingSegmentCount.load());

    // update ReaderContainer related metrics
    versionid_t oldestReaderVersion = mReaderContainer->GetOldestReaderVersion();
    versionid_t latestReaderVersion = mReaderContainer->GetLatestReaderVersion();
    int64_t readerCount = mReaderContainer->Size();

    mOnlinePartMetrics->SetpartitionReaderVersionCountValue(readerCount);
    mOnlinePartMetrics->SetoldestReaderVersionIdValue(oldestReaderVersion);
    mOnlinePartMetrics->SetlatestReaderVersionIdValue(latestReaderVersion);
}

void OnlinePartition::LoadReaderPatch(
        const PartitionDataPtr& partitionData, OnlinePartitionReaderPtr& reader)
{
    if (mSchema->GetTableType() == tt_kv || mSchema->GetTableType() == tt_kkv)
    {
        return;
    }
    misc::ScopeLatencyReporter scopeTime(
            mOnlinePartMetrics->GetLoadReaderPatchLatencyMetric().get());
    IE_PREFIX_LOG(INFO, "load reader patch begin");
    
    PartitionModifierPtr modifier = CreateReaderModifier(reader);
    if (modifier)
    {
        bool loadPatchForOpen = false;
        if (mIndexPhase == IndexPartition::OPENING
            || mIndexPhase == IndexPartition::FORCE_OPEN
            || mIndexPhase == IndexPartition::FORCE_REOPEN)
        {
            loadPatchForOpen = true;
        }        
        
        PatchLoader patchLoader(mRtSchema, mOptions.GetOnlineConfig());
        segmentid_t startLoadSegment = mLoadedIncVersion.GetLastSegment() + 1;
        bool isIncConsistentWithRt = (mLoadedIncVersion != INVALID_VERSION)
            && mOptions.GetOnlineConfig().isIncConsistentWithRealtime;

        Version onDiskIncVersion = partitionData->GetOnDiskVersion();
        string locatorStr = partitionData->GetLastLocator();
        IndexLocator indexLocator;
        if (!indexLocator.fromString(locatorStr))
        {
            isIncConsistentWithRt = false;
        }
        else
        {
            bool fromInc = false;
            OnlineJoinPolicy joinPolicy(onDiskIncVersion, mSchema->GetTableType());
            joinPolicy.GetRtSeekTimestamp(indexLocator, fromInc);
            if (fromInc)
            {
                isIncConsistentWithRt =  false;
            }
        }
        IE_PREFIX_LOG(INFO, "load reader patch with isIncConsistentWithRt[%d", isIncConsistentWithRt);
        patchLoader.Init(partitionData, isIncConsistentWithRt,
                         mLoadedIncVersion, startLoadSegment, loadPatchForOpen);
        patchLoader.Load(mLoadedIncVersion, modifier);
    }
    IE_PREFIX_LOG(INFO, "load reader patch end");
}

void OnlinePartition::ReOpenNewSegment()
{
    IndexPhaseGuard phaseGuard(mIndexPhase, REOPEN_RT);
    ScopedLock lock(mDataLock);

    PartitionDataPtr partData = mPartitionDataHolder.Get();
    IE_PREFIX_LOG(INFO, "reopen new segment begin");
    misc::ScopeLatencyReporter scopeTime(
            mOnlinePartMetrics->GetreopenRealtimeLatencyMetric().get());
    partData->CreateNewSegment();
    segmentid_t lastValidLinkRtSegId = partData->GetLastValidRtSegmentInLinkDirectory();
    InMemorySegmentPtr inMemSeg = partData->GetInMemorySegment();
    segmentid_t inMemSegmentId = inMemSeg ? inMemSeg->GetSegmentId() : INVALID_SEGMENTID;
    PartitionVersion partitionVersion(
            partData->GetVersion(), inMemSegmentId, lastValidLinkRtSegId);
    if (partitionVersion.GetHashId() == mReader->GetPartitionVersionHashId() && mWriter)
    {
        return;
    }
    InitReader(partData);

    if (!mWriter)
    {
        InitWriter(partData);
    }
    else
    {
        PartitionModifierPtr modifier = CreateReaderModifier(mReader);
        mWriter->ReOpenNewSegment(modifier);
    }

    mReader->EnableAccessCountors();
    ReportMetrics();
    if (mReaderUpdater)
    {
        mReaderUpdater->Update(mReader);
    }

    IE_PREFIX_LOG(INFO, "reopen new segment end");
}

void OnlinePartition::Close()
{
    if (mClosed)
    {
        IE_PREFIX_LOG(INFO, "close index partition repeatedly, version[%d]", mLoadedIncVersion.GetVersionId());
        return;
    }

    try 
    {
        DoClose();
    }
    catch(const ExceptionBase& e)
    {
        throw;
    }
}

void OnlinePartition::DoClose()
{
    IE_PREFIX_LOG(INFO, "close index partition begin, version[%d]", mLoadedIncVersion.GetVersionId());
    mClosed = true;
    mIsServiceNormal = false;
    // should not lock mDataLock, avoid dead lock
    mTaskScheduler->DeleteTask(mCleanResourceTaskId);
    mTaskScheduler->DeleteTask(mReportMetricsTaskId);
    mTaskScheduler->DeleteTask(mIntervalDumpTaskId);
    mTaskScheduler->DeleteTask(mAsyncDumpTaskId);
    mTaskScheduler->DeleteTask(mCheckIndexTaskId);
    mTaskScheduler->DeleteTask(mSubscribeIndexTaskId);
    
    ScopedLock lock(mDataLock);

    while (mReaderContainer->Size() > 1)
    {
        auto unReleasedReader = mReaderContainer->PopOldestReader();
        if (unReleasedReader && unReleasedReader.use_count() > 1)
        {
            IE_PREFIX_LOG(ERROR, "unreleased reader [version: %d] exists"
                          " when IndexPartition is to be closed",
                          unReleasedReader->GetVersion().GetVersionId());
        }
    }
    
    mReaderContainer->Close();
    {
        autil::ScopedLock lock(mReaderLock);
        if (mReader.use_count() > 1)
        {
            INDEXLIB_FATAL_ERROR(InconsistentState,
                    "[%s] IndexPartitionReader is still "
                    "used when the IndexPartition is to be closed",
                    IE_LOG_PREFIX);
        }
        mReader.reset();
    }
    if (mFileSystem)
    {
        mFileSystem->CleanCache();
    }
    if (mWriter)
    {
        mWriter->Close();
    }
    mWriter.reset();
    mAsyncSegmentDumper.reset();
    FlushDumpSegmentInContainer();
    while (mDumpSegmentContainer->GetInMemSegmentContainer()->Size() > 0)
    {
        mDumpSegmentContainer->GetInMemSegmentContainer()->EvictOldestInMemSegment();
    }
    mDumpSegmentContainer.reset();
    mPartitionDataHolder.Reset();
    if (mFileSystem)
    {
        mFileSystem->Sync(true);
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
    //ScopedLock lock(mDataLock);
    OnlineJoinPolicy joinPolicy(mLoadedIncVersion, mSchema->GetTableType());
    return joinPolicy.GetRtSeekTimestampFromIncVersion();
}

IndexPartition::MemoryStatus OnlinePartition::CheckMemoryStatus() const
{
    if (!mResourceCalculator)
    {
        return MS_OK;
    }

    const OnlineConfig& onlineConfig = mOptions.GetOnlineConfig();
    //for reach max rt index memory use
    int64_t rtIndexMemUseInMB = (mResourceCalculator->GetRtIndexMemoryUse() +
                                 mDumpSegmentContainer->GetDumpingSegmentsSize() +
                                 mResourceCalculator->GetWriterMemoryUse()) / 
                                (1024 * 1024);

    int64_t maxRtIndexSizeInMB = onlineConfig.maxRealtimeMemSize;
    if (rtIndexMemUseInMB >= maxRtIndexSizeInMB)
    {
        IE_PREFIX_LOG(WARN, "reach max Rt index size[%ld MB], "
                      "current rtIndexSize is %ld MB", 
                      maxRtIndexSizeInMB, rtIndexMemUseInMB);
        return MS_REACH_MAX_RT_INDEX_SIZE;
    }

    if (mRtMemQuotaSynchronizer && mRtMemQuotaSynchronizer->GetFreeQuota() <= 0)
    {
        IE_PREFIX_LOG(WARN, "reach realtime quota control limit, current free quota[%lu]",
                      mRtMemQuotaSynchronizer->GetFreeQuota());
        return MS_REACH_MAX_RT_INDEX_SIZE;
    }

    //TODO: when reopen, control building mem not exceed BUILD_RESERVE_MEM

    //for free mem not enough for max reopen size
    int64_t availableMemoryUse = mPartitionMemController->GetFreeQuota() / (1024 * 1024);
    int64_t maxReopenMemoryUse;
    if (!onlineConfig.GetMaxReopenMemoryUse(maxReopenMemoryUse))
    {
        maxReopenMemoryUse = 0;
    }

    uint64_t buildingSegmentDumpExpandMemUse =
        mResourceCalculator->GetBuildingSegmentDumpExpandSize() / (1024 * 1024);
    uint64_t dumpingSegmentDumpExpandMemUse =
        mDumpSegmentContainer->GetMaxDumpingSegmentExpandMemUse() / (1024 * 1024);
    uint64_t dumpExpandMemUse = max(buildingSegmentDumpExpandMemUse, dumpingSegmentDumpExpandMemUse);
    if (availableMemoryUse - maxReopenMemoryUse <= (int64_t)dumpExpandMemUse)
    {
        int64_t currentMemUse =
            mResourceCalculator->GetCurrentMemoryUse() / (1024 * 1024);
        IE_PREFIX_LOG(WARN, "reach memory use limit free mem [%ld MB], "
                      "current total memory is %ld MB, maxReopenMemoryUse [%ld MB] "
                      "dumpExpandMemUse [%lu MB]", 
                      availableMemoryUse, currentMemUse,
                      maxReopenMemoryUse, dumpExpandMemUse);
        return MS_REACH_TOTAL_MEM_LIMIT;
    }
    return MS_OK;
}

util::MemoryReserverPtr OnlinePartition::ReserveVirtualAttributesResource(
        const config::AttributeConfigVector& mainVirtualAttrConfigs,
        const config::AttributeConfigVector& subVirtualAttrConfigs)
{
    ScopedLock lock(mDataLock);
    PartitionInfoPtr partitionInfo = mReader->GetPartitionInfo();
    size_t docCount = partitionInfo->GetTotalDocCount();
    AttributeSchemaPtr virtualAttrSchema =
        mRtSchema->GetVirtualAttributeSchema();
    size_t expandSize = EstimateAttributeExpandSize(
            virtualAttrSchema, mainVirtualAttrConfigs, docCount);
    if (subVirtualAttrConfigs.size() != 0)
    {
        PartitionInfoPtr subPartitionInfo = partitionInfo->GetSubPartitionInfo();
        size_t subDocCount = subPartitionInfo->GetTotalDocCount();
        AttributeSchemaPtr subVirtualAttrSchema =
            mRtSchema->GetSubIndexPartitionSchema()->GetVirtualAttributeSchema();
        expandSize += EstimateAttributeExpandSize(
            subVirtualAttrSchema, subVirtualAttrConfigs, subDocCount);
    }
    MemoryReserverPtr memReserver = mFileSystem->CreateMemoryReserver("virtual_attributes");
    if (memReserver->Reserve(expandSize))
    {
        return memReserver;
    }

    return MemoryReserverPtr();
}

size_t OnlinePartition::EstimateAttributeExpandSize(const AttributeSchemaPtr& virtualAttrSchema,
        const config::AttributeConfigVector& attributeConfigs, size_t docCount)
{
    size_t expandSize = 0;
    for (size_t i = 0; i < attributeConfigs.size(); i++)
    {
        if (virtualAttrSchema && virtualAttrSchema->IsInAttribute(attributeConfigs[i]->GetAttrName()))
        {
            continue;
        }
        expandSize += attributeConfigs[i]->EstimateAttributeExpandSize(docCount);
    }
    return expandSize;
}

void OnlinePartition::AddVirtualAttributes(
    const AttributeConfigVector& mainAttrConfigs,
    const AttributeConfigVector& subAttrConfigs)
{
    ScopedLock lock(mDataLock);    
    AttributeConfigVector mainVirtualAttrConfigs = mainAttrConfigs;
    AttributeConfigVector subVirtualAttrConfigs = subAttrConfigs;
    mReaderUpdater->FillVirtualAttributeConfigs(mainVirtualAttrConfigs, subVirtualAttrConfigs);
    IndexPartitionSchemaPtr newSchema = CreateNewSchema(
            mSchema, mainVirtualAttrConfigs, subVirtualAttrConfigs);
    if (!newSchema)
    {
        return;
    }

    AddVirtualAttributeDataCleaner(newSchema);

    IndexPartitionSchemaPtr newRtSchema = CreateRtSchema(mOptions);
    newRtSchema->CloneVirtualAttributes(*newSchema);

    if (mWriter)
    {
        mWriter->DumpSegment();
    }

    {
        ScopedLock schemaLock(mSchemaLock);
        mSchema = newSchema;
        mRtSchema = newRtSchema;
    }
    OpenExecutorPtr executor = CreateReopenPartitionReaderExecutor(true);
    ExecutorResource resource = CreateExecutorResource(mLoadedIncVersion, false);

    // Schema changed, create new inMemorySegment
    resource.mNeedInheritInMemorySegment = false;
    executor->Execute(resource);
    ReportMetrics();
}

bool OnlinePartition::CleanIndexFiles(const vector<versionid_t>& keepVersionIds)
{
    if (!mLocalIndexCleaner)
    {
        mLocalIndexCleaner.reset(new LocalIndexCleaner(
                                     GetFileSystemRootDirectory(),
                                     mOptions.GetOnlineConfig().onlineKeepVersionCount,
                                     mReaderContainer));
    }
    return mLocalIndexCleaner->Clean(keepVersionIds);
}

IndexPartitionSchemaPtr OnlinePartition::CreateNewSchema(
        const IndexPartitionSchemaPtr& schema,
        const AttributeConfigVector& mainVirtualAttrConfigs,
        const AttributeConfigVector& subVirtualAttrConfigs)
{
    IndexPartitionSchemaPtr newSchema = 
        SchemaAdapter::LoadAndRewritePartitionSchema(
                GetFileSystemRootDirectory(), mOptions,
                schema->GetSchemaVersionId());
    // newSchema->CloneVirtualAttributes(*schema);
    newSchema->AddVirtualAttributeConfigs(mainVirtualAttrConfigs);
    const IndexPartitionSchemaPtr &newSubSchema = newSchema->GetSubIndexPartitionSchema();
    if (newSubSchema)
    {
        newSubSchema->AddVirtualAttributeConfigs(subVirtualAttrConfigs);
    }
    if(HasSameVirtualAttrSchema(newSchema, schema)
       && HasSameVirtualAttrSchema(newSubSchema, schema->GetSubIndexPartitionSchema()))
    {
        return IndexPartitionSchemaPtr();
    }
    return newSchema;
}

bool OnlinePartition::HasSameVirtualAttrSchema(
        const IndexPartitionSchemaPtr& newSchema,
        const IndexPartitionSchemaPtr& oldSchema)
{
    if (!newSchema && !oldSchema)
    {
        return true;
    }
    AttributeSchemaPtr newAttrSchema = newSchema->GetVirtualAttributeSchema();
    AttributeSchemaPtr oldAttrSchema = oldSchema->GetVirtualAttributeSchema();
    if (!newAttrSchema)
    {
        return oldAttrSchema == AttributeSchemaPtr();
    }
    return newAttrSchema->HasSameAttributeConfigs(oldAttrSchema);
}

IndexPartitionSchemaPtr OnlinePartition::CreateRtSchema(const IndexPartitionOptions& options)
{
    IndexPartitionOptions clonedOptions = options;
    clonedOptions.SetNeedRewriteFieldType(true);
    IndexPartitionSchemaPtr rtSchema = 
        SchemaAdapter::LoadAndRewritePartitionSchema(GetFileSystemRootDirectory(),
                                                     clonedOptions,
                                                     mSchema->GetSchemaVersionId());
    if (rtSchema->GetTableType() != tt_kkv && rtSchema->GetTableType() != tt_kv)
    {
        SchemaAdapter::RewriteToRtSchema(rtSchema);
    }

    if (clonedOptions.IsOnline() && rtSchema->GetTableType() == tt_kv)
    {
        if (rtSchema->GetRegionCount() > 1)
        {
            // rt schema not use global param, avoid rt compress
            autil::legacy::json::JsonMap emptyMap;
            rtSchema->SetGlobalRegionIndexPreference(emptyMap);
        }
        const IndexSchemaPtr& indexSchema = rtSchema->GetIndexSchema(DEFAULT_REGIONID);
        KVIndexConfigPtr kvConfig = DYNAMIC_POINTER_CAST(KVIndexConfig,
                indexSchema->GetPrimaryKeyIndexConfig());
        const KVIndexPreference::ValueParam valueParam = 
            kvConfig->GetIndexPreference().GetValueParam();
        // online kv table will disable value compress
        KVIndexPreference::ValueParam rewriteValueParam(valueParam.IsEncode(), "");
        if (valueParam.IsFixLenAutoInline()) {
            rewriteValueParam.EnableFixLenAutoInline();
        }
        kvConfig->GetIndexPreference().SetValueParam(rewriteValueParam);
    }
    return rtSchema;
}

void OnlinePartition::ResetRtAndJoinDirPath(
        const file_system::IndexlibFileSystemPtr& fileSystem)
{
    assert(fileSystem);
    const string& rootPath = fileSystem->GetRootPath();
    string joinIndexPath = PathUtil::JoinPath(rootPath, 
            JOIN_INDEX_PARTITION_DIR_NAME);
    fileSystem->RemoveDirectory(joinIndexPath, true);

    string rtIndexPath = PathUtil::JoinPath(rootPath, 
            RT_INDEX_PARTITION_DIR_NAME);
    if (!mOptions.GetOnlineConfig().loadRemainFlushRealtimeIndex)
    {
        fileSystem->RemoveDirectory(rtIndexPath, true);
    }
}

PartitionModifierPtr OnlinePartition::CreateReaderModifier(
        const IndexPartitionReaderPtr& reader)
{
    assert(reader);
    PartitionModifierPtr modifier = 
        PartitionModifierCreator::CreateInplaceModifier(mSchema, reader);
    return modifier;
}

bool OnlinePartition::InitAccessCounters(
        const CounterMapPtr& counterMap,
        const IndexPartitionSchemaPtr& schema)
{
    assert(counterMap);
    IndexMetrics* indexMetrics = mOnlinePartMetrics->GetIndexMetrics();
    AttributeMetrics *attrMetrics = mOnlinePartMetrics->GetAttributeMetrics();
    if (indexMetrics)
    {
        const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
        assert(indexSchema);
        string counterNodePrefix = "online.access.index.";
        auto indexConfigs = indexSchema->CreateIterator(true);
        auto iter = indexConfigs->Begin();
        for (; iter != indexConfigs->End(); iter++)
        {
            const IndexConfigPtr& indexConfig = *iter;
            const string& indexName = indexConfig->GetIndexName();
            string counterNodePath = counterNodePrefix + indexName;
            const auto& counter = counterMap->GetAccCounter(counterNodePath);
            if (!counter)
            {
                IE_PREFIX_LOG(ERROR, "get counter[%s] failed]",
                        counterNodePath.c_str());
                return false;
            }
            indexMetrics->AddAccessCounter(indexName, counter);
        }
    }
    if (attrMetrics)
    {
        const AttributeSchemaPtr& attrSchema = schema->GetAttributeSchema();
        string counterNodePrefix = "online.access.attribute.";        
        if (attrSchema)
        {
            auto attrConfigs = attrSchema->CreateIterator();
            auto iter = attrConfigs->Begin();
            for (; iter != attrConfigs->End(); iter++)
            {
                const AttributeConfigPtr& attrConfig = *iter;
                const string& attrName = attrConfig->GetAttrName();
                string counterNodePath = counterNodePrefix + attrName; 
                const auto& counter = counterMap->GetAccCounter(counterNodePath);
                if (!counter)
                {
                    IE_PREFIX_LOG(ERROR, "get counter[%s] failed]",
                            counterNodePath.c_str());
                    return false;
                }                
                attrMetrics->AddAccessCounter(attrName, counter);
            }
        }
    }
    const IndexPartitionSchemaPtr& subSchema = schema->GetSubIndexPartitionSchema();
    if (subSchema)
    {
        InitAccessCounters(mCounterMap, subSchema);
    }
    return true;
}

void OnlinePartition::ExecuteTask(OnlinePartitionTaskItem::TaskType taskType)
{
    if (taskType == OnlinePartitionTaskItem::TT_REPORT_METRICS)
    {
        ReportMetrics();
        return;
    }

    if (taskType == OnlinePartitionTaskItem::TT_CLEAN_RESOURCE)
    {
        if (mCleanerLock.trylock() != 0)
        {
            return;
        }

        if (mDataLock.trylock() != 0)
        {
            mCleanerLock.unlock();
            return;
        }

        ExecuteCleanResourceTask();
        mDataLock.unlock();
        mCleanerLock.unlock();
        return;
    }

    if (taskType == OnlinePartitionTaskItem::TT_INTERVAL_DUMP)
    {
        DumpSegmentOverInterval();
        return;
    }

    if (taskType == OnlinePartitionTaskItem::TT_TRIGGER_ASYNC_DUMP)
    {
        if (mAsyncSegmentDumper)
        {
            mAsyncSegmentDumper->TriggerDumpIfNecessary();
        }
        return;
    }

    if (taskType == OnlinePartitionTaskItem::TT_CHECK_SECONDARY_INDEX)
    {
        CheckSecondIndex();
        return;
    }
    
    if (taskType == OnlinePartitionTaskItem::TT_SUBSCRIBE_SECONDARY_INDEX)
    {
        SubscribeSecondIndex();
        return;
    }
}

void OnlinePartition::ExecuteCleanResourceTask()
{
    if (mClosed)
    {
        return;
    }
    
    try
    {
        ScopedLock lock(mCleanerLock);
        if (!mIsServiceNormal)
        {
            return;
        }
        CleanResource();
        DoReopen(false, mLoadedIncVersion.GetVersionId());
    }
    catch(ExceptionBase& e)
    {
        IE_PREFIX_LOG(ERROR, "clean resource exception [%s]!!", e.what());
    }
    catch(...)
    {
        IE_PREFIX_LOG(ERROR, "clean resource exception!!");
    }
}

IndexPartition::OpenStatus OnlinePartition::NormalReopen(
        ScopedLockPtr& scopedLock, Version& onDiskVersion)
{
    IE_PREFIX_LOG(INFO, "normal reopen begin, version from [%d] to [%d]",
                  mLoadedIncVersion.GetVersionId(), onDiskVersion.GetVersionId());
    {
        ExecutorResource&& resource = CreateExecutorResource(onDiskVersion, false);
        size_t incExpandMemSizeInBytes = 
            ReopenPartitionReaderExecutor::EstimateReopenMemoryUse(resource, mPartitionName);
        int64_t maxReopenMemoryUseInMB = 0;
        if (mOptions.GetOnlineConfig().GetMaxReopenMemoryUse(maxReopenMemoryUseInMB)
            && incExpandMemSizeInBytes >= (size_t)maxReopenMemoryUseInMB * 1024 * 1024)
        {
            IE_PREFIX_LOG(WARN, "incExpandMemSizeInMB [%lu] over maxReopenMemoryUse [%ld]",
                          incExpandMemSizeInBytes / (1024 * 1024), maxReopenMemoryUseInMB);
            return OS_FORCE_REOPEN;
        }
    }

    IndexPhaseGuard phaseGuard(mIndexPhase, NORMAL_REOPEN);
    OpenExecutorChainPtr chain = CreateReopenExecutorChain(onDiskVersion, scopedLock);
    ExecutorResource resource = CreateExecutorResource(onDiskVersion, false);
    if (!chain->Execute(resource))
    {
        IE_PREFIX_LOG(INFO, "normal reopen fail, version[%d]",
                      onDiskVersion.GetVersionId());
        return OS_FAIL;
    }
    mLatestNeedSkipIncTs = resource.mLatestNeedSkipIncTs;

    MEMORY_BARRIER();
    SetReaderHashId(mReader ? mReader->GetPartitionVersionHashId() : 0);

    ReportMetrics();
    AddOnDiskIndexCleaner();
    IE_PREFIX_LOG(INFO, "normal reopen success, version[%d]",
                  onDiskVersion.GetVersionId());
    return OS_OK;
}

OpenExecutorPtr OnlinePartition::CreateUnlockExecutor(ScopedLockPtr& scopedLock)
{
    return OpenExecutorPtr(new UnlockExecutor(scopedLock, mDataLock));
}
OpenExecutorPtr OnlinePartition::CreatePreloadExecutor()
{
    return OpenExecutorPtr(new PreloadExecutor());
}
OpenExecutorPtr OnlinePartition::CreatePreJoinExecutor(const JoinSegmentWriterPtr& joinSegWriter)
{
    return OpenExecutorPtr(new PrejoinExecutor(joinSegWriter));
}

OpenExecutorPtr OnlinePartition::CreateLockExecutor(ScopedLockPtr& scopedLock)
{
    return OpenExecutorPtr(new LockExecutor(scopedLock, mDataLock));
}
OpenExecutorPtr OnlinePartition::CreateDumpSegmentExecutor(bool reInitReaderAndWriter)
{
    return OpenExecutorPtr(new DumpSegmentExecutor(mPartitionName, reInitReaderAndWriter));
}
OpenExecutorPtr OnlinePartition::CreateReclaimRtIndexExecutor()
{
    return OpenExecutorPtr(new ReclaimRtIndexExecutor());
}
OpenExecutorPtr OnlinePartition::CreateReclaimRtSegmentsExecutor(bool reclaimBuildingSegment)
{
    return OpenExecutorPtr(new ReclaimRtSegmentsExecutor(reclaimBuildingSegment));
}
OpenExecutorPtr OnlinePartition::CreateGenerateJoinSegmentExecutor(
        const JoinSegmentWriterPtr& joinSegWriter)
{
    return OpenExecutorPtr(new GenerateJoinSegmentExecutor(joinSegWriter));
}
OpenExecutorPtr OnlinePartition::CreateReopenPartitionReaderExecutor(bool hasPreload)
{
    return OpenExecutorPtr(new ReopenPartitionReaderExecutor(hasPreload, mPartitionName));
}
OpenExecutorPtr OnlinePartition::CreateReleaseReaderExecutor()
{
    return OpenExecutorPtr(new ReleasePartitionReaderExecutor());
}

ExecutorResource OnlinePartition::CreateExecutorResource(const Version& onDiskVersion,
        bool forceReopen)
{
    ExecutorResource resource(mLoadedIncVersion, mReader, mReaderLock, mWriter,
                              mPartitionDataHolder, *mOnlinePartMetrics, mNeedReload,
                              mOptions, GetLatestNeedSkipIncTs(onDiskVersion));

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
    return resource;
}

OpenExecutorChainPtr OnlinePartition::CreateReopenExecutorChain(
        Version& onDiskVersion, ScopedLockPtr& scopedLock)
{
    
    OpenExecutorChainPtr chain = createOpenExecutorChain();
    if (mSchema->GetTableType() != tt_kkv && mSchema->GetTableType() != tt_kv)
    {
        JoinSegmentWriterPtr joinSegWriter = CreateJoinSegmentWriter(onDiskVersion, false);
        // release dataLock for rt build when preload
        chain->PushBack(CreateUnlockExecutor(scopedLock));
        chain->PushBack(CreatePreloadExecutor());
        chain->PushBack(CreatePreJoinExecutor(joinSegWriter));
        //push current building segment to dump container
        chain->PushBack(CreateLockExecutor(scopedLock));

        bool enableAsyncDump = mOptions.GetOnlineConfig().enableAsyncDumpSegment;
        chain->PushBack(CreateDumpSegmentExecutor(enableAsyncDump));
        if (enableAsyncDump) {
            chain->PushBack(CreateUnlockExecutor(scopedLock));
            chain->PushBack(OpenExecutorPtr(new DumpContainerFlushExecutor(&mDataLock, mPartitionName)));
            chain->PushBack(CreateLockExecutor(scopedLock));
            chain->PushBack(CreateDumpSegmentExecutor());
            //push left building doc to dump container
            chain->PushBack(OpenExecutorPtr(new DumpContainerFlushExecutor(&mDataLock, mPartitionName)));            
        }
        chain->PushBack(CreateReclaimRtIndexExecutor());
        chain->PushBack(CreateGenerateJoinSegmentExecutor(joinSegWriter));
        chain->PushBack(CreateReopenPartitionReaderExecutor(true));
    }
    else
    {
        // release dataLock for rt build when preload
        chain->PushBack(CreateUnlockExecutor(scopedLock));
        chain->PushBack(CreatePreloadExecutor());
        chain->PushBack(OpenExecutorPtr(new DumpContainerFlushExecutor(&mDataLock, mPartitionName)));

        chain->PushBack(CreateLockExecutor(scopedLock));
        chain->PushBack(OpenExecutorPtr(new DumpContainerFlushExecutor(&mDataLock, mPartitionName)));
        chain->PushBack(CreateReclaimRtSegmentsExecutor(true));
        chain->PushBack(CreateReopenPartitionReaderExecutor(true));
    }
    
    return chain;
}

OpenExecutorChainPtr OnlinePartition::CreateForceReopenExecutorChain(
        Version& onDiskVersion)
{
    OpenExecutorChainPtr chain(new OpenExecutorChain);
    if (mSchema->GetTableType() != tt_kkv && mSchema->GetTableType() != tt_kv)
    {
        JoinSegmentWriterPtr joinSegWriter =
            CreateJoinSegmentWriter(onDiskVersion, true);

        bool enableAsyncDump = mOptions.GetOnlineConfig().enableAsyncDumpSegment;
        chain->PushBack(CreateDumpSegmentExecutor(enableAsyncDump));
        if (enableAsyncDump) {
            chain->PushBack(OpenExecutorPtr(new DumpContainerFlushExecutor(&mDataLock, mPartitionName))); 
        }
        chain->PushBack(CreateReclaimRtIndexExecutor());
        chain->PushBack(CreateGenerateJoinSegmentExecutor(joinSegWriter));
        chain->PushBack(CreateReleaseReaderExecutor());
        chain->PushBack(CreateReopenPartitionReaderExecutor(false));
    }
    else
    {
        chain->PushBack(OpenExecutorPtr(new DumpContainerFlushExecutor(&mDataLock, mPartitionName)));
        chain->PushBack(CreateReclaimRtSegmentsExecutor(true));
        chain->PushBack(CreateReleaseReaderExecutor());
        chain->PushBack(CreateReopenPartitionReaderExecutor(false));
    }
    return chain;
}

segmentid_t OnlinePartition::GetLatestLinkRtSegId(
        const PartitionDataPtr& partData, const MemoryReserverPtr& memReserver) const
{
    assert(partData);
    assert(memReserver);
    
    segmentid_t lastReaderLinkRtSegId =
        !mReader ? INVALID_SEGMENTID : mReader->GetPartitionVersion().GetLastLinkRtSegmentId();
    segmentid_t lastValidLinkRtSegId = partData->GetLastValidRtSegmentInLinkDirectory();
    if (lastValidLinkRtSegId == INVALID_SEGMENTID)
    {
        return INVALID_SEGMENTID;
    }

    OnDiskRealtimeDataCalculator rtCalculator;
    rtCalculator.Init(mSchema, partData, mPluginManager);
    size_t switchExpandMemSize = rtCalculator.CalculateExpandSize(
            lastReaderLinkRtSegId, lastValidLinkRtSegId);
    if (switchExpandMemSize > 0)
    {
        if (!memReserver->Reserve(switchExpandMemSize))
        {
            IE_PREFIX_LOG(WARN, "switch to latest flush rt segments [%d] fail."
                          "mem need [%ld], mem left [%ld]", lastValidLinkRtSegId,
                          switchExpandMemSize, memReserver->GetFreeQuota());
            return lastReaderLinkRtSegId;
        }
    }
    return lastValidLinkRtSegId;    
}

void OnlinePartition::ReclaimRemainFlushRtIndex(const PartitionDataPtr& partData)
{
    assert(partData);
    file_system::DirectoryPtr rtPartDir =
        partData->GetRootDirectory()->GetDirectory(RT_INDEX_PARTITION_DIR_NAME, false);
    if (rtPartDir)
    {
        // reclaim rt segment if new inc index dp to index
        Version version = partData->GetVersion();
        segmentid_t lastSegId = version.GetLastSegment();
        if (lastSegId != INVALID_SEGMENTID &&
            RealtimeSegmentDirectory::IsRtSegmentId(lastSegId))
        {
            Version onDiskVersion = partData->GetOnDiskVersion();
            // only called in open, no building segments
            OpenExecutorPtr executor = CreateReclaimRtSegmentsExecutor(false);
            ExecutorResource resource = CreateExecutorResource(onDiskVersion, false);
            executor->Execute(resource);
        }
    }
}

void OnlinePartition::UpdateSwitchRtSegmentSize()
{
    if (mResourceCalculator && mOptions.GetOnlineConfig().onDiskFlushRealtimeIndex)
    {
        // clean thread may cause rt segment be removed
        // update switch rt segment may throw exception for no segment_info file
        if (mCleanerLock.trylock() != 0)
        {
            return;
        }
        
        try
        {
            vector<segmentid_t> switchRtSegmentIds;
            mReaderContainer->GetSwitchRtSegments(switchRtSegmentIds);
            mResourceCalculator->UpdateSwitchRtSegments(mRtSchema, switchRtSegmentIds);
        }
        catch (const ExceptionBase& e)
        {
            IE_PREFIX_LOG(ERROR, "error occurred in calculate size"
                          " for UpdateSwitchRtSegments, [%s]", e.what());
        }
        catch (...)
        {
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
    if (!locatorStr.empty() && !locator.fromString(locatorStr))
    {
        IE_PREFIX_LOG(WARN, "deserialze rt locator fail!");
        return mLatestNeedSkipIncTs;
    }

    if (locator == INVALID_INDEX_LOCATOR)
    {
        return mLatestNeedSkipIncTs;
    }
    OnlineJoinPolicy joinPolicy(incVersion, mSchema->GetTableType());

    bool fromInc;
    int64_t rtSeekTs = joinPolicy.GetRtSeekTimestamp(locator, fromInc);
    if (fromInc)
    {
        return rtSeekTs;
    }
    return mLatestNeedSkipIncTs;
}

void OnlinePartition::DumpSegmentOverInterval()
{
    if (mDataLock.trylock() != 0)
    {
        return;
    }

    if (!mWriter)
    {
        mDataLock.unlock();
        return;
    }

    int64_t maxDumpInterval = mOptions.GetOnlineConfig().maxRealtimeDumpInterval;
    int64_t currentTimeInSeconds = TimeUtility::currentTimeInSeconds();
    if (currentTimeInSeconds - mWriter->GetLastDumpTimestamp() < maxDumpInterval)
    {
        mDataLock.unlock();
        return;
    }

    IE_PREFIX_LOG(INFO, "trigger realtime dumpsegment by dump interval [%ld]", maxDumpInterval);
    OpenExecutorPtr dumpSegmentExecutor = CreateDumpSegmentExecutor();
    ExecutorResource resource = CreateExecutorResource(mLoadedIncVersion, false);
    try
    {
        dumpSegmentExecutor->Execute(resource);
    }
    catch(const ExceptionBase& e)
    {
        IE_PREFIX_LOG(FATAL, "error occurred in onlinepartition DumpSegmentOverInterval[%s]",
                      e.what());
    }
    if (mReaderUpdater)
    {
        mReaderUpdater->Update(mReader);
    }

    mDataLock.unlock();
}

void OnlinePartition::FlushDumpSegmentInContainer()
{
    IE_PREFIX_LOG(INFO, "flush dump segment begin.");
    while (mWriter || !mClosed || mDumpSegmentContainer->GetLastSegment())
    {
        bool hasDumpedSegment = false;
        {
            ScopedLock lock(mCleanerLock);   
            NormalSegmentDumpItemPtr item = mDumpSegmentContainer->GetOneSegmentItemToDump();
            if (item)
            {
                try
                {
                    item->Dump();
                }
                catch (const misc::ExceptionBase& e)
                {
                    IE_PREFIX_LOG(ERROR, "flush dump segment end. "
                            "because catch exception:[%s].", e.what());
                    return;
                }
                mPartitionDataHolder.Get()->CommitVersion();
                hasDumpedSegment = true;
            }
            
            if (!mClosed && hasDumpedSegment)
            {
                try
                {
                    ReOpenNewSegment();
                }
                catch (const misc::ExceptionBase& e)
                {
                    IE_PREFIX_LOG(ERROR, "async dump segment thread end. "
                            "because reopen new segment catch exception:[%s].", e.what());
                    return;
                }
            }

            if (!hasDumpedSegment)
            {
                break;
            }
        }
        usleep(500000); // 1s            
    }
    IE_PREFIX_LOG(INFO, "flush dump segment end.");
}

MemoryQuotaSynchronizerPtr OnlinePartition::CreateRealtimeMemoryQuotaSynchronizer(
        const MemoryQuotaControllerPtr& realtimeQuotaController)
{
    assert(realtimeQuotaController);
    PartitionMemoryQuotaControllerPtr rtPartitionMemoryQuotaControl(
            new PartitionMemoryQuotaController(realtimeQuotaController,
                    "realtime_partition_controller"));
    
    BlockMemoryQuotaControllerPtr blockMemQuotaControl(
            new BlockMemoryQuotaController(rtPartitionMemoryQuotaControl,
                    "block_realtime_partition_controller"));
    
    MemoryQuotaSynchronizerPtr memQuotaSync(new MemoryQuotaSynchronizer(blockMemQuotaControl));
    return memQuotaSync;
}

namespace
{
    bool TryLock(autil::ThreadMutex& lock, size_t times, int64_t intervalInUs)
    {
        size_t trys = 0;
        while (trys++ < times)
        {
            if (lock.trylock() == 0)
            {
                return true;
            }
            usleep(intervalInUs);
        }
        return false;
    }
}

void OnlinePartition::CheckSecondIndex()
{
    index_base::Version loadedIncVersion;
    if (!TryLock(mDataLock, 5, 1000))
    {
        return;
    }

    loadedIncVersion = mLoadedIncVersion;
    mDataLock.unlock();

    int64_t missingSegmentCount = 0;
    string versionPath = FileSystemWrapper::JoinPath(mOpenIndexPrimaryDir, loadedIncVersion.GetVersionFileName());
    bool hasException = false;
    try
    {
        if (!mFileSystem->IsExistInSecondaryPath(versionPath, true))
        {
            missingSegmentCount = loadedIncVersion.GetSegmentCount();
            IE_PREFIX_LOG(ERROR, "segment version [%s] not exist", versionPath.c_str());
        }
        else
        {
            const auto& segmentVec = loadedIncVersion.GetSegmentVector();
            for (const auto& segmentId : segmentVec)
            {
                string segmentPath = FileSystemWrapper::JoinPath(mOpenIndexPrimaryDir,
                                                                 loadedIncVersion.GetSegmentDirName(segmentId));
                string segInfoPath = FileSystemWrapper::JoinPath(segmentPath, SEGMENT_INFO_FILE_NAME);
                if (!mFileSystem->IsExistInSecondaryPath(segInfoPath, true))
                {
                    ++missingSegmentCount;
                    IE_PREFIX_LOG(ERROR, "segment_info [%s] not exist", segInfoPath.c_str());
                }
            }
        }
    }
    catch (const ExceptionBase& e)
    {
        hasException = true;
        IE_PREFIX_LOG(ERROR, "caught exception %s", e.GetMessage().c_str());
    }
    catch (...)
    {
        hasException = true;
        auto eptr = std::current_exception();
        INDEXLIB_HANDLE_EPTR(eptr);
    }

    if (missingSegmentCount > 0 || (missingSegmentCount == 0 && !hasException))
    {
        if (!TryLock(mDataLock, 5, 1000))
        {
            return;
        }
        if (loadedIncVersion.GetVersionId() == mLoadedIncVersion.GetVersionId())
        {
            mMissingSegmentCount = missingSegmentCount;
        }
        mDataLock.unlock();
    }
}

void OnlinePartition::SubscribeSecondIndex()
{
    versionid_t loadedIncVersionId = mLoadedIncVersion.GetVersionId();

    auto deployMetaLockFilePath = PathUtil::JoinPath(
        mOpenIndexPrimaryDir,
        DeployIndexWrapper::GetDeployMetaLockFileName(loadedIncVersionId));
    std::string absPath;
    if (!mFileSystem->GetAbsSecondaryPath(deployMetaLockFilePath, absPath))
    {
        IE_PREFIX_LOG(ERROR, "Get Absolute path for deploy meta lock file [%s] failed!",
            deployMetaLockFilePath.c_str());
        return;
    }
    {
        std::string ipAddr;
        if (!NetUtil::GetDefaultIp(ipAddr))
        {
            IE_PREFIX_LOG(ERROR, "Get IpAddr failed!");
            return;
        }
        if (!mFileSystem->UpdatePanguInlineFile(absPath, ipAddr))
        {
            IE_PREFIX_LOG(ERROR, "Update deploy meta lock file [%s] failed!", absPath.c_str());
        }
        else
        {
            IE_PREFIX_LOG(INFO, "Update deploy meta lock file [%s] successfully, ipAddr [%s]",
                absPath.c_str(), ipAddr.c_str());
        }
    }
}

#undef IE_LOG_PREFIX

IE_NAMESPACE_END(partition);

