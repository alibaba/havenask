#include <future>
#include <chrono>
#include <autil/HashAlgorithm.h>
#include "indexlib/partition/custom_online_partition.h"
#include "indexlib/partition/custom_online_partition_writer.h"
#include "indexlib/partition/online_partition_metrics.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/partition/index_partition_resource.h"
#include "indexlib/partition/custom_online_partition_writer.h"
#include "indexlib/partition/custom_online_partition_reader.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/partition/deploy_index_validator.h"
#include "indexlib/partition/reader_container.h"
#include "indexlib/partition/partition_resource_calculator.h"
#include "indexlib/partition/partition_reader_cleaner.h"
#include "indexlib/partition/in_memory_index_cleaner.h"
#include "indexlib/partition/on_disk_index_cleaner.h"
#include "indexlib/partition/local_index_cleaner.h"
#include "indexlib/partition/reopen_decider.h"
#include "indexlib/partition/memory_stat_collector.h"
#include "indexlib/partition/memory_stat_reporter.h"
#include "indexlib/partition/segment/dump_segment_queue.h"
#include "indexlib/util/memory_control/memory_quota_controller_creator.h"
#include "indexlib/util/memory_control/memory_quota_synchronizer.h"
#include "indexlib/util/memory_control/block_memory_quota_controller.h"
#include "indexlib/util/memory_control/memory_reserver.h"
#include "indexlib/util/member_function_task_item.h"
#include "indexlib/util/task_scheduler.h"
#include "indexlib/util/path_util.h"
#include "indexlib/util/env_util.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/index/online_join_policy.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_base/index_meta/index_file_list.h"
#include "indexlib/table/table_plugin_loader.h"
#include "indexlib/table/table_factory_wrapper.h"
#include "indexlib/table/table_reader.h"
#include "indexlib/partition/open_executor/open_executor_chain.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/file_system/indexlib_file_system.h"
#include "indexlib/common/executor_scheduler.h"
#include "indexlib/util/counter/counter_map.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(table);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, CustomOnlinePartition);

#define IE_LOG_PREFIX mPartitionName.c_str()

size_t CustomOnlinePartition::DEFAULT_IO_EXCEPTION_RETRY_LIMIT = 3u;
size_t CustomOnlinePartition::DEFAULT_IO_EXCEPTION_RETRY_INTERVAL = 1000000; // 1 second
    

CustomOnlinePartition::CustomOnlinePartition(
    const std::string& partitionName, const IndexPartitionResource& partitionResource)
    : IndexPartition(partitionName, partitionResource)
    , mClosed(true)
    , mIsServiceNormal(false)
    , mIndexPhase(PENDING)      
    , mOnlinePartMetrics(new OnlinePartitionMetrics(partitionResource.metricProvider))
    , mCleanPartitionReaderTaskId(TaskScheduler::INVALID_TASK_ID)
    , mCleanResourceTaskId(TaskScheduler::INVALID_TASK_ID)
    , mReportMetricsTaskId(TaskScheduler::INVALID_TASK_ID)
    , mAsyncDumpTaskId(TaskScheduler::INVALID_TASK_ID)
    , mCheckIndexTaskId(TaskScheduler::INVALID_TASK_ID)
    , mCheckSecondaryIndexIntervalInMin(partitionResource.checkSecondIndexIntervalInMin)
    , mIOExceptionRetryLimit(DEFAULT_IO_EXCEPTION_RETRY_LIMIT)
    , mIOExceptionRetryInterval(DEFAULT_IO_EXCEPTION_RETRY_INTERVAL)
    , mMissingSegmentCount(0)
    , mSupportSegmentLevelReader(false)
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

CustomOnlinePartition::~CustomOnlinePartition() 
{
    try
    {
        Close();
    }
    catch(const ExceptionBase& e)
    {
        IE_PREFIX_LOG(FATAL, "ERROR occurred in OnlinePartition deconstructor[%s]",
                      e.what());
    }
    Reset();    
}

string CustomOnlinePartition::GetLastLocator() const
{
    string locator;
    if (mWriter)
    {
        locator = mWriter->GetLastLocator();
    }
    if (!locator.empty())
    {
        return locator;
    }
    PartitionDataPtr partitionData = mPartitionDataHolder.Get();
    return partitionData ? partitionData->GetLastLocator() : std::string(""); 
} 


void CustomOnlinePartition::Reset()
{
    mResourceCalculator.reset();
    mResourceCleaner.reset();
    mLocalIndexCleaner.reset();
    mReaderContainer.reset();
    mReader.reset();
    mWriter.reset();
    mDumpSegmentQueue.reset();
    mPluginManager.reset();
    IndexPartition::Reset();
}

void CustomOnlinePartition::Close()
{
    if (mClosed)
    {
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

IndexPartition::OpenStatus CustomOnlinePartition::Open(
    const std::string& primaryDir, const std::string& secondaryDir,
    const config::IndexPartitionSchemaPtr& schema,
    const config::IndexPartitionOptions& options,
    versionid_t targetVersionId)
{
    IndexPartition::OpenStatus os = OS_FAIL;
    try
    {
        os = DoOpen(primaryDir, secondaryDir, schema, options, targetVersionId, false);
        if (os != OS_OK)
        {
            IE_LOG(ERROR, "open partition failed. Partition is to be closed");
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
    return os;
}

IndexPartition::OpenStatus CustomOnlinePartition::DoOpen(
        const string& primaryDir, const string& secondaryDir,
        const IndexPartitionSchemaPtr& schema,
        const IndexPartitionOptions& options, versionid_t targetVersionId,
        bool isForceOpen)
{
    IE_PREFIX_LOG(INFO, "Open Index Partition: primary[%s], secondary[%s]",
                  primaryDir.c_str(), secondaryDir.c_str());
    IndexPhaseGuard phaseGuard(mIndexPhase, OPENING);    
    mReaderContainer.reset(new ReaderContainer);
    if (!mDumpSegmentQueue)
    {
        mDumpSegmentQueue.reset(new DumpSegmentQueue());
    }
    int64_t ioExceptionRetryLimit = EnvUtil::GetEnv<int64_t>("CUSTOM_IO_EXCEPTION_RETRY_LIMIT", -1);
    int64_t ioExceptionRetryInterval = EnvUtil::GetEnv<int64_t>("CUSTOM_IO_EXCEPTION_RETRY_INTERVAL", -1);
    if (ioExceptionRetryLimit != -1)
    {
        mIOExceptionRetryLimit = ioExceptionRetryLimit;
    }
    if (ioExceptionRetryInterval != -1)
    {
        mIOExceptionRetryInterval = ioExceptionRetryInterval;
    }
    IE_LOG(INFO, "IO exception retry limit = [%zu], retry interval = [%zu]",
           mIOExceptionRetryLimit, mIOExceptionRetryInterval);
    
    IndexPartition::OpenStatus openStatus = IndexPartition::Open(
            primaryDir, secondaryDir, schema, options, targetVersionId);

    if (openStatus != OS_OK) {
        return openStatus;
    }
    mReaderContainer.reset(new ReaderContainer);
    if (!mRealtimeQuotaController)
    {
        mRealtimeQuotaController.reset(new MemoryQuotaController(
                        options.GetOnlineConfig().maxRealtimeMemSize * 1024 * 1024));
    }
    mRtMemQuotaSynchronizer = CreateRealtimeMemoryQuotaSynchronizer(mRealtimeQuotaController);
    mClosed = true;

    Version onDiskVersion;
    VersionLoader::GetVersion(GetFileSystemRootDirectory(), onDiskVersion, targetVersionId);
    InitPathMetaCache(GetFileSystemRootDirectory(), onDiskVersion);

    ScopedLock lock(mDataLock);
    ScopedLock schemaLock(mSchemaLock);
    IE_PREFIX_LOG(INFO, "current free memory is %ld MB", 
                  mPartitionMemController->GetFreeQuota() / (1024 * 1024));

    // rt build disable package file
    mOptions.GetBuildConfig(true).enablePackageFile = false;

    mSchema = SchemaAdapter::LoadSchema(GetFileSystemRootDirectory(), onDiskVersion.GetSchemaVersionId());
    CheckParam(mSchema, mOptions);
    if (schema)
    {
        mSchema->AssertCompatible(*schema);
    }

    mTableWriterMemController.reset(new BlockMemoryQuotaController(
        mPartitionMemController, mSchema->GetSchemaName() + "_TableWriterMemory"));
    
    mOnlinePartMetrics->RegisterMetrics(mSchema->GetTableType());
    mCounterMap.reset(new CounterMap());

    mPluginManager =
        TablePluginLoader::Load(mIndexPluginPath, mSchema, mOptions);

    if (!mPluginManager)
    {
        IE_LOG(ERROR, "load index plugin failed for schema [%s]",
               mSchema->GetSchemaName().c_str());
        return OS_FAIL;
    }
    if (!mTableFactory)
    {
        mTableFactory.reset(new TableFactoryWrapper(
                                mSchema, mOptions, mPluginManager));
        if (!mTableFactory->Init())
        {
            IE_LOG(ERROR, "Init TableFactory failed in table[%s]",
                   mSchema->GetSchemaName().c_str());
            return OS_FAIL;
        }
    } 

    PartitionDataPtr newPartitionData = CreateCustomPartitionData(
        onDiskVersion, INVALID_VERSION, mDumpSegmentQueue, -1, false);
    mPartitionDataHolder.Reset(newPartitionData);
    
    if (mOptions.GetOnlineConfig().enableLoadSpeedControlForOpen)
    {
        mFileSystem->EnableLoadSpeedSwitch();         
    }
    else
    {
        mFileSystem->DisableLoadSpeedSwitch();        
    }

    mWriter.reset(new CustomOnlinePartitionWriter(
                         mOptions, mTableFactory, mFlushedLocatorContainer,
                         mOnlinePartMetrics.get(), mPartitionName));
    InitWriter(mPartitionDataHolder.Get(), mWriter);

    InitResourceCalculator(mPartitionDataHolder.Get(), mWriter);
    auto reader = InitReaderWithMemoryLimit(
        mPartitionDataHolder.Get(), mWriter, INVALID_VERSION, onDiskVersion);

    if (reader)
    {
        auto tableReader = reader->GetTableReader();
        mSupportSegmentLevelReader = tableReader->SupportSegmentLevelReader();
    }
    SwitchReader(reader);
    MEMORY_BARRIER();
    SetReaderHashId(mReader ? mReader->GetPartitionVersionHashId() : 0); 
    
    mFileSystem->EnableLoadSpeedSwitch();
    mLoadedIncVersion = mPartitionDataHolder.Get()->GetOnDiskVersion();

    InitResourceCleaner();
    CleanPartitionReaders();
    CleanResource();
    ReportMetrics();

    if (!mReaderUpdater)
    {
        mReaderUpdater.reset(new SearchReaderUpdater(mSchema->GetSchemaName()));
    }

    if (!PrepareIntervalTask())
    {
        return OS_FAIL;
    }
    mClosed = false;
    mIsServiceNormal = true;
    IE_PREFIX_LOG(INFO, "Open Index Partition End");
    return OS_OK;    
}

void CustomOnlinePartition::InitResourceCalculator(
    const PartitionDataPtr& partData, const CustomOnlinePartitionWriterPtr& writer)
{
    DirectoryPtr rootDirectory = partData->GetRootDirectory();
    mResourceCalculator.reset(
            new PartitionResourceCalculator(mOptions.GetOnlineConfig()));
    mResourceCalculator->Init(rootDirectory, writer, InMemorySegmentContainerPtr(), mPluginManager); 
}

CustomOnlinePartitionReaderPtr CustomOnlinePartition::InitReaderWithMemoryLimit(
    const PartitionDataPtr& partData,
    const CustomOnlinePartitionWriterPtr& writer,
    const index_base::Version& lastIncVersion,
    const index_base::Version& newIncVersion)
{
    assert(mResourceCalculator);
    CustomOnlinePartitionReaderPtr reader;
    DirectoryPtr rootDirectory = partData->GetRootDirectory();
    size_t retryCounter = 0;
    while (true)
    {
        try
        {
            // TODO: delete oldest rt segments to release memory
            size_t readerEstimateMemUse = mResourceCalculator->EstimateDiffVersionLockSizeWithoutPatch(
                mSchema, rootDirectory, newIncVersion, lastIncVersion);
            const MemoryReserverPtr memReserver =
                mFileSystem->CreateMemoryReserver("reopen_partition_reader_executor");
            if (!memReserver->Reserve(readerEstimateMemUse))
            {
                INDEXLIB_FATAL_ERROR(
                    OutOfMemory, "load reader fail, "
                    "incExpandMemSize [%luB] over FreeQuota [%ldB]",
                    readerEstimateMemUse, memReserver->GetFreeQuota());
            }
            return InitReader(writer, partData);
        }
        catch(const FileIOException& e)
        {
            if (retryCounter >= mIOExceptionRetryLimit)
            {
                IE_LOG(ERROR, "reach IOException[%s] Limit[%lu] while init reader for partition[%s], "
                       "load version[%d]", e.GetMessage().c_str(),
                       mIOExceptionRetryLimit, mPartitionName.c_str(), newIncVersion.GetVersionId()); 
                throw; 
            }
            else
            {
                retryCounter++;
                IE_LOG(WARN, "raise IOException[%s] while init reader for partition[%s], "
                       "load version[%d], trigger retry", e.GetMessage().c_str(),
                       mPartitionName.c_str(), newIncVersion.GetVersionId()); 
                usleep(mIOExceptionRetryInterval); 
                continue;
            }
        }        
        catch(...)
        {
            IE_LOG(ERROR, "init reader failed for partition[%s] when load version[%d]",
                   mPartitionName.c_str(), newIncVersion.GetVersionId());
            throw;
        }        
    }
    return reader;
}

PartitionDataPtr CustomOnlinePartition::CreateCustomPartitionData(
    const index_base::Version& incVersion, 
    const index_base::Version& lastIncVersion,
    const DumpSegmentQueuePtr& dumpSegmentQueue, 
    int64_t reclaimTimestamp, 
    bool ignoreInMemVersion)
{
    size_t retryCounter = 0;
    while (true)
    {
        try
        {
            PartitionDataPtr newPartitionData = PartitionDataCreator::CreateCustomPartitionData(
                mFileSystem, mSchema, mOptions, mPartitionMemController, mTableWriterMemController,
                incVersion, mMetricProvider, mCounterMap, mPluginManager, dumpSegmentQueue, 
                reclaimTimestamp, ignoreInMemVersion);

            if (mOptions.GetOnlineConfig().IsValidateIndexEnabled())
            {
                DeployIndexValidator::ValidateDeploySegments(
                    newPartitionData->GetRootDirectory(),
                    incVersion - lastIncVersion);
            }
            return newPartitionData;
        }
        catch(const FileIOException& e)
        {
            if (retryCounter >= mIOExceptionRetryLimit)
            {
                IE_LOG(ERROR, "reach IOException[%s] Limit[%lu] while init PartitionData for partition[%s]",
                       e.GetMessage().c_str(), mIOExceptionRetryLimit, mPartitionName.c_str());                
                throw;
            }
            else
            {
                retryCounter++;
                IE_LOG(WARN, "raise IOException[%s] while init PartitionData for partition[%s], trigger retry",
                       e.GetMessage().c_str(), mPartitionName.c_str()); 
                usleep(mIOExceptionRetryInterval);
                continue;
            }
        }
        catch(...)
        {
            IE_LOG(ERROR, "init PartitionData failed for partition[%s]", mPartitionName.c_str());
            throw;
        }            
    }
}

void CustomOnlinePartition::InitWriter(
    const PartitionDataPtr& partData,
    CustomOnlinePartitionWriterPtr& writer)
{
    assert(writer);
    size_t retryCounter = 0;
    while (true)
    {
        try
        {
            writer->Open(mSchema, partData, PartitionModifierPtr());
            return;
        }
        catch(const FileIOException& e)
        {
            if (retryCounter >= mIOExceptionRetryLimit)
            {
                IE_LOG(ERROR, "reach IOException[%s] Limit[%lu] while init writer for partition[%s]",
                       e.GetMessage().c_str(), mIOExceptionRetryLimit, mPartitionName.c_str());                
                throw;
            }
            else
            {
                retryCounter++;
                IE_LOG(WARN, "raise IOException[%s] while init writer for partition[%s], trigger retry",
                       e.GetMessage().c_str(), mPartitionName.c_str()); 
                usleep(mIOExceptionRetryInterval);
                continue;
            }
        }
        catch(...)
        {
            IE_LOG(ERROR, "init writer failed for partition[%s]", mPartitionName.c_str());
            throw;
        }    
    }
}

CustomOnlinePartitionReaderPtr CustomOnlinePartition::InitReader(
    const CustomOnlinePartitionWriterPtr& writer,
    const PartitionDataPtr& partitionData)
{
    CustomOnlinePartitionReaderPtr reader;
    if (writer)
    {
        reader.reset(new CustomOnlinePartitionReader(
                mSchema, mOptions, mPartitionName, mTableFactory,
                writer->GetTableWriter()));    
    }
    else
    {
        TableWriterPtr nullWriter;;
        reader.reset(new CustomOnlinePartitionReader(
                mSchema, mOptions, mPartitionName, mTableFactory, nullWriter));
    }
    reader->SetForceSeekInfo(mForceSeekInfo);
    reader->Open(partitionData);
    return reader;
}

CustomOnlinePartitionReaderPtr CustomOnlinePartition::InitReaderWithHeritage(
    const CustomOnlinePartitionWriterPtr& writer,
    const PartitionDataPtr& partitionData,
    const CustomOnlinePartitionReaderPtr& lastReader,
    const CustomOnlinePartitionReaderPtr& preLoadReader)
{
    if (!lastReader)
    {
        return InitReader(writer, partitionData);
    }
    CustomOnlinePartitionReaderPtr reader(
        new CustomOnlinePartitionReader(
            mSchema, mOptions, mPartitionName, mTableFactory, 
            writer->GetTableWriter()));
    reader->SetForceSeekInfo(mForceSeekInfo);
    
    TableReaderPtr lastTableReader;
    if (lastReader)
    {
        lastTableReader = lastReader->GetTableReader();
    }

    TableReaderPtr preloadTableReader;
    if (preLoadReader)
    {
        preloadTableReader = preLoadReader->GetTableReader();
    }

    reader->OpenWithHeritage(partitionData, lastTableReader, preloadTableReader);
    return reader;
}

void CustomOnlinePartition::CheckParam(const IndexPartitionSchemaPtr& schema,
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

    if (schema->GetTableType() != tt_customized)
    {
        INDEXLIB_FATAL_ERROR(BadParameter,
                             "[%s] CustomOnlinePartition only support customzied table!",
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

MemoryQuotaSynchronizerPtr CustomOnlinePartition::CreateRealtimeMemoryQuotaSynchronizer(
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


IndexPartition::OpenStatus CustomOnlinePartition::ReOpen(
    bool forceReopen, versionid_t targetVersionId)
{
    OpenStatus os;
    try
    {
        if (mClosed)
        {
            IE_LOG(WARN, "Partition is closed. Try Open instead");
            return DoOpen(mOpenIndexPrimaryDir, mOpenIndexSecondaryDir,
                          mOpenSchema, mOpenOptions, targetVersionId, false);
        }
        CleanPartitionReaders();
        // scopelock will forbidden cleanResource when process reopen
        ScopedLock lock(mCleanerLock);
        CleanResource();
        return DoReopen(forceReopen, targetVersionId);
    }
    catch (const FileIOException& e)
    {
        IE_PREFIX_LOG(ERROR, "reopen caught file io exception: %s", e.what());
        if (!forceReopen && mOptions.GetOnlineConfig().IsReopenRetryOnIOExceptionEnabled())
        {
            // suez will retry normalReopen if return OS_FAIL & set RS_ALLOW_FORCE_LOAD = false
            os = OS_FAIL;
        }
        else
        {
            os = OS_FILEIO_EXCEPTION;
        }
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
    return os;
}

IndexPartition::OpenStatus CustomOnlinePartition::DoReopen(
        bool forceReopen, versionid_t targetVersionId)
{
    ScopedLockPtr scopedLock(new ScopedLock(mDataLock));
    
    misc::ScopeLatencyReporter scopeTime(
            mOnlinePartMetrics->GetreopenIncLatencyMetric().get());

    ReportMetrics();
    
    Version reopenVersion;
    ReopenDecider::ReopenType reopenType = MakeReopenDecision(
            forceReopen, targetVersionId, reopenVersion);

    OpenStatus ret = OS_FAIL;
    PartitionDataPtr partData = mPartitionDataHolder.Get();
    
    switch (reopenType)
    {
    case ReopenDecider::NORMAL_REOPEN:
        InitPathMetaCache(GetFileSystemRootDirectory(), reopenVersion);        
        ret = NormalReopen(scopedLock, reopenVersion);
        break;        
    case ReopenDecider::FORCE_REOPEN:
    {
        IE_LOG(WARN, "custom table force reopen version[%d] will "
               "abandon realtime data and switch to force *OPEN*", reopenVersion.GetVersionId());
        Close();
        Reset();
        ret = Open(mOpenIndexPrimaryDir, mOpenIndexSecondaryDir, mOpenSchema, mOpenOptions, targetVersionId);
        break;
    }        
    case ReopenDecider::NO_NEED_REOPEN:
        ret = IndexPartition::OS_OK;
        break;        
    case ReopenDecider::INCONSISTENT_SCHEMA_REOPEN:
        ret = IndexPartition::OS_INCONSISTENT_SCHEMA;
        break;
    case ReopenDecider::INDEX_ROLL_BACK_REOPEN:
        // TODO: return new flag: OS_INDEX_ROLL_BACK;
        ret = IndexPartition::OS_FAIL;
    default:
        IE_PREFIX_LOG(WARN, "unsupported reopenType : [%d]", reopenType);
        assert(false);        
    };

    if (ret == OS_OK && mReaderUpdater)
    {
        if (!mReaderUpdater->Update(mReader))
        {
            ret = OS_FAIL;
        }
    }    
    return ret;
}

CustomOnlinePartitionReaderPtr CustomOnlinePartition::CreateDiffVersionReader(
    const index_base::Version& newVersion, const index_base::Version& lastVersion)
{
    Version diffVersion = newVersion - lastVersion;
    DumpSegmentQueuePtr emptyQueue;
    CustomOnlinePartitionWriterPtr emptyWriter;
    Version invalidVersion(INVALID_VERSION);
    auto diffPartitionData = CreateCustomPartitionData(
        diffVersion, invalidVersion, emptyQueue, -1, true);
    return InitReaderWithMemoryLimit(diffPartitionData, emptyWriter, diffVersion, invalidVersion); 
}

IndexPartition::OpenStatus CustomOnlinePartition::NormalReopenWithSegmentLevelPreload(
    ScopedLockPtr& dataLock, 
    index_base::Version& incVersion)
{
    OnlineJoinPolicy joinPolicy(incVersion, mSchema->GetTableType());
    int64_t reclaimTimestamp = joinPolicy.GetReclaimRtTimestamp();
    PartitionDataPtr oldPartData = mPartitionDataHolder.Get();
    CustomOnlinePartitionReaderPtr newReader;
    
    dataLock.reset();
    CustomOnlinePartitionReaderPtr diffVersionReader = 
        CreateDiffVersionReader(incVersion, oldPartData->GetOnDiskVersion());
    IE_PREFIX_LOG(INFO, "create diffVersion done");

    PartitionDataPtr newPartitionData = CreateCustomPartitionData(
        incVersion, oldPartData->GetOnDiskVersion(), mDumpSegmentQueue, reclaimTimestamp, false);
    IE_PREFIX_LOG(INFO, "create new partition data done");

    TableResourcePtr newTableResource;
    if (!mWriter->PrepareTableResource(
            DYNAMIC_POINTER_CAST(CustomPartitionData,newPartitionData), 
            newTableResource))
    {
        IE_PREFIX_LOG(ERROR, "prepare TableResource failed");
        if (mOptions.GetOnlineConfig().IsReopenRetryOnIOExceptionEnabled())
        {
            IE_PREFIX_LOG(ERROR, "retryOnIOException is true, will return OS_FAIL"); 
            return OS_FAIL;
        }
        return OS_INDEXLIB_EXCEPTION;
    }
    TableResourcePtr oldTableResource;
    try
    {
        dataLock.reset(new ScopedLock(mDataLock)); 
        oldTableResource = mWriter->GetTableResource();
        mWriter->ResetPartitionData(newPartitionData, newTableResource);
        newReader = InitReaderWithHeritage(mWriter, newPartitionData, mReader, diffVersionReader);
        IE_LOG(INFO, "init reader with  new partition data done");
    }
    catch (const FileIOException& e)
    {
        IE_PREFIX_LOG(ERROR, "catch FILEIO exception in normalReopen, mWriter will be reset");
        mWriter->ResetPartitionData(oldPartData, oldTableResource);
        if (mOptions.GetOnlineConfig().IsReopenRetryOnIOExceptionEnabled())
        {
            return OS_FAIL;
        }
        return OS_FILEIO_EXCEPTION;
    }
    catch (const ExceptionBase& e)
    {
        IE_PREFIX_LOG(ERROR, "catch Indexlib exception in normalReopen, mWriter will be reset");
        mWriter->ResetPartitionData(oldPartData, oldTableResource);
        return OS_INDEXLIB_EXCEPTION;
    }
    catch (...)
    {
        IE_PREFIX_LOG(ERROR, "catch unknown exception in normalReopen, mWriter will be reset");
        mWriter->ResetPartitionData(oldPartData, oldTableResource);
        return OS_INDEXLIB_EXCEPTION;
    }
    mPartitionDataHolder.Reset(newPartitionData);
    // dump obsolete building segment, so it can be removed
    NewSegmentMetaPtr buildingSegMeta = mWriter->GetBuildingSegmentMeta();
    if (buildingSegMeta && (buildingSegMeta->segmentInfo.timestamp < reclaimTimestamp))
    {
        mWriter->DumpSegment();
        CustomPartitionDataPtr typedData = DYNAMIC_POINTER_CAST(CustomPartitionData, newPartitionData);
        typedData->RemoveObsoleteSegments();
        ReOpenNewSegment();
    }
    else
    {
        CustomPartitionDataPtr typedData = DYNAMIC_POINTER_CAST(CustomPartitionData, newPartitionData);
        typedData->RemoveObsoleteSegments();
        // SwitchReader depends on mPartitionDataHolder
        SwitchReader(newReader);
    }
    MEMORY_BARRIER();
    SetReaderHashId(mReader ? mReader->GetPartitionVersionHashId() : 0);
    ReportMetrics();
    AddOnDiskIndexCleaner();

    IE_PREFIX_LOG(INFO, "reopen end : normal reopen version[%d] success",
                  incVersion.GetVersionId());
    mLoadedIncVersion = incVersion;
    return OS_OK;    
}

IndexPartition::OpenStatus CustomOnlinePartition::NormalReopen(
    ScopedLockPtr& dataLock, 
    index_base::Version& incVersion)
{
    IE_PREFIX_LOG(INFO, "normal reopen begin");
    IndexPhaseGuard phaseGuard(mIndexPhase, NORMAL_REOPEN);

    if (mSupportSegmentLevelReader && mOptions.GetOnlineConfig().enableAsyncDumpSegment)
    {
        return NormalReopenWithSegmentLevelPreload(dataLock, incVersion); 
    }

    OnlineJoinPolicy joinPolicy(incVersion, mSchema->GetTableType());
    int64_t reclaimTimestamp = joinPolicy.GetReclaimRtTimestamp();
    PartitionDataPtr oldPartData = mPartitionDataHolder.Get();
    CustomOnlinePartitionReaderPtr newReader;
    PartitionDataPtr newPartitionData;
    DumpSegmentQueuePtr newDumpSegmentQueue;
    if (mDumpSegmentQueue)
    {
        newDumpSegmentQueue.reset(mDumpSegmentQueue->Clone());
        newDumpSegmentQueue->ReclaimSegments(reclaimTimestamp);
        IE_LOG(INFO, "dumpSegmentQueue reclaim segments done"); 
    }
    TableResourcePtr oldTableResource = mWriter->GetTableResource();

    try
    {
        Version lastIncVersion = oldPartData->GetOnDiskVersion();
        // will trigger relcaim rt segments
        newPartitionData = CreateCustomPartitionData(
            incVersion, lastIncVersion, newDumpSegmentQueue, reclaimTimestamp, false);
        TableResourcePtr newTableResource;
        if (!mWriter->PrepareTableResource(
                DYNAMIC_POINTER_CAST(CustomPartitionData, newPartitionData), 
                newTableResource))
        {
            if (mOptions.GetOnlineConfig().IsReopenRetryOnIOExceptionEnabled())
            {
                IE_PREFIX_LOG(ERROR, "retryOnIOException is true, will return OS_FAIL"); 
                return OS_FAIL;
            }
            IE_PREFIX_LOG(ERROR, "prepare TableResource failed");
            return OS_INDEXLIB_EXCEPTION;
        }
        IE_LOG(INFO, "create new partition data done");
        mWriter->ResetPartitionData(newPartitionData, newTableResource);
        newReader = InitReaderWithMemoryLimit(newPartitionData, mWriter, lastIncVersion, incVersion);
        IE_LOG(INFO, "init reader with  new partition data done");
    }
    catch (const FileIOException& e)
    {
        IE_PREFIX_LOG(ERROR, "catch FILEIO exception in normalReopen, mWriter will be reset");
        mWriter->ResetPartitionData(oldPartData, oldTableResource);
        if (mOptions.GetOnlineConfig().IsReopenRetryOnIOExceptionEnabled())
        {
            return OS_FAIL;
        }
        return OS_FILEIO_EXCEPTION;
    }
    catch (const ExceptionBase& e)
    {
        IE_PREFIX_LOG(ERROR, "catch Indexlib exception in normalReopen, mWriter will be reset");
        mWriter->ResetPartitionData(oldPartData, oldTableResource);
        return OS_INDEXLIB_EXCEPTION;
    }
    catch(...)
    {
        IE_PREFIX_LOG(ERROR, "catch unknown exception in normalReopen, mWriter will be reset");
        mWriter->ResetPartitionData(oldPartData, oldTableResource);
        return OS_INDEXLIB_EXCEPTION;
    }
    mDumpSegmentQueue = newDumpSegmentQueue;
    mPartitionDataHolder.Reset(newPartitionData);

    NewSegmentMetaPtr buildingSegMeta = mWriter->GetBuildingSegmentMeta();
    if (buildingSegMeta && (buildingSegMeta->segmentInfo.timestamp < reclaimTimestamp))
    {
        mWriter->DumpSegment();
        CustomPartitionDataPtr typedData = DYNAMIC_POINTER_CAST(CustomPartitionData, newPartitionData);
        typedData->RemoveObsoleteSegments();
        ReOpenNewSegment();
    }
    else
    {
        CustomPartitionDataPtr typedData = DYNAMIC_POINTER_CAST(CustomPartitionData, newPartitionData);
        typedData->RemoveObsoleteSegments();
        // SwitchReader depends on mPartitionDataHolder
        SwitchReader(newReader);
    }
    MEMORY_BARRIER();
    SetReaderHashId(mReader ? mReader->GetPartitionVersionHashId() : 0);
    ReportMetrics();
    AddOnDiskIndexCleaner();

    IE_PREFIX_LOG(INFO, "reopen end : normal reopen version[%d] success",
                  incVersion.GetVersionId());
    mLoadedIncVersion = incVersion;
    return OS_OK;
}


ReopenDecider::ReopenType CustomOnlinePartition::MakeReopenDecision(
    bool forceReopen,
    versionid_t targetVersionId,
    index_base::Version& reopenVersion)
{
    misc::ScopeLatencyReporter scopeTime(
            mOnlinePartMetrics->GetdecisionLatencyMetric().get()); 
    DirectoryPtr rootDirectory = mPartitionDataHolder.Get()->GetRootDirectory();
    VersionLoader::GetVersion(rootDirectory, reopenVersion, targetVersionId);
    Version lastIncVersion = mPartitionDataHolder.Get()->GetOnDiskVersion();
    
    if (reopenVersion.GetVersionId() <= lastIncVersion.GetVersionId())
    {
        IE_LOG(WARN, "reopen versionId [%d] <= last loaded version[%d], "
               "no need to reopen",
               reopenVersion.GetVersionId(), lastIncVersion.GetVersionId());
        reopenVersion = lastIncVersion;
        return ReopenDecider::NO_NEED_REOPEN;
    }
    if (reopenVersion.GetSchemaVersionId() != lastIncVersion.GetSchemaVersionId())
    {
        IE_LOG(WARN, "schema_version not match, "
               "[%u] in target version [%d], [%u] in current version [%d]",
               reopenVersion.GetSchemaVersionId(), reopenVersion.GetVersionId(),
               lastIncVersion.GetSchemaVersionId(),
               lastIncVersion.GetVersionId());
        return ReopenDecider::INCONSISTENT_SCHEMA_REOPEN;
    }

    if (reopenVersion.GetTimestamp() < lastIncVersion.GetTimestamp())
    {
        IE_LOG(WARN, "new version with smaller timestamp, "
               "[%ld] in target version [%d], [%ld] in current version [%d]. "
               "new version may be rollback index!",
               reopenVersion.GetTimestamp(), reopenVersion.GetVersionId(),
               lastIncVersion.GetTimestamp(),
               lastIncVersion.GetVersionId());
        return ReopenDecider::INDEX_ROLL_BACK_REOPEN;
    }    
    if (forceReopen)
    {
        return ReopenDecider::FORCE_REOPEN;
    }
    return ReopenDecider::NORMAL_REOPEN;
}
void CustomOnlinePartition::ReOpenNewSegment()
{
    IndexPhaseGuard phaseGuard(mIndexPhase, REOPEN_RT);
    IE_PREFIX_LOG(INFO, "reopen new segment begin");    
    ScopedLock lock(mDataLock);
    mWriter->ReOpenNewSegment(PartitionModifierPtr());
    UpdateReader();
    // CustomOnlinePartitionReaderPtr reader =
    //     InitReader(mWriter, mPartitionDataHolder.Get());
    // SwitchReader(reader);
    // if (mReaderUpdater)
    // {
    //     mReaderUpdater->Update(mReader);
    // }
    IE_PREFIX_LOG(INFO, "roeopen new segment end");    
}

void CustomOnlinePartition::DoClose()
{
    mClosed = true;
    mIsServiceNormal = false;

    if (mTaskScheduler)
    {
        mTaskScheduler->DeleteTask(mCleanPartitionReaderTaskId);
        mTaskScheduler->DeleteTask(mCleanResourceTaskId);
        mTaskScheduler->DeleteTask(mReportMetricsTaskId);
        mTaskScheduler->DeleteTask(mAsyncDumpTaskId);
        mTaskScheduler->DeleteTask(mCheckIndexTaskId);
    }
    ScopedLock lock(mDataLock);
    while (mReaderContainer->Size() > 1)
    {
        auto unReleasedReader = mReaderContainer->PopOldestReader();
        if (unReleasedReader && unReleasedReader.use_count() > 1)
        {
            IE_LOG(ERROR, "unreleased reader [version: %d] exists"
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
    if (mReader)
    {
        mReader.reset();
    }
    if (mWriter)
    {
        mWriter->Close();
    }
    mWriter.reset();
    FlushDumpSegmentQueue();
    mDumpSegmentQueue->Clear();
    mPartitionDataHolder.Reset();
    if (mFileSystem)
    {
        mFileSystem->Sync(true);
    }
    mTableFactory.reset();
    mPluginManager.reset();
    
    mFileSystem.reset();
    mRtMemQuotaSynchronizer.reset();
    mTableWriterMemController.reset();    
    IndexPartition::Close();
}

PartitionWriterPtr CustomOnlinePartition::GetWriter() const
{
    ScopedLock lock(mDataLock);    
    return mWriter;
}

IndexPartitionReaderPtr CustomOnlinePartition::GetReader() const
{
    ScopedLock lock(mReaderLock);
    return mReader;
}

void CustomOnlinePartition::SetForceSeekInfo(const std::pair<int64_t, int64_t>& forceSeekInfo)
{
    ScopedLock lock(mReaderLock);
    mReader->SetForceSeekInfo(forceSeekInfo);
    IndexPartition::SetForceSeekInfo(forceSeekInfo);
}

int64_t CustomOnlinePartition::GetRtSeekTimestampFromIncVersion() const
{
    OnlineJoinPolicy joinPolicy(mLoadedIncVersion, mSchema->GetTableType());
    return joinPolicy.GetRtSeekTimestampFromIncVersion();    
}

bool CustomOnlinePartition::CleanIndexFiles(const vector<versionid_t>& keepVersionIds)
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

void CustomOnlinePartition::SwitchReader(const CustomOnlinePartitionReaderPtr& reader)
{
    {
        ScopedLock lock(mReaderLock);
        mReader = reader;
        MEMORY_BARRIER();
        SetReaderHashId(mReader ? mReader->GetPartitionVersionHashId() : 0);        
    }
    mReaderContainer->AddReader(reader);
    mResourceCalculator->CalculateCurrentIndexSize(mPartitionDataHolder.Get(), mSchema); 
}

IndexPartition::MemoryStatus CustomOnlinePartition::CheckMemoryStatus() const
{
    if (!mResourceCalculator)
    {
        return MS_OK;
    }
    
    const OnlineConfig& onlineConfig = mOptions.GetOnlineConfig();
    int64_t maxRtIndexSizeInMB = onlineConfig.maxRealtimeMemSize;

    int64_t rtIndexMemUseInMB
        = (mResourceCalculator->GetRtIndexMemoryUse() + mDumpSegmentQueue->GetEstimateDumpSize()
           + mTableWriterMemController->GetUsedQuota()) / (1024 * 1024);

    // TODO: delete oldest rtSegments to release memory
    if (rtIndexMemUseInMB >= maxRtIndexSizeInMB)
    {  
        IE_PREFIX_LOG(WARN, "Reach max Rt index size[%ld MB], "
                      "current rtIndexSize is %ld MB", 
                      maxRtIndexSizeInMB, rtIndexMemUseInMB);
        return MS_REACH_MAX_RT_INDEX_SIZE;
    }
    if (mRtMemQuotaSynchronizer && mRtMemQuotaSynchronizer->GetFreeQuota() <= 0)
    {
        IE_PREFIX_LOG(WARN, "Reach realtime quota control limit, current free quota[%lu]",
                      mRtMemQuotaSynchronizer->GetFreeQuota());
        return MS_REACH_MAX_RT_INDEX_SIZE;
    }
    int64_t availableMemoryUse = mPartitionMemController->GetFreeQuota() / (1024 * 1024);
    uint64_t buildingSegmentDumpExpandMemUse =
        mResourceCalculator->GetBuildingSegmentDumpExpandSize() / (1024 * 1024);

    if (availableMemoryUse <= (int64_t)buildingSegmentDumpExpandMemUse)
    {
        int64_t currentMemUse =
            mResourceCalculator->GetCurrentMemoryUse() / (1024 * 1024);
        IE_PREFIX_LOG(WARN, "Reach memory use limit free mem [%ld MB], "
                      "current total memory is %ld MB, "//maxReopenMemoryUse [%ld MB] "
                      "dumpExpandMemUse [%lu MB]", 
                      availableMemoryUse, currentMemUse,
                      /*maxReopenMemoryUse,*/ buildingSegmentDumpExpandMemUse);
        return MS_REACH_TOTAL_MEM_LIMIT;
    }
    return MS_OK;    
}

void CustomOnlinePartition::InitResourceCleaner()
{
    mResourceCleaner.reset(new ExecutorScheduler);

    ExecutorPtr inMemoryIndexCleaner(
        new InMemoryIndexCleaner(mReaderContainer,
                                 mPartitionDataHolder.Get()->GetRootDirectory()));
    mResourceCleaner->Add(inMemoryIndexCleaner, ExecutorScheduler::ST_REPEATEDLY);
    AddOnDiskIndexCleaner();
}

void CustomOnlinePartition::AddOnDiskIndexCleaner()
{
    // TODO: In future, suez will in controll of local index clean
    char* envStr = getenv("INDEXLIB_ONLINE_CLEAN_ON_DISK_INDEX");
    if (envStr && std::string(envStr) == "false")
    {
        return;
    }
    ExecutorPtr onDiskIndexCleaner(new OnDiskIndexCleaner(
                    mOptions.GetOnlineConfig().onlineKeepVersionCount,
                    mReaderContainer, GetRootDirectory()));
    mResourceCleaner->Add(onDiskIndexCleaner, ExecutorScheduler::ST_ONCE);    
}

void CustomOnlinePartition::ExecuteCleanResourceTask()
{
    if (mClosed)
    {
        return;
    }
    
    if (mCleanerLock.trylock() != 0)
    {
        return;
    }

    if (mDataLock.trylock() != 0)
    {
        mCleanerLock.unlock();
        return;
    }

    try
    {
        CleanResource();
    }
    catch(ExceptionBase& e)
    {
        IE_PREFIX_LOG(ERROR, "Clean resource exception [%s]!!", e.what());
    }
    catch(...)
    {
        IE_PREFIX_LOG(ERROR, "Clean resource exception!!");
    }    

    mDataLock.unlock();
    mCleanerLock.unlock();    
}

void CustomOnlinePartition::CleanResource()
{
    ScopedLock lock(mCleanerLock);
    if (!mIsServiceNormal)
    {
        return;
    }
    
    mResourceCleaner->Execute();    
}

void CustomOnlinePartition::ExecuteCleanPartitionReaderTask()
{
    if (mClosed)
    {
        return;
    }
    
    try
    {
        CleanPartitionReaders();
    }
    catch(ExceptionBase& e)
    {
        IE_PREFIX_LOG(ERROR, "Clean partition reader exception [%s]!!", e.what());
    }
    catch(...)
    {
        IE_PREFIX_LOG(ERROR, "Clean partition reader exception!!");
    }    
}

void CustomOnlinePartition::CleanPartitionReaders() {
    if (!mIsServiceNormal)
    {
        return;
    }
    auto doSomething = mReaderContainer->EvictOldReaders();
    if (doSomething)
    {
        mFileSystem->CleanCache();
    }
}

void CustomOnlinePartition::ReportMetrics()
{
    UpdateOnlinePartitionMetrics();
    mOnlinePartMetrics->ReportMetrics();
    mOnlinePartMetrics->PrintMetrics(mOptions.GetOnlineConfig(), mPartitionName);
    mFileSystem->ReportMetrics();
    if (mWriter)
    {
        mWriter->ReportMetrics();        
    }
}

void CustomOnlinePartition::UpdateOnlinePartitionMetrics()
{
    if (!mResourceCalculator)
    {
        return;
    }
    size_t builtRtIndexMemoryUse = mResourceCalculator->GetRtIndexMemoryUse();
    size_t writerMemoryUse = mTableWriterMemController->GetUsedQuota();
    size_t rtIndexMemoryUse = builtRtIndexMemoryUse + writerMemoryUse + mDumpSegmentQueue->GetEstimateDumpSize(); 
    if (mRtMemQuotaSynchronizer)
    {
        mRtMemQuotaSynchronizer->SyncMemoryQuota(rtIndexMemoryUse);
    }

    IndexPartition::MemoryStatus memoryStatus = CheckMemoryStatus();
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
    mOnlinePartMetrics->SetoldInMemorySegmentMemoryUseValue(writerMemoryUse);
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

void CustomOnlinePartition::DoInitPathMetaCache(const DirectoryPtr& deployMetaDir, const Version& version)
{
    IndexFileList deployMeta;
    const string& deployMetaFileName = DeployIndexWrapper::GetDeployMetaFileName(version.GetVersionId());
    deployMeta.Load(deployMetaDir, deployMetaFileName);
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
    }; 
}


void CustomOnlinePartition::InitPathMetaCache(const DirectoryPtr& deployMetaDir,
                                        const Version& version)
{
    size_t retryCounter = 0;
    if (!mOptions.GetBuildConfig(true).usePathMetaCache)
    {
        return;
    }
    while (true)
    {
        try
        {
            DoInitPathMetaCache(deployMetaDir, version);
            return;
        }
        catch(const FileIOException& e)
        {
            if (retryCounter >= mIOExceptionRetryLimit)
            {
                IE_LOG(ERROR, "reach IOException[%s] Limit[%lu] while init PathMetaCache for partition[%s]",
                       e.GetMessage().c_str(), mIOExceptionRetryLimit, mPartitionName.c_str()); 
                throw;
            }
            else
            {
                retryCounter++;
                IE_LOG(WARN, "raise IOException[%s] while init PathMetaCache for"
                       " partition[%s], trigger retry",
                       e.GetMessage().c_str(), mPartitionName.c_str()); 
                usleep(mIOExceptionRetryInterval);
                continue;
            }
        }        
        catch(...)
        {
            IE_LOG(ERROR, "init reader failed for partition[%s] when load version[%d]",
                   mPartitionName.c_str(), version.GetVersionId());
            throw;
        }                
    }
}

bool CustomOnlinePartition::PrepareIntervalTask()
{
    if (!mTaskScheduler)
    {
        IE_LOG(ERROR, "TaskScheduler is NULL");
        return false;
    }
    
    int32_t sleepTime = INDEXLIB_REPORT_METRICS_INTERVAL;
    if (unlikely(mOptions.TEST_mQuickExit))
    {
        sleepTime /= 1000;
    }
    if (mOptions.GetOnlineConfig().enableAsyncCleanResource)
    {
        TaskItemPtr cleanResourceTask(
            new MemberFunctionTaskItem<CustomOnlinePartition, &CustomOnlinePartition::ExecuteCleanResourceTask>(*this));
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

        TaskItemPtr cleanPartitionReaderTask(new MemberFunctionTaskItem<CustomOnlinePartition,
            &CustomOnlinePartition::ExecuteCleanPartitionReaderTask>(*this));
        if (!mTaskScheduler->DeclareTaskGroup("clean_reader", sleepTime))
        {
            IE_PREFIX_LOG(ERROR, "declare clean partition reader task failed!");
            return false;
        }
        mCleanPartitionReaderTaskId = mTaskScheduler->AddTask("clean_reader", cleanPartitionReaderTask);
        if (mCleanPartitionReaderTaskId == TaskScheduler::INVALID_TASK_ID)
        {
            IE_PREFIX_LOG(ERROR, "add clean partition reader task failed!");
            return false;
        }
    }
    TaskItemPtr reportMetricsTask(
        new MemberFunctionTaskItem<CustomOnlinePartition, &CustomOnlinePartition::ReportMetrics>(*this));

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

    if (mOptions.GetOnlineConfig().enableAsyncDumpSegment)
    {
        if (!mTaskScheduler->DeclareTaskGroup("async_dump", sleepTime))
        {
            IE_PREFIX_LOG(ERROR, "declare async dump group failed");
            return false;
        }
        TaskItemPtr asyncDumpTask(
            new MemberFunctionTaskItem<CustomOnlinePartition, &CustomOnlinePartition::AsyncDumpSegment>(*this));
        mAsyncDumpTaskId = mTaskScheduler->AddTask("async_dump", asyncDumpTask);
        if (mAsyncDumpTaskId == TaskScheduler::INVALID_TASK_ID)
        {
            IE_PREFIX_LOG(ERROR, "add async dump task failed");
            return false;
        }
    }
    if (NeedCheckSecondaryIndex())
    {
        if (!mTaskScheduler->DeclareTaskGroup("check_index",
                                              mCheckSecondaryIndexIntervalInMin * 60 * 1000 * 1000))
        {
            IE_PREFIX_LOG(ERROR, "declare check_index task group failed");
            return false;
        }
        TaskItemPtr checkSecondaryIndexTask(
            new MemberFunctionTaskItem<CustomOnlinePartition, &CustomOnlinePartition::CheckSecondaryIndex>(*this));
        mCheckIndexTaskId = mTaskScheduler->AddTask("check_index", checkSecondaryIndexTask);        
        if (mCheckIndexTaskId == TaskScheduler::INVALID_TASK_ID)
        {
            IE_PREFIX_LOG(ERROR, "add check_index task failed!");
            return false;
        }
    }
    return true;
}

bool CustomOnlinePartition::TryLock(autil::ThreadMutex& lock, size_t times, int64_t intervalInUs)
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

void CustomOnlinePartition::CheckSecondaryIndex()
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

void CustomOnlinePartition::AsyncDumpSegment()
{
    if (mClosed || !mDumpSegmentQueue)
    {
        return;
    }
    if (!mAsyncDumpFuture.valid())
    {
        mAsyncDumpFuture = std::move(std::async(std::launch::async,
                                                &CustomOnlinePartition::FlushDumpSegmentQueue, this));
        return;
    }
    std::chrono::milliseconds span(10);
    auto waitRet = mAsyncDumpFuture.wait_for(span);
    if (waitRet == std::future_status::timeout)
    {
        return;
    }
    assert(waitRet == std::future_status::ready);
    mAsyncDumpFuture = std::move(std::async(std::launch::async,
                                            &CustomOnlinePartition::FlushDumpSegmentQueue, this));
    return;    
}

void CustomOnlinePartition::FlushDumpSegmentQueue()
{
    int64_t latency = 0;
    uint32_t dumpCounter = 0;
    {
        ScopedTime timer(latency);    
        while (mWriter || !mClosed || mDumpSegmentQueue)
        {
            if (mDumpSegmentQueue->Size() == 0)
            {
                break;
            }
            bool hasDumpedSegment = false;
            bool dumpSuccess = true;
            {
                ScopedLock lock(mCleanerLock);
                dumpSuccess = mDumpSegmentQueue->ProcessOneItem(hasDumpedSegment);
                if (!dumpSuccess)
                {
                    IE_PREFIX_LOG(ERROR, "dump segment failed!");
                    return;
                }
                if (hasDumpedSegment)
                {
                    dumpCounter++;
                    mPartitionDataHolder.Get()->CommitVersion();
                    mDumpSegmentQueue->PopDumpedItems();
                }
                if (!mClosed && hasDumpedSegment)
                {
                    try
                    {
                        UpdateReader();
                    }
                    catch (const misc::ExceptionBase& e)
                    {
                        IE_LOG(ERROR, "Async dump segment thread end. "
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
    }
    if (dumpCounter > 0)
    {
        IE_PREFIX_LOG(INFO, "flush dump segment items done , total count [%u], time elapsed [%ld ms].",
                      dumpCounter, latency / 1000); 
    }
}

void CustomOnlinePartition::UpdateReader()
{
    ScopedLock lock(mDataLock);
    PartitionDataPtr clonedPartitionData(mPartitionDataHolder.Get()->Clone());
    CustomOnlinePartitionReaderPtr reader;
    if (!mSupportSegmentLevelReader)
    {
        reader = InitReader(mWriter, clonedPartitionData);
    }
    else
    {
        CustomOnlinePartitionReaderPtr nullPartReader;
        reader = InitReaderWithHeritage(mWriter, clonedPartitionData, mReader, nullPartReader);
    }
    SwitchReader(reader);
    if (mReaderUpdater)
    {
        mReaderUpdater->Update(mReader);
    }
}


IE_NAMESPACE_END(partition);

