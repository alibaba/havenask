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
#include "indexlib/partition/custom_online_partition.h"

#include <chrono>
#include <future>

#include "autil/EnvUtil.h"
#include "autil/HashAlgorithm.h"
#include "indexlib/common/executor_scheduler.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/IndexFileList.h"
#include "indexlib/file_system/MountOption.h"
#include "indexlib/file_system/RemoveOption.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/index_base/lifecycle_strategy.h"
#include "indexlib/index_base/online_join_policy.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/partition/custom_online_partition_reader.h"
#include "indexlib/partition/custom_online_partition_writer.h"
#include "indexlib/partition/custom_partition_data.h"
#include "indexlib/partition/deploy_index_validator.h"
#include "indexlib/partition/in_memory_index_cleaner.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/partition/index_partition_resource.h"
#include "indexlib/partition/local_index_cleaner.h"
#include "indexlib/partition/memory_stat_collector.h"
#include "indexlib/partition/memory_stat_reporter.h"
#include "indexlib/partition/on_disk_index_cleaner.h"
#include "indexlib/partition/on_disk_realtime_data_calculator.h"
#include "indexlib/partition/online_partition_metrics.h"
#include "indexlib/partition/open_executor/open_executor_chain.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/partition/partition_reader_cleaner.h"
#include "indexlib/partition/partition_resource_calculator.h"
#include "indexlib/partition/reader_container.h"
#include "indexlib/partition/reopen_decider.h"
#include "indexlib/partition/segment/dump_segment_queue.h"
#include "indexlib/table/executor_manager.h"
#include "indexlib/table/table_factory_wrapper.h"
#include "indexlib/table/table_plugin_loader.h"
#include "indexlib/table/table_reader.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/Singleton.h"
#include "indexlib/util/TaskScheduler.h"
#include "indexlib/util/counter/CounterMap.h"
#include "indexlib/util/member_function_task_item.h"
#include "indexlib/util/memory_control/BlockMemoryQuotaController.h"
#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"
#include "indexlib/util/memory_control/MemoryQuotaSynchronizer.h"
#include "indexlib/util/memory_control/MemoryReserver.h"
#include "indexlib/util/params_util.h"

using namespace std;
using namespace autil;

using namespace indexlib::index;
using namespace indexlib::file_system;
using namespace indexlib::util;
using namespace indexlib::config;
using namespace indexlib::common;
using namespace indexlib::table;
using namespace indexlib::file_system;
using namespace indexlib::index_base;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, CustomOnlinePartition);

#define IE_LOG_PREFIX mPartitionName.c_str()

size_t CustomOnlinePartition::DEFAULT_IO_EXCEPTION_RETRY_LIMIT = 3u;
size_t CustomOnlinePartition::DEFAULT_IO_EXCEPTION_RETRY_INTERVAL = 1000000; // 1 second
size_t CustomOnlinePartition::DEFAULT_CHECK_RT_BUILD_DUMP_LIMIT = 3u;
size_t CustomOnlinePartition::DEFAULT_LOW_MEM_USED_WATERMARK = 60;
size_t CustomOnlinePartition::DEFAULT_HIGH_MEM_USED_WATERMARK = 90;

CustomOnlinePartition::CustomOnlinePartition(const IndexPartitionResource& partitionResource)
    : IndexPartition(partitionResource)
    , mClosed(true)
    , mIsServiceNormal(false)
    , mIndexPhase(PENDING)
    , mOnlinePartMetrics(new OnlinePartitionMetrics(partitionResource.metricProvider))
    , mCleanPartitionReaderTaskId(TaskScheduler::INVALID_TASK_ID)
    , mCleanResourceTaskId(TaskScheduler::INVALID_TASK_ID)
    , mReportMetricsTaskId(TaskScheduler::INVALID_TASK_ID)
    , mRefreshReaderTaskId(TaskScheduler::INVALID_TASK_ID)
    , mCheckIndexTaskId(TaskScheduler::INVALID_TASK_ID)
    , mCheckRealtimeBuildTaskId(TaskScheduler::INVALID_TASK_ID)
    , mCheckSecondaryIndexIntervalInMin(EnvUtil::getEnv<int64_t>("SECONDARY_INDEX_CHECK_INTERVAL", -1))
    , mHighMemUsedWatermark(DEFAULT_HIGH_MEM_USED_WATERMARK)
    , mLowMemUsedWatermark(DEFAULT_LOW_MEM_USED_WATERMARK)
    , mIOExceptionRetryLimit(DEFAULT_IO_EXCEPTION_RETRY_LIMIT)
    , mIOExceptionRetryInterval(DEFAULT_IO_EXCEPTION_RETRY_INTERVAL)
    , mMissingSegmentCount(0)
    , mCheckRtBuildDumpLimit(DEFAULT_CHECK_RT_BUILD_DUMP_LIMIT)
    , mSupportSegmentLevelReader(false)
    , mNeedCheckRealTimeBuild(true)
    , mEnableAsyncDump(false)
{
    if (partitionResource.memoryStatReporter) {
        MemoryStatCollectorPtr collector = partitionResource.memoryStatReporter->GetMemoryStatCollector();
        if (collector) {
            collector->AddTableMetrics(partitionResource.partitionGroupName, mOnlinePartMetrics);
        }
    }
    mRealtimeQuotaController = partitionResource.realtimeQuotaController;
    mPartitionIdentifier = autil::HashAlgorithm::hashString64(mPartitionName.c_str(), mPartitionName.length());
}

CustomOnlinePartition::~CustomOnlinePartition()
{
    try {
        Close();
    } catch (const ExceptionBase& e) {
        IE_PREFIX_LOG(ERROR, "ERROR occurred in OnlinePartition deconstructor[%s]", e.what());
    }
    Reset();
}

string CustomOnlinePartition::GetLastLocator() const
{
    string locator;
    if (mWriter) {
        locator = mWriter->GetLastLocator();
    }
    if (!locator.empty()) {
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
    if (mClosed) {
        return;
    }
    try {
        DoClose();
    } catch (const ExceptionBase& e) {
        throw;
    }
}

IndexPartition::OpenStatus CustomOnlinePartition::Open(const std::string& primaryDir, const std::string& secondaryDir,
                                                       const config::IndexPartitionSchemaPtr& schema,
                                                       const config::IndexPartitionOptions& options,
                                                       versionid_t targetVersionId)
{
    IndexPartition::OpenStatus os = OS_FAIL;
    try {
        const std::string& sourceRoot =
            (options.GetOnlineConfig().NeedReadRemoteIndex() && !secondaryDir.empty()) ? secondaryDir : primaryDir;
        os = DoOpen(primaryDir, sourceRoot, schema, options, targetVersionId, false);
        if (os != OS_OK) {
            IE_LOG(ERROR, "open partition failed. Partition is to be closed");
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
    return os;
}

IndexPartition::OpenStatus CustomOnlinePartition::DoOpen(const string& primaryDir, const string& secondaryDir,
                                                         const IndexPartitionSchemaPtr& schema,
                                                         const IndexPartitionOptions& options,
                                                         versionid_t targetVersionId, bool isForceOpen)
{
    IE_PREFIX_LOG(INFO, "Open Index Partition: primary[%s], secondary[%s]", primaryDir.c_str(), secondaryDir.c_str());
    IndexPhaseGuard phaseGuard(mIndexPhase, OPENING);
    mReaderContainer.reset(new ReaderContainer);
    if (!mDumpSegmentQueue) {
        mDumpSegmentQueue.reset(new DumpSegmentQueue());
    }
    mEnableAsyncDump =
        (options.GetOnlineConfig().enableAsyncDumpSegment || options.GetBuildConfig(true).EnableAsyncDump());
    int64_t ioExceptionRetryLimit = autil::EnvUtil::getEnv<int64_t>("CUSTOM_IO_EXCEPTION_RETRY_LIMIT", -1);
    int64_t ioExceptionRetryInterval = autil::EnvUtil::getEnv<int64_t>("CUSTOM_IO_EXCEPTION_RETRY_INTERVAL", -1);
    if (ioExceptionRetryLimit != -1) {
        mIOExceptionRetryLimit = ioExceptionRetryLimit;
    }
    if (ioExceptionRetryInterval != -1) {
        mIOExceptionRetryInterval = ioExceptionRetryInterval;
    }
    IE_LOG(INFO, "IO exception retry limit = [%zu], retry interval = [%zu]", mIOExceptionRetryLimit,
           mIOExceptionRetryInterval);

    auto customizedParams = options.GetBuildConfig(true).GetCustomizedParams();
    int64_t checkRtBuildDumpLimit = ParamsUtil::GetParam<int64_t>("check_rt_build_dump_limit", customizedParams, -1);
    int64_t highMemUsedWatermark = ParamsUtil::GetParam<int64_t>("high_mem_used_watermark", customizedParams, -1);
    int64_t lowMemUsedWatermark = ParamsUtil::GetParam<int64_t>("low_mem_used_watermark", customizedParams, -1);
    mNeedCheckRealTimeBuild = ParamsUtil::GetParam<bool>("need_check_rt_build", customizedParams, true);
    if (checkRtBuildDumpLimit != -1) {
        mCheckRtBuildDumpLimit = max(int64_t(0), checkRtBuildDumpLimit);
    }
    if (highMemUsedWatermark != -1) {
        mHighMemUsedWatermark = highMemUsedWatermark;
    }
    if (lowMemUsedWatermark != -1) {
        mLowMemUsedWatermark = lowMemUsedWatermark;
    }
    IE_LOG(INFO,
           "Check rt build dump limit = [%zu], "
           "high mem used watermark = [%zu], low mem used watermark = [%zu]",
           mCheckRtBuildDumpLimit, mHighMemUsedWatermark, mLowMemUsedWatermark);
    IndexPartition::OpenStatus openStatus =
        IndexPartition::Open(primaryDir, secondaryDir, schema, options, targetVersionId);

    if (openStatus != OS_OK) {
        return openStatus;
    }
    mReaderContainer.reset(new ReaderContainer);
    if (!mRealtimeQuotaController) {
        mRealtimeQuotaController.reset(
            new MemoryQuotaController(options.GetOnlineConfig().maxRealtimeMemSize * 1024 * 1024));
    }
    mRtMemQuotaSynchronizer = CreateRealtimeMemoryQuotaSynchronizer(mRealtimeQuotaController);
    mClosed = true;

    Version onDiskVersion;
    VersionLoader::GetVersionS(secondaryDir, onDiskVersion, targetVersionId);

    auto versionDpDesc =
        CreateVersionDeployDescription(primaryDir, mOptions.GetOnlineConfig(), onDiskVersion.GetVersionId());
    if (versionDpDesc == nullptr) {
        string schemaName = (schema == nullptr) ? "NULL-schema" : schema->GetSchemaName();
        IE_LOG(ERROR, "create version deploy description failed in table[%s], versionId[%d]", schemaName.c_str(),
               static_cast<int>(onDiskVersion.GetVersionId()));
        return IndexPartition::OS_FAIL;
    }
    auto versionLevelLifecycleTable = versionDpDesc->GetLifecycleTable();
    if (versionLevelLifecycleTable == nullptr) {
        IE_LOG(ERROR, "get lifecycle Table from versionDpDesc error, versionId[%d]",
               static_cast<int>(onDiskVersion.GetVersionId()));
        return IndexPartition::OS_FAIL;
    }
    if (unlikely(onDiskVersion.GetVersionId() == INVALID_VERSIONID)) {
        const std::string& root = secondaryDir;
        THROW_IF_FS_ERROR(mFileSystem->MountFile(root, SCHEMA_FILE_NAME, SCHEMA_FILE_NAME, FSMT_READ_ONLY, -1, false),
                          "mount schema file failed");
        THROW_IF_FS_ERROR(mFileSystem->MountFile(root, INDEX_FORMAT_VERSION_FILE_NAME, INDEX_FORMAT_VERSION_FILE_NAME,
                                                 FSMT_READ_ONLY, -1, false),
                          "mount index format version file failed");
    } else {
        if (!mOptions.GetOnlineConfig().NeedReadRemoteIndex()) {
            THROW_IF_FS_ERROR(mFileSystem->MountVersion(primaryDir, onDiskVersion.GetVersionId(), "", FSMT_READ_ONLY,
                                                        versionLevelLifecycleTable),
                              "mount version failed");
        } else {
            THROW_IF_FS_ERROR(mFileSystem->MountVersion(secondaryDir, onDiskVersion.GetVersionId(), "", FSMT_READ_ONLY,
                                                        versionLevelLifecycleTable),
                              "mount version failed");
        }
    }
    ScopedLock lock(mDataLock);
    ScopedLock schemaLock(mSchemaLock);
    IE_PREFIX_LOG(INFO, "current free memory is %ld MB", mPartitionMemController->GetFreeQuota() / (1024 * 1024));

    // rt build disable package file
    mOptions.GetBuildConfig(true).enablePackageFile = false;

    mSchema = SchemaAdapter::LoadSchema(GetFileSystemRootDirectory(), onDiskVersion.GetSchemaVersionId());
    CheckParam(mSchema, mOptions);
    if (schema) {
        mSchema->AssertCompatible(*schema);
    }
    mTableWriterMemController.reset(
        new BlockMemoryQuotaController(mPartitionMemController, mSchema->GetSchemaName() + "_TableWriterMemory"));

    mOnlinePartMetrics->RegisterMetrics(mSchema->GetTableType());
    mCounterMap.reset(new CounterMap());
    if (mOptions.GetOnlineConfig().loadRemainFlushRealtimeIndex && !mOptions.TEST_mReadOnly) {
        // first open will recover rt index
        mOptions.GetOnlineConfig().enableRecoverIndex = true;
    }
    mPluginManager = TablePluginLoader::Load(mIndexPluginPath, mSchema, mOptions);
    if (!mPluginManager) {
        IE_LOG(ERROR, "load index plugin failed for schema [%s]", mSchema->GetSchemaName().c_str());
        return OS_FAIL;
    }
    if (!mTableFactory) {
        mTableFactory.reset(new TableFactoryWrapper(mSchema, mOptions, mPluginManager));
        if (!mTableFactory->Init()) {
            IE_LOG(ERROR, "Init TableFactory failed in table[%s]", mSchema->GetSchemaName().c_str());
            return OS_FAIL;
        }
    }
    PartitionDataPtr newPartitionData =
        CreateCustomPartitionData(onDiskVersion, index_base::Version(INVALID_VERSIONID), versionDpDesc,
                                  mDumpSegmentQueue, GetReclaimTimestamp(onDiskVersion), false);
    mPartitionDataHolder.Reset(newPartitionData);
    // only recover once when open
    mOptions.GetOnlineConfig().enableRecoverIndex = false;

    mFileSystem->SwitchLoadSpeedLimit(
        mOptions.GetOnlineConfig().enableLoadSpeedControlForOpen); // true for on, false for off

    if (!mOptions.TEST_mReadOnly) {
        mWriter.reset(new CustomOnlinePartitionWriter(mOptions, mTableFactory, mFlushedLocatorContainer,
                                                      mOnlinePartMetrics.get(), mPartitionName));
        InitWriter(mPartitionDataHolder.Get(), mWriter);
    }
    InitResourceCalculator(mPartitionDataHolder.Get(), mWriter);
    PartitionDataPtr clonedPartitionData(mPartitionDataHolder.Get()->Clone());
    auto reader =
        InitReaderWithMemoryLimit(clonedPartitionData, mWriter, index_base::Version(INVALID_VERSIONID), onDiskVersion);

    if (reader) {
        auto tableReader = reader->GetTableReader();
        mSupportSegmentLevelReader = tableReader->SupportSegmentLevelReader();
    }
    // remove inc covered segment
    CustomPartitionDataPtr typedData = DYNAMIC_POINTER_CAST(CustomPartitionData, newPartitionData);
    typedData->RemoveObsoleteSegments();

    SwitchReader(reader);
    MEMORY_BARRIER();

    mFileSystem->SwitchLoadSpeedLimit(true);
    mLoadedIncVersion = mPartitionDataHolder.Get()->GetOnDiskVersion();

    InitResourceCleaner();
    CleanPartitionReaders();
    CleanResource();
    ReportMetrics();

    if (!mReaderUpdater) {
        mReaderUpdater.reset(new SearchReaderUpdater(mSchema->GetSchemaName()));
    }

    if (!PrepareIntervalTask()) {
        return OS_FAIL;
    }
    mClosed = false;
    mIsServiceNormal = true;
    IE_PREFIX_LOG(INFO, "Open Index Partition End");
    return OS_OK;
}

void CustomOnlinePartition::InitResourceCalculator(const PartitionDataPtr& partData,
                                                   const CustomOnlinePartitionWriterPtr& writer)
{
    DirectoryPtr rootDirectory = partData->GetRootDirectory();
    mResourceCalculator.reset(new PartitionResourceCalculator(mOptions.GetOnlineConfig()));
    mResourceCalculator->Init(rootDirectory, writer, InMemorySegmentContainerPtr(), mPluginManager);
}

CustomOnlinePartitionReaderPtr CustomOnlinePartition::InitReaderWithMemoryLimit(
    const PartitionDataPtr& partData, const CustomOnlinePartitionWriterPtr& writer,
    const index_base::Version& lastIncVersion, const index_base::Version& newIncVersion)
{
    assert(mResourceCalculator);
    CustomOnlinePartitionReaderPtr reader;
    DirectoryPtr rootDirectory = partData->GetRootDirectory();
    size_t retryCounter = 0;
    while (true) {
        try {
            // TODO: delete oldest rt segments to release memory
            size_t readerEstimateMemUse = mResourceCalculator->EstimateDiffVersionLockSizeWithoutPatch(
                mSchema, rootDirectory, newIncVersion, lastIncVersion);
            const MemoryReserverPtr memReserver = mFileSystem->CreateMemoryReserver("reopen_partition_reader_executor");
            if (!memReserver->Reserve(readerEstimateMemUse)) {
                INDEXLIB_FATAL_ERROR(OutOfMemory,
                                     "load reader fail, "
                                     "incExpandMemSize [%luB] over FreeQuota [%ldB]",
                                     readerEstimateMemUse, memReserver->GetFreeQuota());
            }
            return InitReader(writer, partData);
        } catch (const FileIOException& e) {
            if (retryCounter >= mIOExceptionRetryLimit) {
                IE_LOG(ERROR,
                       "reach IOException[%s] Limit[%lu] while init reader for partition[%s], "
                       "load version[%d]",
                       e.GetMessage().c_str(), mIOExceptionRetryLimit, mPartitionName.c_str(),
                       newIncVersion.GetVersionId());
                throw;
            } else {
                retryCounter++;
                IE_LOG(WARN,
                       "raise IOException[%s] while init reader for partition[%s], "
                       "load version[%d], trigger retry",
                       e.GetMessage().c_str(), mPartitionName.c_str(), newIncVersion.GetVersionId());
                usleep(mIOExceptionRetryInterval);
                continue;
            }
        } catch (...) {
            IE_LOG(ERROR, "init reader failed for partition[%s] when load version[%d]", mPartitionName.c_str(),
                   newIncVersion.GetVersionId());
            throw;
        }
    }
    return reader;
}

PartitionDataPtr CustomOnlinePartition::CreateCustomPartitionData(
    const index_base::Version& incVersion, const index_base::Version& lastIncVersion,
    const indexlibv2::framework::VersionDeployDescriptionPtr& versionDpDesc,
    const DumpSegmentQueuePtr& dumpSegmentQueue, int64_t reclaimTimestamp, bool ignoreInMemVersion) const
{
    size_t retryCounter = 0;
    while (true) {
        try {
            PartitionDataPtr newPartitionData = PartitionDataCreator::CreateCustomPartitionData(
                mFileSystem, mSchema, mOptions, mPartitionMemController, mTableWriterMemController, incVersion,
                versionDpDesc, mMetricProvider, mCounterMap, mPluginManager, dumpSegmentQueue, reclaimTimestamp,
                ignoreInMemVersion);

            if (mOptions.GetOnlineConfig().IsValidateIndexEnabled()) {
                DeployIndexValidator::ValidateDeploySegments(newPartitionData->GetRootDirectory(),
                                                             incVersion - lastIncVersion);
            }
            return newPartitionData;
        } catch (const FileIOException& e) {
            if (retryCounter >= mIOExceptionRetryLimit) {
                IE_LOG(ERROR, "reach IOException[%s] Limit[%lu] while init PartitionData for partition[%s]",
                       e.GetMessage().c_str(), mIOExceptionRetryLimit, mPartitionName.c_str());
                throw;
            } else {
                retryCounter++;
                IE_LOG(WARN, "raise IOException[%s] while init PartitionData for partition[%s], trigger retry",
                       e.GetMessage().c_str(), mPartitionName.c_str());
                usleep(mIOExceptionRetryInterval);
                continue;
            }
        } catch (...) {
            IE_LOG(ERROR, "init PartitionData failed for partition[%s]", mPartitionName.c_str());
            throw;
        }
    }
}

void CustomOnlinePartition::InitWriter(const PartitionDataPtr& partData, CustomOnlinePartitionWriterPtr& writer)
{
    assert(writer);
    size_t retryCounter = 0;
    while (true) {
        try {
            writer->Open(mSchema, partData, PartitionModifierPtr());
            return;
        } catch (const FileIOException& e) {
            if (retryCounter >= mIOExceptionRetryLimit) {
                IE_LOG(ERROR, "reach IOException[%s] Limit[%lu] while init writer for partition[%s]",
                       e.GetMessage().c_str(), mIOExceptionRetryLimit, mPartitionName.c_str());
                throw;
            } else {
                retryCounter++;
                IE_LOG(WARN, "raise IOException[%s] while init writer for partition[%s], trigger retry",
                       e.GetMessage().c_str(), mPartitionName.c_str());
                usleep(mIOExceptionRetryInterval);
                continue;
            }
        } catch (...) {
            IE_LOG(ERROR, "init writer failed for partition[%s]", mPartitionName.c_str());
            throw;
        }
    }
}

CustomOnlinePartitionReaderPtr CustomOnlinePartition::InitReader(const CustomOnlinePartitionWriterPtr& writer,
                                                                 const PartitionDataPtr& partitionData)
{
    CustomOnlinePartitionReaderPtr reader;
    if (writer) {
        reader.reset(new CustomOnlinePartitionReader(mSchema, mOptions, mPartitionName, mTableFactory,
                                                     writer->GetTableWriter(), INVALID_SEGMENTID));
    } else {
        TableWriterPtr nullWriter;
        reader.reset(new CustomOnlinePartitionReader(mSchema, mOptions, mPartitionName, mTableFactory, nullWriter,
                                                     INVALID_SEGMENTID));
    }
    reader->SetForceSeekInfo(mForceSeekInfo);
    reader->Open(partitionData);
    return reader;
}

CustomOnlinePartitionReaderPtr CustomOnlinePartition::InitReaderWithHeritage(
    const CustomOnlinePartitionWriterPtr& writer, const PartitionDataPtr& partitionData,
    const CustomOnlinePartitionReaderPtr& lastReader, const CustomOnlinePartitionReaderPtr& preLoadReader)
{
    if (!lastReader) {
        return InitReader(writer, partitionData);
    }

    TableReaderPtr lastTableReader;
    if (lastReader) {
        lastTableReader = lastReader->GetTableReader();
    }
    TableReaderPtr preloadTableReader;
    if (preLoadReader) {
        preloadTableReader = preLoadReader->GetTableReader();
    }
    CustomOnlinePartitionReaderPtr reader(new CustomOnlinePartitionReader(
        mSchema, mOptions, mPartitionName, mTableFactory, writer->GetTableWriter(), INVALID_SEGMENTID));
    reader->SetForceSeekInfo(mForceSeekInfo);
    reader->OpenWithHeritage(partitionData, lastTableReader, preloadTableReader);
    return reader;
}

void CustomOnlinePartition::CheckParam(const IndexPartitionSchemaPtr& schema,
                                       const IndexPartitionOptions& options) const
{
    if (!schema) {
        INDEXLIB_FATAL_ERROR(BadParameter, "[%s] no schema in index", IE_LOG_PREFIX);
    }

    if (!options.IsOnline()) {
        INDEXLIB_FATAL_ERROR(BadParameter, "[%s] OnlinePartition only support online option!", IE_LOG_PREFIX);
    }

    if (schema->GetTableType() != tt_customized) {
        INDEXLIB_FATAL_ERROR(BadParameter, "[%s] CustomOnlinePartition only support customzied table!", IE_LOG_PREFIX);
    }

    options.Check();
    schema->Check();

    // check maxReopenMemoryUse with memoryUseLimit
    int64_t maxReopenMemoryUse;
    int64_t totalMemUse = mPartitionMemController->GetFreeQuota() / (1024 * 1024);
    if (options.GetOnlineConfig().GetMaxReopenMemoryUse(maxReopenMemoryUse) && maxReopenMemoryUse >= totalMemUse) {
        INDEXLIB_FATAL_ERROR(BadParameter,
                             "[%s] maxReopenMemoryUse [%ld]MB should be less than current free memory [%ld]MB!",
                             IE_LOG_PREFIX, maxReopenMemoryUse, totalMemUse);
    }
    if (mLowMemUsedWatermark > mHighMemUsedWatermark || mLowMemUsedWatermark < 0) {
        INDEXLIB_FATAL_ERROR(
            BadParameter,
            "lowMemUsedWatermark[%zu] should not be larger than highMemUsedWatermark, and should not be less than zero",
            mLowMemUsedWatermark);
    }
}

MemoryQuotaSynchronizerPtr
CustomOnlinePartition::CreateRealtimeMemoryQuotaSynchronizer(const MemoryQuotaControllerPtr& realtimeQuotaController)
{
    assert(realtimeQuotaController);
    PartitionMemoryQuotaControllerPtr rtPartitionMemoryQuotaControl(
        new PartitionMemoryQuotaController(realtimeQuotaController, "realtime_partition_controller"));

    BlockMemoryQuotaControllerPtr blockMemQuotaControl(
        new BlockMemoryQuotaController(rtPartitionMemoryQuotaControl, "block_realtime_partition_controller"));

    MemoryQuotaSynchronizerPtr memQuotaSync(new MemoryQuotaSynchronizer(blockMemQuotaControl));
    return memQuotaSync;
}

IndexPartition::OpenStatus CustomOnlinePartition::ReOpen(bool forceReopen, versionid_t targetVersionId)
{
    OpenStatus os;
    try {
        if (mClosed) {
            IE_LOG(WARN, "Partition is closed. Try Open instead");
            return DoOpen(mOpenIndexPrimaryDir, mOpenIndexSecondaryDir, mOpenSchema, mOpenOptions, targetVersionId,
                          false);
        }
        CleanPartitionReaders();
        // scopelock will forbidden cleanResource when process reopen
        ScopedLock lock(mCleanerLock);
        CleanResource();
        return DoReopen(forceReopen, targetVersionId);
    } catch (const FileIOException& e) {
        IE_PREFIX_LOG(ERROR, "reopen caught file io exception: %s", e.what());
        if (!forceReopen && mOptions.GetOnlineConfig().IsReopenRetryOnIOExceptionEnabled()) {
            // suez will retry normalReopen if return OS_FAIL & set RS_ALLOW_FORCE_LOAD = false
            os = OS_FAIL;
        } else {
            os = OS_FILEIO_EXCEPTION;
        }
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
    return os;
}

IndexPartition::OpenStatus CustomOnlinePartition::DoReopen(bool forceReopen, versionid_t targetVersionId)
{
    ScopedLockPtr scopedLock(new ScopedLock(mDataLock));

    util::ScopeLatencyReporter scopeTime(mOnlinePartMetrics->GetreopenIncLatencyMetric().get());

    ReportMetrics();

    Version reopenVersion;
    ReopenDecider::ReopenType reopenType = MakeReopenDecision(forceReopen, targetVersionId, reopenVersion);

    auto versionDpDesc =
        CreateVersionDeployDescription(mOpenIndexPrimaryDir, mOptions.GetOnlineConfig(), reopenVersion.GetVersionId());
    if (versionDpDesc == nullptr) {
        IE_LOG(ERROR, "create version deploy description failed in table[%s], versionId[%d]",
               mSchema->GetSchemaName().c_str(), static_cast<int>(reopenVersion.GetVersionId()));
        return IndexPartition::OS_FAIL;
    }
    auto versionLevelLifecycleTable = versionDpDesc->GetLifecycleTable();
    if (versionLevelLifecycleTable == nullptr) {
        IE_LOG(ERROR, "LifecycleTable is NULL in versionDeployDescription, versionId=[%d], table=[%s]",
               reopenVersion.GetVersionId(), mSchema->GetSchemaName().c_str())
        return IndexPartition::OS_FAIL;
    }

    if (!mOptions.GetOnlineConfig().NeedReadRemoteIndex()) {
        THROW_IF_FS_ERROR(mFileSystem->MountVersion(mOpenIndexPrimaryDir, reopenVersion.GetVersionId(), "",
                                                    FSMT_READ_ONLY, versionLevelLifecycleTable),
                          "mount version failed");
    } else {
        THROW_IF_FS_ERROR(mFileSystem->MountVersion(mOpenIndexSecondaryDir, reopenVersion.GetVersionId(), "",
                                                    FSMT_READ_ONLY, versionLevelLifecycleTable),
                          "mount version fait diff > dev_indexlib_2022091iled");
    }

    OpenStatus ret = OS_FAIL;
    PartitionDataPtr partData = mPartitionDataHolder.Get();

    switch (reopenType) {
    case ReopenDecider::NORMAL_REOPEN:
        ret = NormalReopen(scopedLock, reopenVersion, versionDpDesc);
        break;
    case ReopenDecider::FORCE_REOPEN: {
        IE_LOG(WARN,
               "custom table force reopen version[%d] will "
               "abandon realtime data and switch to force *OPEN*",
               reopenVersion.GetVersionId());
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

    if (ret == OS_OK && mReaderUpdater) {
        if (!mReaderUpdater->Update(mReader)) {
            ret = OS_FAIL;
        }
    }
    return ret;
}

CustomOnlinePartitionReaderPtr CustomOnlinePartition::CreateDiffVersionReader(
    const index_base::Version& newVersion, const indexlibv2::framework::VersionDeployDescriptionPtr& newVersionDpDesc,
    const index_base::Version& lastVersion, const indexlibv2::framework::VersionDeployDescriptionPtr& lastVersionDpDesc)
{
    assert(newVersionDpDesc != nullptr);
    assert(lastVersionDpDesc != nullptr);
    auto newLifecycleTable = newVersionDpDesc->GetLifecycleTable();
    auto lastLifecycleTable = lastVersionDpDesc->GetLifecycleTable();
    Version diffVersion = newVersion;
    const auto& oldSegIdList = lastVersion.GetSegmentVector();
    for (auto segId : oldSegIdList) {
        if (newVersion.HasSegment(segId)) {
            auto segDirName = PathUtil::NormalizeDir(newVersion.GetSegmentDirName(segId));
            if (newLifecycleTable->GetLifecycle(segDirName) == lastLifecycleTable->GetLifecycle(segDirName)) {
                diffVersion.RemoveSegment(segId);
            }
        }
    }
    DumpSegmentQueuePtr emptyQueue;
    CustomOnlinePartitionWriterPtr emptyWriter;
    Version invalidVersion(INVALID_VERSIONID);
    auto diffPartitionData = CreateCustomPartitionData(diffVersion, invalidVersion, nullptr, emptyQueue, -1, true);
    return InitReaderWithMemoryLimit(diffPartitionData, emptyWriter, invalidVersion, diffVersion);
}

IndexPartition::OpenStatus CustomOnlinePartition::NormalReopenWithSegmentLevelPreload(
    ScopedLockPtr& dataLock, index_base::Version& incVersion,
    const indexlibv2::framework::VersionDeployDescriptionPtr& versionDpDesc)
{
    int64_t reclaimTimestamp = GetReclaimTimestamp(incVersion);
    PartitionDataPtr oldPartData = mPartitionDataHolder.Get();
    CustomOnlinePartitionReaderPtr newReader;
    dataLock.reset();
    CustomOnlinePartitionReaderPtr diffVersionReader =
        CreateDiffVersionReader(incVersion, versionDpDesc, oldPartData->GetOnDiskVersion(),
                                oldPartData->GetSegmentDirectory()->GetVersionDeployDescription());
    IE_PREFIX_LOG(INFO, "create diffVersion done");

    PartitionDataPtr newPartitionData = CreateCustomPartitionData(
        incVersion, oldPartData->GetOnDiskVersion(), versionDpDesc, mDumpSegmentQueue, reclaimTimestamp, false);
    IE_PREFIX_LOG(INFO, "create new partition data done");

    TableResourcePtr newTableResource;
    if (!mWriter->PrepareTableResource(DYNAMIC_POINTER_CAST(CustomPartitionData, newPartitionData), newTableResource)) {
        IE_PREFIX_LOG(ERROR, "prepare TableResource failed");
        if (mOptions.GetOnlineConfig().IsReopenRetryOnIOExceptionEnabled()) {
            IE_PREFIX_LOG(ERROR, "retryOnIOException is true, will return OS_FAIL");
            return OS_FAIL;
        }
        return OS_INDEXLIB_EXCEPTION;
    }
    TableResourcePtr oldTableResource;
    try {
        dataLock.reset(new ScopedLock(mDataLock));
        oldTableResource = mWriter->GetTableResource();
        mWriter->ResetPartitionData(newPartitionData, newTableResource);
        newReader = InitReaderWithHeritage(mWriter, newPartitionData, mReader, diffVersionReader);
        IE_LOG(INFO, "init reader with  new partition data done");
    } catch (const FileIOException& e) {
        IE_PREFIX_LOG(ERROR, "catch FILEIO exception in normalReopen, mWriter will be reset");
        mWriter->ResetPartitionData(oldPartData, oldTableResource);
        if (mOptions.GetOnlineConfig().IsReopenRetryOnIOExceptionEnabled()) {
            return OS_FAIL;
        }
        return OS_FILEIO_EXCEPTION;
    } catch (const ExceptionBase& e) {
        IE_PREFIX_LOG(ERROR, "catch Indexlib exception in normalReopen, mWriter will be reset");
        mWriter->ResetPartitionData(oldPartData, oldTableResource);
        return OS_INDEXLIB_EXCEPTION;
    } catch (...) {
        IE_PREFIX_LOG(ERROR, "catch unknown exception in normalReopen, mWriter will be reset");
        mWriter->ResetPartitionData(oldPartData, oldTableResource);
        return OS_INDEXLIB_EXCEPTION;
    }
    mPartitionDataHolder.Reset(newPartitionData);
    // dump obsolete building segment, so it can be removed
    NewSegmentMetaPtr buildingSegMeta = mWriter->GetBuildingSegmentMeta();
    if (buildingSegMeta && (buildingSegMeta->segmentInfo.timestamp < reclaimTimestamp)) {
        mWriter->DumpSegment();
        CustomPartitionDataPtr typedData = DYNAMIC_POINTER_CAST(CustomPartitionData, newPartitionData);
        typedData->RemoveObsoleteSegments();
        ReOpenNewSegment();
    } else {
        CustomPartitionDataPtr typedData = DYNAMIC_POINTER_CAST(CustomPartitionData, newPartitionData);
        typedData->RemoveObsoleteSegments();
        // SwitchReader depends on mPartitionDataHolder
        SwitchReader(newReader);
    }
    MEMORY_BARRIER();
    ReportMetrics();
    AddOnDiskIndexCleaner();

    IE_PREFIX_LOG(INFO, "reopen end : normal reopen version[%d] success", incVersion.GetVersionId());
    mLoadedIncVersion = incVersion;
    return OS_OK;
}

IndexPartition::OpenStatus
CustomOnlinePartition::NormalReopen(ScopedLockPtr& dataLock, index_base::Version& incVersion,
                                    const indexlibv2::framework::VersionDeployDescriptionPtr& versionDpDesc)
{
    IE_PREFIX_LOG(INFO, "normal reopen begin");
    int64_t reclaimTimestamp = GetReclaimTimestamp(incVersion);
    IndexPhaseGuard phaseGuard(mIndexPhase, NORMAL_REOPEN);
    if (mSupportSegmentLevelReader && mEnableAsyncDump) {
        return NormalReopenWithSegmentLevelPreload(dataLock, incVersion, versionDpDesc);
    }

    PartitionDataPtr oldPartData = mPartitionDataHolder.Get();
    CustomOnlinePartitionReaderPtr newReader;
    PartitionDataPtr newPartitionData;
    DumpSegmentQueuePtr newDumpSegmentQueue;
    if (mDumpSegmentQueue) {
        newDumpSegmentQueue.reset(mDumpSegmentQueue->Clone());
        newDumpSegmentQueue->ReclaimSegments(reclaimTimestamp);
        IE_LOG(INFO, "dumpSegmentQueue reclaim segments done");
    }
    TableResourcePtr oldTableResource = mWriter->GetTableResource();

    try {
        Version lastIncVersion = oldPartData->GetOnDiskVersion();
        // will trigger relcaim rt segments
        newPartitionData = CreateCustomPartitionData(incVersion, lastIncVersion, versionDpDesc, newDumpSegmentQueue,
                                                     reclaimTimestamp, false);

        TableResourcePtr newTableResource;
        if (!mWriter->PrepareTableResource(DYNAMIC_POINTER_CAST(CustomPartitionData, newPartitionData),
                                           newTableResource)) {
            if (mOptions.GetOnlineConfig().IsReopenRetryOnIOExceptionEnabled()) {
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
    } catch (const FileIOException& e) {
        IE_PREFIX_LOG(ERROR, "catch FILEIO exception in normalReopen, mWriter will be reset");
        mWriter->ResetPartitionData(oldPartData, oldTableResource);
        if (mOptions.GetOnlineConfig().IsReopenRetryOnIOExceptionEnabled()) {
            return OS_FAIL;
        }
        return OS_FILEIO_EXCEPTION;
    } catch (const ExceptionBase& e) {
        IE_PREFIX_LOG(ERROR, "catch Indexlib exception in normalReopen, mWriter will be reset");
        mWriter->ResetPartitionData(oldPartData, oldTableResource);
        return OS_INDEXLIB_EXCEPTION;
    } catch (...) {
        IE_PREFIX_LOG(ERROR, "catch unknown exception in normalReopen, mWriter will be reset");
        mWriter->ResetPartitionData(oldPartData, oldTableResource);
        return OS_INDEXLIB_EXCEPTION;
    }
    mDumpSegmentQueue = newDumpSegmentQueue;
    mPartitionDataHolder.Reset(newPartitionData);

    NewSegmentMetaPtr buildingSegMeta = mWriter->GetBuildingSegmentMeta();
    if (buildingSegMeta && (buildingSegMeta->segmentInfo.timestamp < reclaimTimestamp)) {
        mWriter->DumpSegment();
        CustomPartitionDataPtr typedData = DYNAMIC_POINTER_CAST(CustomPartitionData, newPartitionData);
        typedData->RemoveObsoleteSegments();
        ReOpenNewSegment();
    } else {
        CustomPartitionDataPtr typedData = DYNAMIC_POINTER_CAST(CustomPartitionData, newPartitionData);
        typedData->RemoveObsoleteSegments();
        // SwitchReader depends on mPartitionDataHolder
        SwitchReader(newReader);
    }
    MEMORY_BARRIER();
    ReportMetrics();
    AddOnDiskIndexCleaner();

    IE_PREFIX_LOG(INFO, "reopen end : normal reopen version[%d] success", incVersion.GetVersionId());
    mLoadedIncVersion = incVersion;
    return OS_OK;
}

ReopenDecider::ReopenType CustomOnlinePartition::MakeReopenDecision(bool forceReopen, versionid_t targetVersionId,
                                                                    index_base::Version& reopenVersion)
{
    util::ScopeLatencyReporter scopeTime(mOnlinePartMetrics->GetdecisionLatencyMetric().get());
    VersionLoader::GetVersionS(mOpenIndexSecondaryDir.empty() ? mOpenIndexPrimaryDir : mOpenIndexSecondaryDir,
                               reopenVersion, targetVersionId);
    Version lastIncVersion = mPartitionDataHolder.Get()->GetOnDiskVersion();

    if (reopenVersion.GetVersionId() <= lastIncVersion.GetVersionId()) {
        IE_LOG(WARN,
               "reopen versionId [%d] <= last loaded version[%d], "
               "no need to reopen",
               reopenVersion.GetVersionId(), lastIncVersion.GetVersionId());
        reopenVersion = lastIncVersion;
        return ReopenDecider::NO_NEED_REOPEN;
    }
    if (reopenVersion.GetSchemaVersionId() != lastIncVersion.GetSchemaVersionId()) {
        IE_LOG(WARN,
               "schema_version not match, "
               "[%u] in target version [%d], [%u] in current version [%d]",
               reopenVersion.GetSchemaVersionId(), reopenVersion.GetVersionId(), lastIncVersion.GetSchemaVersionId(),
               lastIncVersion.GetVersionId());
        return ReopenDecider::INCONSISTENT_SCHEMA_REOPEN;
    }

    if (reopenVersion.GetTimestamp() < lastIncVersion.GetTimestamp()) {
        IE_LOG(WARN,
               "new version with smaller timestamp, "
               "[%ld] in target version [%d], [%ld] in current version [%d]. "
               "new version may be rollback index!",
               reopenVersion.GetTimestamp(), reopenVersion.GetVersionId(), lastIncVersion.GetTimestamp(),
               lastIncVersion.GetVersionId());
        return ReopenDecider::INDEX_ROLL_BACK_REOPEN;
    }
    if (forceReopen) {
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
    UpdateReader(nullptr);
    IE_PREFIX_LOG(INFO, "reopen new segment end");
}

void CustomOnlinePartition::DoClose()
{
    mClosed = true;
    mIsServiceNormal = false;

    if (mTaskScheduler) {
        mTaskScheduler->DeleteTask(mCleanPartitionReaderTaskId);
        mTaskScheduler->DeleteTask(mCleanResourceTaskId);
        mTaskScheduler->DeleteTask(mReportMetricsTaskId);
        mTaskScheduler->DeleteTask(mRefreshReaderTaskId);
        mTaskScheduler->DeleteTask(mCheckIndexTaskId);
        mTaskScheduler->DeleteTask(mCheckRealtimeBuildTaskId);
    }
    ScopedLock lock1(mCleanerLock);
    ScopedLock lock2(mDataLock);
    while (mReaderContainer->Size() > 1) {
        auto unReleasedReader = mReaderContainer->PopOldestReader();
        if (unReleasedReader && unReleasedReader.use_count() > 1) {
            IE_LOG(ERROR,
                   "unreleased reader [version: %d] exists"
                   " when IndexPartition is to be closed",
                   unReleasedReader->GetVersion().GetVersionId());
        }
    }
    mReaderContainer->Close();
    {
        autil::ScopedLock lock(mReaderLock);
        if (mReader.use_count() > 1) {
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
    if (mReader) {
        mReader.reset();
    }
    if (mWriter) {
        mWriter->Close();
    }
    mWriter.reset();
    DoRefreshReader(false);
    if (mOptions.GetOnlineConfig().loadRemainFlushRealtimeIndex && !autil::EnvUtil::getEnv("TEST_QUICK_EXIT", false)) {
        IE_LOG(INFO, "will sleep ten seconds to wait flushQueue end flush");
        sleep(10);
    }
    mDumpSegmentQueue->Clear();

    ExecutorManager* executorManager = indexlib::util::Singleton<ExecutorManager>::GetInstance();
    size_t clearCounter = executorManager->ClearUselessExecutors();
    if (clearCounter > 0) {
        IE_LOG(INFO, "Clear useless executors[count=%zu] from table[%s]", clearCounter,
               mSchema->GetSchemaName().c_str());
    }

    mPartitionDataHolder.Reset();
    if (mFileSystem) {
        mFileSystem->Sync(true).GetOrThrow();
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
    OnlineJoinPolicy joinPolicy(mLoadedIncVersion, mSchema->GetTableType(), document::SrcSignature());
    return joinPolicy.GetRtSeekTimestampFromIncVersion();
}

bool CustomOnlinePartition::CleanIndexFiles(const vector<versionid_t>& keepVersionIds)
{
    if (!mLocalIndexCleaner) {
        mLocalIndexCleaner.reset(new LocalIndexCleaner(mOpenIndexPrimaryDir, mOpenIndexSecondaryDir,
                                                       mOptions.GetOnlineConfig().onlineKeepVersionCount,
                                                       mReaderContainer));
    }
    return mLocalIndexCleaner->Clean(keepVersionIds);
}

bool CustomOnlinePartition::CleanUnreferencedIndexFiles(const std::set<std::string>& toKeepFiles)
{
    if (!mLocalIndexCleaner) {
        mLocalIndexCleaner.reset(new LocalIndexCleaner(mOpenIndexPrimaryDir, mOpenIndexSecondaryDir,
                                                       mOptions.GetOnlineConfig().onlineKeepVersionCount,
                                                       mReaderContainer));
    }
    return mLocalIndexCleaner->CleanUnreferencedIndexFiles(mLoadedIncVersion, toKeepFiles);
}

void CustomOnlinePartition::SwitchReader(const CustomOnlinePartitionReaderPtr& reader)
{
    {
        ScopedLock lock(mReaderLock);
        mReader = reader;
        MEMORY_BARRIER();
    }
    mReaderContainer->AddReader(reader);
    mResourceCalculator->CalculateCurrentIndexSize(mPartitionDataHolder.Get(), mSchema);
}

IndexPartition::MemoryStatus CustomOnlinePartition::CheckMemoryStatus() const
{
    auto status = CheckMemoryWatermark();
    if (status == MS_REACH_HIGH_WATERMARK_MEM_LIMIT || status == MS_REACH_LOW_WATERMARK_MEM_LIMIT) {
        return MS_OK;
    }
    return status;
}

IndexPartition::MemoryStatus CustomOnlinePartition::CheckMemoryWatermark() const
{
    if (!mResourceCalculator) {
        return MS_OK;
    }

    const OnlineConfig& onlineConfig = mOptions.GetOnlineConfig();
    int64_t maxRtIndexSizeInMB = onlineConfig.maxRealtimeMemSize;

    int64_t rtIndexMemUseInMB = (mResourceCalculator->GetRtIndexMemoryUse() + mDumpSegmentQueue->GetEstimateDumpSize() +
                                 mTableWriterMemController->GetUsedQuota()) /
                                (1024 * 1024);

    // TODO: delete oldest rtSegments to release memory
    if (rtIndexMemUseInMB >= maxRtIndexSizeInMB) {
        IE_INTERVAL_LOG(128, WARN,
                        "Reach max Rt index size[%ld MB], "
                        "current rtIndexSize is %ld MB",
                        maxRtIndexSizeInMB, rtIndexMemUseInMB);
        return MS_REACH_MAX_RT_INDEX_SIZE;
    }
    if (mRtMemQuotaSynchronizer && mRtMemQuotaSynchronizer->GetFreeQuota() <= 0) {
        IE_INTERVAL_LOG(128, WARN, "Reach realtime quota control limit, current free quota[%lu]",
                        mRtMemQuotaSynchronizer->GetFreeQuota());
        return MS_REACH_MAX_RT_INDEX_SIZE;
    }
    int64_t availableMemoryUse = mPartitionMemController->GetFreeQuota() / (1024 * 1024);
    uint64_t buildingSegmentDumpExpandMemUse = mResourceCalculator->GetBuildingSegmentDumpExpandSize() / (1024 * 1024);

    if (availableMemoryUse <= (int64_t)buildingSegmentDumpExpandMemUse) {
        int64_t currentMemUse = mResourceCalculator->GetCurrentMemoryUse() / (1024 * 1024);
        IE_INTERVAL_LOG(128, WARN,
                        "Reach memory use limit free mem [%ld MB], "
                        "current total memory is %ld MB, " // maxReopenMemoryUse [%ld MB] "
                        "dumpExpandMemUse [%lu MB]",
                        availableMemoryUse, currentMemUse,
                        /*maxReopenMemoryUse,*/ buildingSegmentDumpExpandMemUse);
        return MS_REACH_TOTAL_MEM_LIMIT;
    }
    uint64_t rtIndexMemoryRatio = 100 * rtIndexMemUseInMB / maxRtIndexSizeInMB;
    if (rtIndexMemoryRatio >= mHighMemUsedWatermark) {
        IE_INTERVAL_LOG(128, WARN,
                        "Reach high Rt index watermark[%ld], "
                        "current rtIndexMemUseRate is %ld",
                        mHighMemUsedWatermark, rtIndexMemoryRatio);
        return MS_REACH_HIGH_WATERMARK_MEM_LIMIT;
    } else if (rtIndexMemoryRatio >= mLowMemUsedWatermark) {
        return MS_REACH_LOW_WATERMARK_MEM_LIMIT;
    }
    return MS_OK;
}

void CustomOnlinePartition::InitResourceCleaner()
{
    mResourceCleaner.reset(new ExecutorScheduler);

    ExecutorPtr inMemoryIndexCleaner(
        new InMemoryIndexCleaner(mReaderContainer, mPartitionDataHolder.Get()->GetRootDirectory()));
    mResourceCleaner->Add(inMemoryIndexCleaner, ExecutorScheduler::ST_REPEATEDLY);
    AddOnDiskIndexCleaner();
}

void CustomOnlinePartition::AddOnDiskIndexCleaner()
{
    // TODO: In future, suez will in controll of local index clean
    string envStr = autil::EnvUtil::getEnv("INDEXLIB_ONLINE_CLEAN_ON_DISK_INDEX");
    if (envStr == "false") {
        return;
    }
    ExecutorPtr onDiskIndexCleaner(new OnDiskIndexCleaner(mOptions.GetOnlineConfig().onlineKeepVersionCount,
                                                          mReaderContainer, GetRootDirectory()));
    mResourceCleaner->Add(onDiskIndexCleaner, ExecutorScheduler::ST_ONCE);
}

void CustomOnlinePartition::ExecuteCleanResourceTask(std::function<void()> UDFCleanResourceTask)
{
    if (mClosed) {
        return;
    }

    if (mCleanerLock.trylock() != 0) {
        return;
    }

    try {
        CleanResource();
    } catch (ExceptionBase& e) {
        IE_PREFIX_LOG(ERROR, "Clean resource exception [%s]!!", e.what());
    } catch (...) {
        IE_PREFIX_LOG(ERROR, "Clean resource exception!!");
    }

    mCleanerLock.unlock();
    if (UDFCleanResourceTask) {
        UDFCleanResourceTask();
    }
}

void CustomOnlinePartition::CleanResource()
{
    ScopedLock lock(mCleanerLock);
    if (!mIsServiceNormal) {
        return;
    }
    mResourceCleaner->Execute();
    UpdateSwitchRtSegmentSize();
}

void CustomOnlinePartition::UpdateSwitchRtSegmentSize()
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
            mResourceCalculator->UpdateSwitchRtSegments(mSchema, switchRtSegmentIds);
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

void CustomOnlinePartition::ExecuteCleanPartitionReaderTask()
{
    if (mClosed) {
        return;
    }
    try {
        CleanPartitionReaders();
    } catch (ExceptionBase& e) {
        IE_PREFIX_LOG(ERROR, "Clean partition reader exception [%s]!!", e.what());
    } catch (...) {
        IE_PREFIX_LOG(ERROR, "Clean partition reader exception!!");
    }
}

void CustomOnlinePartition::CleanPartitionReaders()
{
    if (!mIsServiceNormal) {
        return;
    }
    auto doSomething = mReaderContainer->EvictOldReaders();
    if (doSomething) {
        mFileSystem->CleanCache();
    }
}

void CustomOnlinePartition::TEST_DeleteBackGroundTask() { mTaskScheduler->CleanTask(); }

void CustomOnlinePartition::ReportMetrics()
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

void CustomOnlinePartition::UpdateOnlinePartitionMetrics()
{
    if (!mResourceCalculator) {
        return;
    }
    size_t builtRtIndexMemoryUse = mResourceCalculator->GetRtIndexMemoryUse();
    size_t writerMemoryUse = mTableWriterMemController->GetUsedQuota();
    size_t rtIndexMemoryUse = builtRtIndexMemoryUse + writerMemoryUse + mDumpSegmentQueue->GetEstimateDumpSize();
    if (mRtMemQuotaSynchronizer) {
        mRtMemQuotaSynchronizer->SyncMemoryQuota(rtIndexMemoryUse);
    }

    IndexPartition::MemoryStatus memoryStatus = CheckMemoryStatus();
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
    mOnlinePartMetrics->SetoldInMemorySegmentMemoryUseValue(writerMemoryUse);
    mOnlinePartMetrics->SetpartitionMemoryQuotaUseValue(mPartitionMemController->GetUsedQuota());
    mOnlinePartMetrics->SetmissingSegmentCountValue(mMissingSegmentCount.load());

    // update ReaderContainer related metrics
    versionid_t oldestReaderVersion = mReaderContainer->GetOldestReaderVersion();
    versionid_t latestReaderVersion = mReaderContainer->GetLatestReaderVersion();
    int64_t readerCount = mReaderContainer->Size();

    mOnlinePartMetrics->SetpartitionReaderVersionCountValue(readerCount);
    mOnlinePartMetrics->SetoldestReaderVersionIdValue(oldestReaderVersion);
    mOnlinePartMetrics->SetlatestReaderVersionIdValue(latestReaderVersion);
    if (!mReader) {
        return;
    }
    mOnlinePartMetrics->SetinMemRtSegmentCountValue(mReader->GetInMemRtSegmentCount());
    mOnlinePartMetrics->SetonDiskRtSegmentCountValue(mReader->GetOnDiskRtSegmentCount());
    mOnlinePartMetrics->SetusedOnDiskRtSegmentCountValue(mReader->GetUsedOnDiskRtSegmentCount());
}

IFileSystemPtr CustomOnlinePartition::CreateFileSystem(const string& primaryDir, const string& secondaryDir)
{
    if (mOptions.TEST_mReadOnly) {
        return IndexPartition::CreateFileSystem(primaryDir, secondaryDir);
    }
    auto fileSystem = IndexPartition::CreateFileSystem(primaryDir, secondaryDir);
    ResetRtAndJoinDirPath(fileSystem);
    return fileSystem;
}

void CustomOnlinePartition::ResetRtAndJoinDirPath(const file_system::IFileSystemPtr& fileSystem)
{
    assert(fileSystem != nullptr);
    THROW_IF_FS_ERROR(fileSystem->RemoveDirectory(JOIN_INDEX_PARTITION_DIR_NAME, RemoveOption::MayNonExist()),
                      "remove JOIN_INDEX_PARTITION_DIR_NAME failed");
    if (!mOptions.GetOnlineConfig().loadRemainFlushRealtimeIndex) {
        THROW_IF_FS_ERROR(fileSystem->RemoveDirectory(RT_INDEX_PARTITION_DIR_NAME, RemoveOption::MayNonExist()),
                          "remove RT_INDEX_PARTITION_DIR_NAME failed");
    }
}

bool CustomOnlinePartition::PrepareIntervalTask()
{
    if (!mTaskScheduler) {
        IE_LOG(ERROR, "TaskScheduler is NULL");
        return false;
    }

    int32_t sleepTime = REPORT_METRICS_INTERVAL;
    if (unlikely(autil::EnvUtil::getEnv("TEST_QUICK_EXIT", false))) {
        sleepTime /= 1000;
    }
    if (mOptions.GetOnlineConfig().enableAsyncCleanResource) {
        TaskItemPtr cleanResourceTask(new BindFunctionTaskItem(std::bind(
            &CustomOnlinePartition::ExecuteCleanResourceTask, this, mTableFactory->CreateCleanResourceTask())));
        if (!mTaskScheduler->DeclareTaskGroup("clean_resource", sleepTime)) {
            IE_PREFIX_LOG(ERROR, "declare clean resource task failed!");
            return false;
        }
        mCleanResourceTaskId = mTaskScheduler->AddTask("clean_resource", cleanResourceTask);
        if (mCleanResourceTaskId == TaskScheduler::INVALID_TASK_ID) {
            IE_PREFIX_LOG(ERROR, "add clean resource task failed!");
            return false;
        }

        TaskItemPtr cleanPartitionReaderTask(
            new MemberFunctionTaskItem<CustomOnlinePartition, &CustomOnlinePartition::ExecuteCleanPartitionReaderTask>(
                *this));
        if (!mTaskScheduler->DeclareTaskGroup("clean_reader", sleepTime)) {
            IE_PREFIX_LOG(ERROR, "declare clean partition reader task failed!");
            return false;
        }
        mCleanPartitionReaderTaskId = mTaskScheduler->AddTask("clean_reader", cleanPartitionReaderTask);
        if (mCleanPartitionReaderTaskId == TaskScheduler::INVALID_TASK_ID) {
            IE_PREFIX_LOG(ERROR, "add clean partition reader task failed!");
            return false;
        }
    }
    TaskItemPtr reportMetricsTask(
        new MemberFunctionTaskItem<CustomOnlinePartition, &CustomOnlinePartition::ReportMetrics>(*this));

    if (!mTaskScheduler->DeclareTaskGroup("report_metrics", sleepTime)) {
        IE_PREFIX_LOG(ERROR, "declare report metrics task failed!");
        return false;
    }
    mReportMetricsTaskId = mTaskScheduler->AddTask("report_metrics", reportMetricsTask);
    if (mReportMetricsTaskId == TaskScheduler::INVALID_TASK_ID) {
        IE_PREFIX_LOG(ERROR, "add report metrics task failed!");
        return false;
    }

    if (!mTaskScheduler->DeclareTaskGroup("refresh_reader", sleepTime)) {
        IE_PREFIX_LOG(ERROR, "declare async dump group failed");
        return false;
    }
    TaskItemPtr asyncDumpTask(
        new MemberFunctionTaskItem<CustomOnlinePartition, &CustomOnlinePartition::RefreshReader>(*this));
    mRefreshReaderTaskId = mTaskScheduler->AddTask("refresh_reader", asyncDumpTask);
    if (mRefreshReaderTaskId == TaskScheduler::INVALID_TASK_ID) {
        IE_PREFIX_LOG(ERROR, "add async dump task failed");
        return false;
    }

    if (NeedCheckSecondaryIndex()) {
        if (!mTaskScheduler->DeclareTaskGroup("check_index", mCheckSecondaryIndexIntervalInMin * 60 * 1000 * 1000)) {
            IE_PREFIX_LOG(ERROR, "declare check_index task group failed");
            return false;
        }
        TaskItemPtr checkSecondaryIndexTask(
            new MemberFunctionTaskItem<CustomOnlinePartition, &CustomOnlinePartition::CheckSecondaryIndex>(*this));
        mCheckIndexTaskId = mTaskScheduler->AddTask("check_index", checkSecondaryIndexTask);
        if (mCheckIndexTaskId == TaskScheduler::INVALID_TASK_ID) {
            IE_PREFIX_LOG(ERROR, "add check_index task failed!");
            return false;
        }
    }
    if (!mTaskScheduler->DeclareTaskGroup("check_realtime_build", sleepTime)) {
        IE_PREFIX_LOG(ERROR, "declare check_realtime_build task group failed");
        return false;
    }
    if (mNeedCheckRealTimeBuild) {
        TaskItemPtr checkRealtimeBuildTask(
            new MemberFunctionTaskItem<CustomOnlinePartition, &CustomOnlinePartition::CheckRealtimeBuild>(*this));
        mCheckRealtimeBuildTaskId = mTaskScheduler->AddTask("check_realtime_build", checkRealtimeBuildTask);
        if (mCheckRealtimeBuildTaskId == TaskScheduler::INVALID_TASK_ID) {
            IE_PREFIX_LOG(ERROR, "add check_realtime_build task failed!");
            return false;
        }
    }
    return true;
}

bool CustomOnlinePartition::TryLock(autil::ThreadMutex& lock, size_t times, int64_t intervalInUs)
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

void CustomOnlinePartition::CheckSecondaryIndex()
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
void CustomOnlinePartition::CheckRealtimeBuild()
{
    if (mClosed) {
        return;
    }
    IndexPartition::MemoryStatus memoryStatus = CheckMemoryWatermark();
    if (MS_REACH_MAX_RT_INDEX_SIZE == memoryStatus && mCheckRtBuildDumpLimit &&
        (mDumpSegmentQueue && !mDumpSegmentQueue->Size())) {
        // avoid dumping too many times when rtMem is full
        IE_LOG(WARN, "reach MS_REACH_MAX_RT_INDEX_SIZE, will trigger external dump to resume realTime build");
        --mCheckRtBuildDumpLimit;
        if (!mCheckRtBuildDumpLimit) {
            IE_LOG(ERROR, "reach external dump limit, may stop build later");
        }
        DumpSegmentWhenReachMemLimit();
    }
}

void CustomOnlinePartition::RefreshReader()
{
    if (mClosed) {
        return;
    }
    if (!mRefreshReaderFuture.valid()) {
        mRefreshReaderFuture = std::async(std::launch::async, &CustomOnlinePartition::DoRefreshReader, this, false);
        return;
    }
    std::chrono::milliseconds span(10);
    auto waitRet = mRefreshReaderFuture.wait_for(span);
    if (waitRet == std::future_status::timeout) {
        return;
    }
    assert(waitRet == std::future_status::ready);
    mRefreshReaderFuture.get();
    mRefreshReaderFuture = std::async(std::launch::async, &CustomOnlinePartition::DoRefreshReader, this, false);
    return;
}

void CustomOnlinePartition::DoRefreshReader(bool needSync)
{
    int64_t latency = 0;
    uint32_t dumpCounter = 0;
    {
        ScopedTime timer(latency);
        while (!mClosed || mDumpSegmentQueue) {
            bool hasDumpedSegment = false;
            bool dumpSuccess = true;
            try {
                ScopedLock lock(mCleanerLock);
                if (mDumpSegmentQueue) {
                    dumpSuccess = mDumpSegmentQueue->ProcessOneItem(hasDumpedSegment);
                }
                if (!dumpSuccess) {
                    IE_PREFIX_LOG(ERROR, "dump segment failed!");
                    return;
                }
                if (needSync && mFileSystem) {
                    mFileSystem->Sync(true).GetOrThrow();
                }
                if (hasDumpedSegment) {
                    dumpCounter++;
                    // update rt segment lifecycle in commitversion
                    mPartitionDataHolder.Get()->CommitVersion();
                    mDumpSegmentQueue->PopDumpedItems();
                }
                auto preloadReader = CreateReaderWithUpdatedLifecycle(mReader);
                if (!mClosed && (hasDumpedSegment || preloadReader != nullptr)) {
                    UpdateReader(preloadReader);
                }
                if (!hasDumpedSegment) {
                    break;
                }
            } catch (const util::ExceptionBase& e) {
                IE_LOG(ERROR,
                       "Async dump segment thread end. "
                       "because updateReader catch exception:[%s].",
                       e.what());
                return;
            }
            usleep(500000); // 1s
        }
    }
    if (dumpCounter > 0) {
        IE_PREFIX_LOG(INFO, "flush dump segment items done , total count [%u], time elapsed [%ld ms].", dumpCounter,
                      latency / 1000);
    }
}

CustomOnlinePartitionReaderPtr CustomOnlinePartition::InitReaderWithFlushedSegments(segmentid_t latestLinkRtSegId)
{
    CustomOnlinePartitionReaderPtr reader;
    PartitionDataPtr clonedPartitionData(mPartitionDataHolder.Get()->Clone());
    set<segmentid_t> newFlushedSegList;
    auto lastReaderVersion = clonedPartitionData->GetVersion();
    segmentid_t lastLinkRtSegId = mReader->GetPartitionVersion()->GetLastLinkRtSegmentId();
    size_t segCount = lastReaderVersion.GetSegmentCount();
    for (size_t i = 0; i < segCount; i++) {
        auto segId = lastReaderVersion[i];
        if (!RealtimeSegmentDirectory::IsRtSegmentId(segId)) {
            continue;
        }
        if (segId > lastLinkRtSegId && segId <= latestLinkRtSegId) {
            newFlushedSegList.insert(segId);
        }
    }
    if (newFlushedSegList.empty()) {
        return reader;
    }
    string segListStr = StringUtil::toString(newFlushedSegList.begin(), newFlushedSegList.end(), ",");
    try {
        reader.reset(new CustomOnlinePartitionReader(mSchema, mOptions, mPartitionName, mTableFactory,
                                                     mWriter->GetTableWriter(), latestLinkRtSegId));
        reader->SetForceSeekInfo(mForceSeekInfo);
        reader->OpenWithHeritage(clonedPartitionData, mReader->GetTableReader(), newFlushedSegList);
    } catch (const FileIOException& e) {
        IE_PREFIX_LOG(ERROR, "catch FILEIO exception[%s] in InitReader With flushed segments[%s]", e.what(),
                      segListStr.c_str());
        reader.reset();
    } catch (const ExceptionBase& e) {
        IE_PREFIX_LOG(ERROR, "catch unexpected exception[%s] in InitReader with flushed segments[%s]", e.what(),
                      segListStr.c_str());
        reader.reset();
    } catch (...) {
        IE_PREFIX_LOG(ERROR, "catch unknown exception in InitReader with flushed segments[%s]", segListStr.c_str());
        reader.reset();
    }
    return reader;
}

void CustomOnlinePartition::UpdateReader(const CustomOnlinePartitionReaderPtr& preloadReader)
{
    ScopedLock lock(mDataLock);
    CustomOnlinePartitionReaderPtr reader;
    PartitionDataPtr clonedPartitionData;
    if (!mSupportSegmentLevelReader) {
        clonedPartitionData.reset(mPartitionDataHolder.Get()->Clone());
        reader = InitReader(mWriter, clonedPartitionData);
    } else {
        clonedPartitionData.reset(mPartitionDataHolder.Get()->Clone());
        reader = InitReaderWithHeritage(mWriter, clonedPartitionData, mReader, preloadReader);
    }
    SwitchReader(reader);
    if (mReaderUpdater) {
        mReaderUpdater->Update(mReader);
    }
}

bool CustomOnlinePartition::UpdateLatestLinkRtSegId(const PartitionDataPtr& partData,
                                                    segmentid_t& latestLinkRtSegId) const
{
    assert(partData);

    segmentid_t lastReaderLinkRtSegId =
        !mReader ? INVALID_SEGMENTID : mReader->GetPartitionVersion()->GetLastLinkRtSegmentId();
    segmentid_t lastValidLinkRtSegId = partData->GetLastValidRtSegmentInLinkDirectory();
    if (lastValidLinkRtSegId == INVALID_SEGMENTID) {
        latestLinkRtSegId = INVALID_SEGMENTID;
        return false;
    }
    if (lastValidLinkRtSegId == lastReaderLinkRtSegId) {
        latestLinkRtSegId = lastReaderLinkRtSegId;
        return false;
    }
    if (mIndexPhase != OPENING) {
        IndexPartition::MemoryStatus memoryStatus = CheckMemoryWatermark();
        if (memoryStatus == MS_OK) {
            IE_PREFIX_LOG(INFO,
                          "will not switch to latest flush rt segments [%d]."
                          "realtime mem use rate is under low mem used watermark [%ld]",
                          lastValidLinkRtSegId, mLowMemUsedWatermark);
            latestLinkRtSegId = lastReaderLinkRtSegId;
            return false;
        } else if (memoryStatus == MS_REACH_LOW_WATERMARK_MEM_LIMIT) {
            segmentid_t nextValidLinkRtSegId = mReader->GetNextValidSegIdToLink();
            lastValidLinkRtSegId =
                nextValidLinkRtSegId < lastValidLinkRtSegId ? nextValidLinkRtSegId : lastValidLinkRtSegId;
            IE_PREFIX_LOG(WARN,
                          "prepare to switch to onDiskFlushed rt segments [%d]."
                          "realtime mem use rate is above low mem used watermark [%ld] "
                          "and is under high mem used watermark [%ld]",
                          lastValidLinkRtSegId, mLowMemUsedWatermark, mHighMemUsedWatermark);
        }
    }
    MemoryReserverPtr memReserver = mFileSystem->CreateMemoryReserver("switch_flush_rt_segment");
    OnDiskRealtimeDataCalculator rtCalculator;
    rtCalculator.Init(mSchema, partData, mPluginManager);
    size_t switchExpandMemSize = rtCalculator.CalculateExpandSize(lastReaderLinkRtSegId, lastValidLinkRtSegId);
    if (switchExpandMemSize > 0) {
        if (!memReserver->Reserve(switchExpandMemSize)) {
            IE_PREFIX_LOG(WARN,
                          "switch to latest flush rt segments [%d] fail."
                          "mem need [%ld], mem left [%ld]",
                          lastValidLinkRtSegId, switchExpandMemSize, memReserver->GetFreeQuota());
            latestLinkRtSegId = lastReaderLinkRtSegId;
            return false;
        }
    }
    latestLinkRtSegId = lastValidLinkRtSegId;
    return true;
}

int64_t CustomOnlinePartition::GetReclaimTimestamp(const index_base::Version& version)
{
    OnlineJoinPolicy joinPolicy(version, mSchema->GetTableType(), document::SrcSignature());
    return joinPolicy.GetReclaimRtTimestamp();
}

void CustomOnlinePartition::DumpSegmentWhenReachMemLimit()
{
    // avoid from oom
    while (mDumpSegmentQueue->Size()) {
        usleep(500000); // 1s
    }
    ScopedLockPtr scopedLock(new ScopedLock(mDataLock));
    mWriter->DumpSegment();
    ReOpenNewSegment();
}

indexlibv2::framework::VersionDeployDescriptionPtr CustomOnlinePartition::CreateVersionDeployDescription(
    const string& indexRoot, const indexlib::config::OnlineConfig& onlineConfig, versionid_t versionId)
{
    auto versionDpDesc = std::make_shared<indexlibv2::framework::VersionDeployDescription>();
    if (!onlineConfig.EnableLocalDeployManifestChecking()) {
        versionDpDesc->DisableFeature(
            indexlibv2::framework::VersionDeployDescription::FeatureType::DEPLOY_META_MANIFEST);
        return versionDpDesc;
    }
    if (!indexlibv2::framework::VersionDeployDescription::LoadDeployDescription(indexRoot, versionId,
                                                                                versionDpDesc.get())) {
        IE_LOG(ERROR,
               "load version deploy description failed in CreateVersionDeployDescription, versionId[%d], "
               "indexRootPath[%s]",
               static_cast<int>(versionId), GetRootPath().c_str());
        return nullptr;
    }
    return versionDpDesc;
}

CustomOnlinePartitionReaderPtr
CustomOnlinePartition::CreateReaderWithUpdatedLifecycle(const CustomOnlinePartitionReaderPtr& lastReader)
{
    // if seg[i]'s lifecycle is updated, re-create segReader[i]
    // if no segment has updated lifecycle, return nullptr
    if (!mSupportSegmentLevelReader || (lastReader == nullptr)) {
        return nullptr;
    }
    PartitionDataPtr clonedPartitionData(mPartitionDataHolder.Get()->Clone());
    std::set<segmentid_t> toReloadSegIdList = CalculateLifecycleUpdatedSegments(clonedPartitionData, lastReader);
    if (toReloadSegIdList.empty()) {
        return nullptr;
    }

    auto tableWriter = (mWriter == nullptr) ? nullptr : mWriter->GetTableWriter();
    CustomOnlinePartitionReaderPtr reader(
        new CustomOnlinePartitionReader(mSchema, mOptions, mPartitionName, mTableFactory, tableWriter));
    reader->OpenWithHeritage(clonedPartitionData, lastReader->GetTableReader(), toReloadSegIdList);
    return reader;
}

std::set<segmentid_t>
CustomOnlinePartition::CalculateLifecycleUpdatedSegments(const PartitionDataPtr& newPartitionData,
                                                         const CustomOnlinePartitionReaderPtr& oldPartitionReader) const
{
    assert(oldPartitionReader != nullptr);
    assert(newPartitionData != nullptr);
    std::set<segmentid_t> toReloadSegIdList;
    auto oldSegmentLifecycles = oldPartitionReader->GetBuiltSegmentLifecycles();
    const auto& newSegmentDirectory = newPartitionData->GetSegmentDirectory();
    if (oldSegmentLifecycles.size() == 0) {
        return toReloadSegIdList;
    }
    if (newSegmentDirectory == nullptr) {
        IE_LOG(ERROR, "UNEXPECTED: cannot get segmentDirectory from partitionData[schema=%s]",
               mSchema->GetSchemaName().c_str());
        return toReloadSegIdList;
    }
    stringstream ss;
    for (const auto& kv : oldSegmentLifecycles) {
        const auto& segId = kv.first;
        const auto& oldLifecycle = kv.second;
        auto ret = newSegmentDirectory->GetSegmentLifecycle(segId);
        if (ret.first == true && ret.second != oldLifecycle) {
            ss << segId << "{" << oldLifecycle << "->" << ret.second << "},";
            toReloadSegIdList.insert(segId);
        }
    }
    if (!toReloadSegIdList.empty()) {
        IE_LOG(INFO, "table[%s] with lifecycle updates: [%s]", mSchema->GetSchemaName().c_str(), ss.str().c_str());
    }
    return toReloadSegIdList;
}

}} // namespace indexlib::partition
