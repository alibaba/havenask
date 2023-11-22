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
#include "indexlib/partition/custom_offline_partition.h"

#include <fstream>

#include "autil/EnvUtil.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index_base/branch_fs.h"
#include "indexlib/index_base/index_meta/index_format_version.h"
#include "indexlib/index_base/index_meta/parallel_build_info.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/partition/building_partition_data.h"
#include "indexlib/partition/custom_offline_partition_writer.h"
#include "indexlib/partition/custom_partition_data.h"
#include "indexlib/partition/index_partition_resource.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/partition/segment/dump_segment_queue.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/table/table_factory_wrapper.h"
#include "indexlib/table/table_plugin_loader.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/counter/CounterMap.h"
#include "indexlib/util/mem_util.h"
#include "indexlib/util/memory_control/BlockMemoryQuotaController.h"

using namespace std;
using namespace autil;

using namespace indexlib::config;
using namespace indexlib::common;
using namespace indexlib::util;
using namespace indexlib::index;
using namespace indexlib::plugin;
using namespace indexlib::table;
using namespace indexlib::index_base;
using namespace indexlib::file_system;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, CustomOfflinePartition);

CustomOfflinePartition::CustomOfflinePartition(const IndexPartitionResource& partitionResource)
    : IndexPartition(partitionResource)
    , mParallelBuildInfo(partitionResource.parallelBuildInfo)
    , mClosed(true)
    , mDisableBackgroundThread(false)
    , mPartitionRange(partitionResource.range)
    , mMemLimitInBytes(numeric_limits<int64_t>::max())
    , mEnableAsyncDump(false)
{
}

CustomOfflinePartition::~CustomOfflinePartition()
{
    mClosed = true;
    mAsyncDumpThreadPtr.reset();
    mBackGroundThreadPtr.reset();

    mWriter.reset();
    IndexPartition::Reset();
    mTableFactory.reset();
}

string CustomOfflinePartition::GetLastLocator() const
{
    string locator;
    if (mWriter) {
        locator = mWriter->GetLastLocator();
    }
    if (!locator.empty()) {
        return locator;
    }
    PartitionDataPtr partitionData = mPartitionDataHolder.Get();
    return partitionData ? partitionData->GetLastLocator() : string("");
}

IndexPartition::OpenStatus CustomOfflinePartition::Open(const std::string& primaryDir, const std::string& secondaryDir,
                                                        const config::IndexPartitionSchemaPtr& schema,
                                                        const config::IndexPartitionOptions& options,
                                                        versionid_t versionId)
{
    IndexPartition::OpenStatus os = OS_FAIL;
    try {
        os = DoOpen(primaryDir, schema, options, versionId);
    } catch (const FileIOException& e) {
        IE_LOG(ERROR, "open caught file io exception: %s", e.what());
        os = OS_FILEIO_EXCEPTION;
    } catch (const ExceptionBase& e) {
        // IndexCollapsedException, UnSupportedException,
        // BadParameterException, InconsistentStateException, ...
        IE_LOG(ERROR, "open caught exception: %s", e.what());
        os = OS_INDEXLIB_EXCEPTION;
    } catch (const exception& e) {
        IE_LOG(ERROR, "open caught std exception: %s", e.what());
        os = OS_UNKNOWN_EXCEPTION;
    } catch (...) {
        IE_LOG(ERROR, "open caught unknown exception");
        os = OS_UNKNOWN_EXCEPTION;
    }
    return os;
}

IndexPartition::OpenStatus CustomOfflinePartition::DoOpen(const string& root, const IndexPartitionSchemaPtr& schema,
                                                          const IndexPartitionOptions& options, versionid_t versionId)
{
    mParallelBuildInfo.CheckValid();
    string outputRoot = mParallelBuildInfo.IsParallelBuild() ? mParallelBuildInfo.GetParallelInstancePath(root) : root;
    IndexPartition::OpenStatus openStatus = IndexPartition::Open(outputRoot, root, schema, options, versionId);
    if (openStatus != OS_OK) {
        return openStatus;
    }

    mOptions.GetOfflineConfig().recoverType = RecoverStrategyType::RST_VERSION_LEVEL;
    Version version;
    version.SyncSchemaVersionId(schema);

    if (versionId != INVALID_VERSIONID) {
        IE_LOG(ERROR, "CustomOfflinePartition does not support open with specified version");
        return OS_FAIL;
    }
    // inc build
    VersionLoader::GetVersionS(root, version, versionId);
    if (version.GetVersionId() != INVALID_VERSIONID) {
        THROW_IF_FS_ERROR(mFileSystem->MountVersion(root, version.GetVersionId(), "", FSMT_READ_ONLY, nullptr),
                          "mount version failed");
    } else {
        THROW_IF_FS_ERROR(mFileSystem->MountFile(root, INDEX_FORMAT_VERSION_FILE_NAME, INDEX_FORMAT_VERSION_FILE_NAME,
                                                 FSMT_READ_ONLY, -1, false),
                          "mount index format version file failed");
        string schemaFileName = Version::GetSchemaFileName(version.GetSchemaVersionId());
        THROW_IF_FS_ERROR(mFileSystem->MountFile(root, schemaFileName, schemaFileName, FSMT_READ_ONLY, -1, false),
                          "mount schema file failed");
    }
    mSchema = SchemaAdapter::LoadSchema(mRootDirectory, version.GetSchemaVersionId());
    ValidateConfigAndParam(schema, mSchema, mOptions);

    mMemLimitInBytes = GetAllowedMemoryInBytes();
    mTableWriterMemController.reset(
        new BlockMemoryQuotaController(mPartitionMemController, mSchema->GetSchemaName() + "_TableWriterMemory"));

    if (!InitPlugin(mIndexPluginPath, mSchema, mOptions)) {
        return OS_FAIL;
    }

    if (mParallelBuildInfo.IsParallelBuild()) {
        mParallelBuildInfo.SetBaseVersion(version.GetVersionId());
        mParallelBuildInfo.StoreIfNotExist(mRootDirectory);
    }

    mCounterMap.reset(new CounterMap());
    mDumpSegmentQueue.reset(new DumpSegmentQueue());
    mEnableAsyncDump = options.GetBuildConfig(false).EnableAsyncDump();

    if (mEnableAsyncDump) {
        IE_LOG(INFO, "async dump thread is enabled");
    } else {
        IE_LOG(INFO, "async dump thread is disabled");
    }

    if (!InitPartitionData(mFileSystem, version, mCounterMap, mPluginManager)) {
        return OS_FAIL;
    }

    PartitionDataPtr partitionData = mPartitionDataHolder.Get();

    if (!ValidatePartitionMeta(partitionData->GetPartitionMeta())) {
        return OS_FAIL;
    }

    if (!mTableFactory) {
        mTableFactory.reset(new TableFactoryWrapper(mSchema, mOptions, partitionData->GetPluginManager()));
        if (!mTableFactory->Init()) {
            IE_LOG(ERROR, "Init TableFactory failed in table[%s]", mSchema->GetSchemaName().c_str());
            return OS_FAIL;
        }
    }
    InitWriter(partitionData);
    mClosed = false;
    if (!PrepareIntervalTask()) {
        IE_LOG(ERROR, "Prepare interval task failed in table[%s]", mSchema->GetSchemaName().c_str());
        return OS_FAIL;
    }

    return OS_OK;
}

int64_t CustomOfflinePartition::GetAllowedMemoryInBytes() const
{
    int64_t buildTotalMem = mOptions.GetBuildConfig(false).buildTotalMemory * 1024 * 1024;
    int64_t memTotalInBytes = MemUtil::GetContainerMemoryLimitsInBytes();
    if (memTotalInBytes == -1) {
        IE_LOG(ERROR, "failed to get process memoryLimit, use default[numeric_limits<int64>::max]");
        memTotalInBytes = numeric_limits<int64_t>::max();
    }
    uint32_t defaultEnvFactor = DEFAULT_OFFLINE_MEMORY_FACTOR;
    uint32_t envFactor = autil::EnvUtil::getEnv("INDEXLIB_OFFLINE_MEMORY_FACTOR", defaultEnvFactor);
    if (envFactor == 0) {
        IE_LOG(WARN,
               "INDEXLIB_OFFLINE_MEMORY_FACTOR should larger than 0, "
               "set to default value[%u] instead",
               defaultEnvFactor);
        envFactor = defaultEnvFactor;
    }
    int64_t modifiedMemLimits =
        std::min(int64_t(memTotalInBytes * DEFAULT_MACHINE_MEMORY_RATIO), buildTotalMem * envFactor);
    IE_LOG(INFO,
           "AllowedMemoryLimit = [%ld] bytes, MachineMemoryLimit = [%ld] bytes, "
           "envFactor = [%u], buildTotalMem=[%ld]",
           modifiedMemLimits, memTotalInBytes, envFactor, buildTotalMem);
    return modifiedMemLimits;
}

bool CustomOfflinePartition::PrepareIntervalTask()
{
    if (!mTaskScheduler) {
        IE_LOG(ERROR, "TaskScheduler is nullptr in table[%s]", mSchema->GetSchemaName().c_str());
        return false;
    }

    int32_t sleepTime = REPORT_METRICS_INTERVAL;
    if (autil::EnvUtil::getEnv("TEST_QUICK_EXIT", false)) {
        sleepTime /= 1000;
    }

    if (mEnableAsyncDump) {
        mAsyncDumpThreadPtr =
            autil::Thread::createThread(std::bind(&CustomOfflinePartition::AsyncDumpThread, this), "asyncDumpThread");
        if (!mAsyncDumpThreadPtr) {
            IE_LOG(ERROR, "create asyncDumpThread failed in table[%s]", mSchema->GetSchemaName().c_str());
            return false;
        }
    }
    if (!mDisableBackgroundThread) {
        mBackGroundThreadPtr =
            autil::Thread::createThread(std::bind(&CustomOfflinePartition::BackGroundThread, this), "indexBackground");
        if (!mBackGroundThreadPtr) {
            IE_LOG(ERROR, "create BackGroundThread failed in table[%s]", mSchema->GetSchemaName().c_str());
            return false;
        }
    }
    return true;
}

void CustomOfflinePartition::CleanIntervalTask()
{
    mAsyncDumpThreadPtr.reset();
    mBackGroundThreadPtr.reset();
}

bool CustomOfflinePartition::InitPlugin(const string& pluginPath, const IndexPartitionSchemaPtr& schema,
                                        const IndexPartitionOptions& options)
{
    mPluginManager = TablePluginLoader::Load(pluginPath, schema, options);
    if (!mPluginManager) {
        IE_LOG(ERROR, "load index plugin failed for schema [%s]", schema->GetSchemaName().c_str());
        return false;
    }
    return true;
}

IndexPartition::OpenStatus CustomOfflinePartition::ReOpen(bool forceReopen, versionid_t reopenVersionId)
{
    assert(false);
    return OS_FAIL;
}

void CustomOfflinePartition::ReOpenNewSegment()
{
    assert(mWriter);
    mWriter->ReOpenNewSegment(PartitionModifierPtr());
}

void CustomOfflinePartition::Close()
{
    if (mClosed) {
        return;
    }

    mClosed = true;
    CleanIntervalTask();
    if (mWriter) {
        try {
            mWriter->Close(); // will trigger dump version
        } catch (...) {
            throw;
        }
    }
    // reset sequence: writer -> partitionData -> fileSystsm
    mWriter.reset();
    FlushDumpSegmentQueue();
    mPartitionDataHolder.Reset();
    if (mFileSystem) {
        mFileSystem->Sync(true).GetOrThrow();
    }
    mFileSystem.reset();
    mTableFactory.reset();
    mTableWriterMemController.reset();
    IndexPartition::Close();
}

PartitionWriterPtr CustomOfflinePartition::GetWriter() const { return mWriter; }

IndexPartitionReaderPtr CustomOfflinePartition::GetReader() const
{
    // TODO: support GetReader in future
    return IndexPartitionReaderPtr();
}

void CustomOfflinePartition::ValidateConfigAndParam(const IndexPartitionSchemaPtr& schemaInConfig,
                                                    const IndexPartitionSchemaPtr& schemaOnDisk,
                                                    const IndexPartitionOptions& options)
{
    if (schemaInConfig) {
        schemaOnDisk->AssertCompatible(*schemaInConfig);
    }

    if (!options.IsOffline()) {
        INDEXLIB_FATAL_ERROR(BadParameter, "CustomOfflinePartition only support offline option!");
    }

    options.Check();
    if (!schemaOnDisk) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "schema is NULL");
    }
    schemaOnDisk->Check();

    if (schemaOnDisk->GetTableType() != TableType::tt_customized) {
        INDEXLIB_FATAL_ERROR(BadParameter, "CustomOfflinePartition only support tableType[tt_customized]!");
    }
}

bool CustomOfflinePartition::ValidateSchema(const IndexPartitionSchemaPtr& schema)
{
    assert(false);
    return false;
}

bool CustomOfflinePartition::InitPartitionData(const file_system::IFileSystemPtr& fileSystem, Version& version,
                                               const CounterMapPtr& counterMap, const PluginManagerPtr& pluginManager)
{
    if (version.GetVersionId() == INVALID_VERSIONID) {
        indexlibv2::framework::LevelInfo& levelInfo = version.GetLevelInfo();
        const BuildConfig& buildConfig = mOptions.GetBuildConfig();
        levelInfo.Init(buildConfig.levelTopology, buildConfig.levelNum, buildConfig.shardingColumnNum);
    }
    mPartitionDataHolder.Reset(PartitionDataCreator::CreateCustomPartitionData(
        fileSystem, mSchema, mOptions, mPartitionMemController, mTableWriterMemController, version, nullptr,
        mMetricProvider, counterMap, pluginManager, mDumpSegmentQueue));

    if (!mPartitionDataHolder.Get()) {
        IE_LOG(ERROR, "create partitionData failed for schema[%s]", mSchema->GetSchemaName().c_str());
        return false;
    }
    return true;
}

bool CustomOfflinePartition::ValidatePartitionMeta(const PartitionMeta& partitionMeta) const
{
    // CustomOfflinePartition does not support sortBuild, check no sortDescription is defined
    if (partitionMeta.Size() != 0) {
        IE_LOG(ERROR, "CustomOfflinePartition does not support sortBuild");
        return false;
    }
    return true;
}

void CustomOfflinePartition::InitWriter(const PartitionDataPtr& partitionData)
{
    mWriter.reset(new CustomOfflinePartitionWriter(mOptions, mTableFactory, mFlushedLocatorContainer, mMetricProvider,
                                                   mPartitionName, mPartitionRange));
    mWriter->Open(mSchema, partitionData, PartitionModifierPtr());
}

void CustomOfflinePartition::BackGroundThread()
{
    IE_LOG(DEBUG, "offline report metrics thread start");
    static int32_t sleepTime = REPORT_METRICS_INTERVAL;
    if (unlikely(autil::EnvUtil::getEnv("TEST_QUICK_EXIT", false))) {
        sleepTime /= 1000;
    }
    while (!mClosed) {
        assert(mFileSystem);
        mFileSystem->ReportMetrics();
        mWriter->ReportMetrics();
        usleep(sleepTime);
    }
    IE_LOG(DEBUG, "offline report metrics thread exit");
}

void CustomOfflinePartition::AsyncDumpThread()
{
    IE_LOG(INFO, "async dump thread start");
    static int32_t sleepTime =
        autil::EnvUtil::getEnv("TEST_QUICK_TEST", false) ? REPORT_METRICS_INTERVAL / 1000 : REPORT_METRICS_INTERVAL;

    while (!mClosed) {
        FlushDumpSegmentQueue();
        usleep(sleepTime);
    }
    IE_LOG(INFO, "async dump thread exit");
}

void CustomOfflinePartition::FlushDumpSegmentQueue()
{
    static bool hasFatalError = false;
    ScopedLock lock(mAsyncDumpLock);
    int64_t latency = 0;

    if (hasFatalError) {
        IE_LOG(ERROR, "FlushDumpSegmentQueue aborted due to previous fatalError");
        return;
    }

    uint32_t dumpCounter = 0;
    {
        ScopedTime timer(latency);
        while (mDumpSegmentQueue && (mDumpSegmentQueue->Size() > 0)) {
            bool hasDumpedSegment = false;
            bool dumpSuccess = mDumpSegmentQueue->ProcessOneItem(hasDumpedSegment);

            if (!dumpSuccess) {
                IE_LOG(ERROR, "dump segment failed!");
                hasFatalError = true;
                INDEXLIB_FATAL_ERROR(Runtime, "DumpSegment failed in FlushDumpSegmentQueue");
            }
            if (hasDumpedSegment) {
                dumpCounter++;
                mPartitionDataHolder.Get()->CommitVersion();
                mDumpSegmentQueue->PopDumpedItems();
            } else {
                break;
            }
        }
    }
    if (dumpCounter > 0) {
        IE_LOG(INFO, "flush dump segment items done , total count [%u], time elapsed [%ld ms].", dumpCounter,
               latency / 1000);
    }
    return;
}

bool CustomOfflinePartition::NeedReclaimMemory() const
{
    if (mClosed) {
        return false;
    }

    if (mDumpSegmentQueue && mTableWriterMemController) {
        int64_t estimateDumpSize = mDumpSegmentQueue->GetEstimateDumpSize();
        int64_t estimateTableWriterMemUse = mTableWriterMemController->GetUsedQuota();
        int64_t totalMemUseInBytes = estimateDumpSize + estimateTableWriterMemUse;

        bool reachMemoryQuota = totalMemUseInBytes >= mMemLimitInBytes;

        if (reachMemoryQuota) {
            IE_LOG(WARN,
                   "Reach MemoryLimit[%ld], estimateDumpSize = [%ld], "
                   "estimateTableWriterMemUse = [%ld], dumpSegmentQueueSize = [%zu]",
                   mMemLimitInBytes, estimateDumpSize, estimateTableWriterMemUse, mDumpSegmentQueue->Size());
        }
        if (reachMemoryQuota && mDumpSegmentQueue->Size() > 0) {
            return true;
        }
    }
    return false;
}

void CustomOfflinePartition::ExecuteBuildMemoryControl()
{
    if (NeedReclaimMemory()) {
        ScopedLock dataLock(mDataLock);
        IE_LOG(INFO, "reclaim memory by flushing dumpSegmentQueue, current queue size = [%zu]",
               mDumpSegmentQueue->Size());
        FlushDumpSegmentQueue();
    }
}
}} // namespace indexlib::partition
