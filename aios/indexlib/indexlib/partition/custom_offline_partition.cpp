#include "indexlib/partition/custom_offline_partition.h"
#include "indexlib/partition/custom_offline_partition_writer.h"
#include "indexlib/partition/index_partition_resource.h"
#include "indexlib/partition/segment/dump_segment_container.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index_base/index_meta/parallel_build_info.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/index_base/index_meta/index_format_version.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/indexlib_file_system.h"
#include "indexlib/table/table_factory_wrapper.h"
#include "indexlib/table/table_plugin_loader.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/util/counter/counter_map.h"
#include "indexlib/util/memory_control/block_memory_quota_controller.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(plugin);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(table);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, CustomOfflinePartition);

CustomOfflinePartition::CustomOfflinePartition(
    const string& partitionName, const IndexPartitionResource& partitionResource)
    : IndexPartition(partitionName, partitionResource)
    , mParallelBuildInfo(partitionResource.parallelBuildInfo)
    , mClosed(true)
    , mDisableBackgroundThread(false)
    , mPartitionRange(partitionResource.range)
{
}
                                               
CustomOfflinePartition::~CustomOfflinePartition() 
{
    mClosed = true;
    mBackGroundThreadPtr.reset();
    mWriter.reset();
    IndexPartition::Reset();
    mTableFactory.reset();
}

string CustomOfflinePartition::GetLastLocator() const
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
    return partitionData ? partitionData->GetLastLocator() : string(""); 
}    

IndexPartition::OpenStatus CustomOfflinePartition::Open(const std::string& primaryDir,
                const std::string& secondaryDir,
                const config::IndexPartitionSchemaPtr& schema, 
                const config::IndexPartitionOptions& options,
                versionid_t versionId)
{
    IndexPartition::OpenStatus os = OS_FAIL;
    try
    {
        os = DoOpen(primaryDir, schema, options, versionId);
    }
    catch (const FileIOException& e)
    {
        IE_LOG(ERROR, "open caught file io exception: %s", e.what());
        os = OS_FILEIO_EXCEPTION;
    }
    catch (const ExceptionBase& e)
    {
        // IndexCollapsedException, UnSupportedException, 
        // BadParameterException, InconsistentStateException, ...
        IE_LOG(ERROR, "open caught exception: %s", e.what());
        os = OS_INDEXLIB_EXCEPTION;
    }
    catch (const exception& e)
    {
        IE_LOG(ERROR, "open caught std exception: %s", e.what());
        os = OS_UNKNOWN_EXCEPTION;
    }
    catch (...)
    {
        IE_LOG(ERROR, "open caught unknown exception");
        os = OS_UNKNOWN_EXCEPTION;
    }
    return os;    
}

IndexPartition::OpenStatus CustomOfflinePartition::DoOpen(
        const string& dir, const IndexPartitionSchemaPtr& schema,
        const IndexPartitionOptions& options, versionid_t versionId)
{
    std::string secondaryDir;    
    mRootDir = dir;
    if (mParallelBuildInfo.IsParallelBuild())
    {
        secondaryDir = dir;
        mRootDir = mParallelBuildInfo.GetParallelInstancePath(dir);
    }
    mParallelBuildInfo.CheckValid(mRootDir);    
    bool isNewRootDir = MakeRootDir(dir);
    IndexPartition::OpenStatus openStatus = IndexPartition::Open(
            mRootDir, secondaryDir, schema, options, versionId);
    if (openStatus != OS_OK)
    {
        return openStatus;
    }
    mOptions.GetOfflineConfig().recoverType = RecoverStrategyType::RST_VERSION_LEVEL;
    Version version;
    version.SyncSchemaVersionId(schema);

    // read schema for partition path
    bool isEmptyDir = isNewRootDir;
    if (!isNewRootDir)
    {
        CleanIndexDir(dir, schema, isEmptyDir);
    }
    if (isEmptyDir)
    {
        //full build
        if (!schema)
        {
            INDEXLIB_FATAL_ERROR(BadParameter,
                    "Invalid parameter, schema is empty");
        }
        mSchema.reset(schema->Clone());
        if (mParallelBuildInfo.IsParallelBuild() && !IsFirstIncParallelInstance())
        {
            INDEXLIB_FATAL_ERROR(InitializeFailed,
                    "inc parallel build from empty parent path [%s], "
                    "wait instance[0] init parent path!", dir.c_str());
        }
    }
    else
    {
        if (versionId != INVALID_VERSION)
        {
            IE_LOG(ERROR, "CustomOfflinePartition does not support open with specified version")
            return OS_FAIL;
        }
        // inc build
        VersionLoader::GetVersion(dir, version, versionId);
        mSchema = SchemaAdapter::LoadSchema(dir, version.GetSchemaVersionId());
    }

    ValidateConfigAndParam(schema, mSchema, mOptions);

    mTableWriterMemController.reset(new BlockMemoryQuotaController(
        mPartitionMemController, mSchema->GetSchemaName() + "_TableWriterMemory"));

    if (!InitPlugin(mIndexPluginPath, mSchema, mOptions))
    {
        return OS_FAIL;
    }

    if (isEmptyDir) {
        InitEmptyIndexDir(dir);
    }

    if (mParallelBuildInfo.IsParallelBuild())
    {
        mParallelBuildInfo.SetBaseVersion(version.GetVersionId());
        mParallelBuildInfo.StoreIfNotExist(mRootDir);
    }

    mCounterMap.reset(new CounterMap());    
    mDumpSegmentQueue.reset(new DumpSegmentQueue());
    
    if (!InitPartitionData(mFileSystem, version, mCounterMap, mPluginManager))
    {
        return OS_FAIL;
    }

    PartitionDataPtr partitionData = mPartitionDataHolder.Get();
    
    if (!ValidatePartitionMeta(partitionData->GetPartitionMeta()))
    {
        return OS_FAIL;
    }

    if (!mTableFactory)
    {
        mTableFactory.reset(new TableFactoryWrapper(
                                mSchema, mOptions, partitionData->GetPluginManager()));
        if (!mTableFactory->Init())
        {
            IE_LOG(ERROR, "Init TableFactory failed in table[%s]",
                   mSchema->GetSchemaName().c_str());
            return OS_FAIL;
        }
    }
    InitWriter(partitionData);
    mClosed = false;
    if (!mDisableBackgroundThread)
    {
        mBackGroundThreadPtr = Thread::createThread(
                tr1::bind(&CustomOfflinePartition::BackGroundThread, this), "indexBackground");
    }
    return OS_OK;
}

bool CustomOfflinePartition::InitPlugin(const string& pluginPath,
                                        const IndexPartitionSchemaPtr& schema,
                                        const IndexPartitionOptions& options)
{
    mPluginManager = TablePluginLoader::Load(pluginPath, schema, options);
    if (!mPluginManager)
    {
        IE_LOG(ERROR, "load index plugin failed for schema [%s]",
               schema->GetSchemaName().c_str());
        return false;;
    }
    return true;
}

IndexPartition::OpenStatus CustomOfflinePartition::ReOpen(
    bool forceReopen, versionid_t reopenVersionId)
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
    if (mClosed)
    {
        return;
    }

    mClosed = true;
    mBackGroundThreadPtr.reset();
    if (mWriter)
    {
        try
        {
            mWriter->Close(); // will trigger dump version
        }
        catch (...)
        {
            throw;
        }
    }
    // reset sequence: writer -> partitionData -> fileSystsm
    mWriter.reset();
    mPartitionDataHolder.Reset();
    if (mFileSystem)
    {
        mFileSystem->Sync(true);
    }
    mFileSystem.reset();
    mTableFactory.reset();
    mTableWriterMemController.reset();
    IndexPartition::Close();
}

PartitionWriterPtr CustomOfflinePartition::GetWriter() const
{
    return mWriter;
}

IndexPartitionReaderPtr CustomOfflinePartition::GetReader() const
{
    // TODO: support GetReader in future
    return IndexPartitionReaderPtr();
}

bool CustomOfflinePartition::MakeRootDir(const string& rootDir)
{
    if (rootDir.empty())
    {
        INDEXLIB_FATAL_ERROR(BadParameter, "Invalid parameter, dir is empty");
    }
    if (!FileSystemWrapper::IsExist(rootDir))
    {
        FileSystemWrapper::MkDir(rootDir, true);
        return true;
    }
    return false;
}

void CustomOfflinePartition::CleanIndexDir(
    const string& dir, const IndexPartitionSchemaPtr& schema, bool& isEmptyDir) const
{
    Version version;
    VersionLoader::GetVersion(dir, version, INVALID_VERSION);
    if (version.GetVersionId() != INVALID_VERSION)
    {
        isEmptyDir = false;
        return;
    }
    
    schemavid_t schemaId = DEFAULT_SCHEMAID;
    if (schema)
    {
        schemaId = schema->GetSchemaVersionId();
    }
    
    string schemaPath = FileSystemWrapper::JoinPath(
            dir, Version::GetSchemaFileName(schemaId));
    string formatVersionPath = 
        FileSystemWrapper::JoinPath(dir, INDEX_FORMAT_VERSION_FILE_NAME);

    bool schemaExist = FileSystemWrapper::IsExist(schemaPath);
    bool formatVersionExist = FileSystemWrapper::IsExist(formatVersionPath);

    if (schemaExist && formatVersionExist)
    {
        isEmptyDir = false;
        return;
    }

    if (!schemaExist && !formatVersionExist)
    {
        isEmptyDir = true;
        return;
    }

    if (mParallelBuildInfo.IsParallelBuild() && !IsFirstIncParallelInstance())
    {
        IE_LOG(WARN, "non-first inc parallel build instance "
               "not allow to operate schema & formatVersion file!");
        isEmptyDir = true;
        return;
    }

    if (formatVersionExist)
    {
        IE_LOG(WARN, "only format version file exist, remove [%s]",
               formatVersionPath.c_str());
        FileSystemWrapper::DeleteFile(formatVersionPath);
    }

    if (schemaExist)
    {
        IE_LOG(WARN, "only schema exist, remove [%s]", schemaPath.c_str());
        FileSystemWrapper::DeleteFile(schemaPath);
    }
    isEmptyDir = true;
    return;
}

void CustomOfflinePartition::ValidateConfigAndParam(const IndexPartitionSchemaPtr& schemaInConfig,
                                                    const IndexPartitionSchemaPtr& schemaOnDisk,
                                                    const IndexPartitionOptions& options)
{
    if (schemaInConfig)
    {
        schemaOnDisk->AssertCompatible(*schemaInConfig); 
    }
    
    if (!options.IsOffline())
    {
        INDEXLIB_FATAL_ERROR(BadParameter,
                             "CustomOfflinePartition only support offline option!");
    }
    
    options.Check();
    if (!schemaOnDisk)
    {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "schema is NULL");
    }
    schemaOnDisk->Check();

    if (schemaOnDisk->GetTableType() != TableType::tt_customized) {
        INDEXLIB_FATAL_ERROR(BadParameter,
                             "CustomOfflinePartition only support tableType[tt_customized]!"); 
    }
}
void CustomOfflinePartition::InitEmptyIndexDir(const string& dir)
{
    // TODO: supports user-difined IndexFormatVersion
    IndexFormatVersion binaryVersion(INDEX_FORMAT_VERSION);
    binaryVersion.Store(FileSystemWrapper::JoinPath(dir, 
                    INDEX_FORMAT_VERSION_FILE_NAME));

    string normalizeDir = FileSystemWrapper::NormalizeDir(dir);
    SchemaAdapter::StoreSchema(normalizeDir, mSchema);    
}
bool CustomOfflinePartition::ValidateSchema(const IndexPartitionSchemaPtr& schema)
{
    assert(false);
    return false;
}

bool CustomOfflinePartition::InitPartitionData(
    const file_system::IndexlibFileSystemPtr& fileSystem, 
    Version& version,
    const CounterMapPtr& counterMap,
    const PluginManagerPtr& pluginManager)
{
    if (version.GetVersionId() == INVALID_VERSION)
    {
        LevelInfo& levelInfo = version.GetLevelInfo();
        const BuildConfig& buildConfig = mOptions.GetBuildConfig();
        levelInfo.Init(buildConfig.levelTopology, buildConfig.levelNum,
                       buildConfig.shardingColumnNum);
    }
    mPartitionDataHolder.Reset(PartitionDataCreator::CreateCustomPartitionData(
                                   fileSystem, mSchema, mOptions, mPartitionMemController, 
                                   mTableWriterMemController, version, mMetricProvider, 
                                   counterMap, pluginManager, mDumpSegmentQueue));

    if (!mPartitionDataHolder.Get())
    {
        IE_LOG(ERROR, "create partitionData failed for schema[%s]",
               mSchema->GetSchemaName().c_str());
        return false;
    }
    return true;
}

bool CustomOfflinePartition::ValidatePartitionMeta(const PartitionMeta& partitionMeta) const
{
    // CustomOfflinePartition does not support sortBuild, check no sortDescription is defined
    if (partitionMeta.Size() != 0)
    {
        IE_LOG(ERROR, "CustomOfflinePartition does not support sortBuild");
        return false;
    }
    return true;
}

void CustomOfflinePartition::InitWriter(const PartitionDataPtr& partitionData)
{
    mWriter.reset(new CustomOfflinePartitionWriter(
                      mOptions, mTableFactory, mFlushedLocatorContainer,
                      mMetricProvider, mPartitionName, mPartitionRange));
    mWriter->Open(mSchema, partitionData, PartitionModifierPtr());
}

void CustomOfflinePartition::BackGroundThread()
{
    IE_LOG(DEBUG, "offline report metrics thread start");
    int32_t sleepTime = INDEXLIB_REPORT_METRICS_INTERVAL;
    if (unlikely(mOptions.TEST_mQuickExit))
    {
        sleepTime /= 1000;
    }

    while (!mClosed)
    {
        assert(mFileSystem);
        mFileSystem->ReportMetrics();
        mWriter->ReportMetrics();
        usleep(sleepTime);
    }
    IE_LOG(DEBUG, "offline report metrics thread exit");
}

IE_NAMESPACE_END(partition);

