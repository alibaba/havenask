#include <autil/legacy/json.h>
#include <autil/StringTokenizer.h>
#include <autil/legacy/string_tools.h>
#include "indexlib/partition/offline_partition.h"
#include "indexlib/partition/table_reader_container_updater.h"
#include "indexlib/partition/index_partition_resource.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/partition/offline_partition_writer.h"
#include "indexlib/partition/modifier/partition_modifier_creator.h"
#include "indexlib/partition/segment/in_memory_segment_releaser.h"
#include "indexlib/partition/offline_partition_reader.h"
#include "indexlib/partition/segment/in_memory_segment_container.h"
#include "indexlib/partition/segment/in_memory_segment_cleaner.h"
#include "indexlib/partition/segment/dump_segment_container.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/index_base/segment/segment_iterator.h"
#include "indexlib/index_base/segment/partition_segment_iterator.h"
#include "indexlib/index_base/index_meta/index_format_version.h"
#include "indexlib/index_base/meta_cache_preloader.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_plugin_resource.h"
#include "indexlib/misc/exception.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/load_config/cache_load_strategy.h"
#include "indexlib/file_system/indexlib_file_system.h"
#include "indexlib/plugin/index_plugin_loader.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/util/cache/search_cache_partition_wrapper.h"
#include "indexlib/util/memory_control/memory_quota_controller_creator.h"
#include "indexlib/util/memory_control/memory_reserver.h"
#include "indexlib/util/counter/counter_map.h"
#include "indexlib/util/task_scheduler.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(plugin);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, OfflinePartition);

OfflinePartition::OfflinePartition(const string& partitionName, const IndexPartitionResource& partitionResource)
    : IndexPartition(partitionName, partitionResource)
    , mClosed(true)
    , mDumpSegmentContainer(new DumpSegmentContainer)
    , mInMemSegCleaner(mDumpSegmentContainer->GetInMemSegmentContainer())
    , mParallelBuildInfo(partitionResource.parallelBuildInfo)
    , mPartitionRange(partitionResource.range)
{
    IE_INIT_METRIC_GROUP(mMetricProvider, oldInMemorySegmentMemoryUse,
                         "build/oldInMemorySegmentMemoryUse", kmonitor::GAUGE, "byte");
}

OfflinePartition::OfflinePartition(const string& partitionName,
                                   const MemoryQuotaControllerPtr& memQuotaController,
                                   MetricProviderPtr metricProvider)
    : IndexPartition(memQuotaController, partitionName)
    , mClosed(true)
    , mDumpSegmentContainer(new DumpSegmentContainer)
    , mInMemSegCleaner(mDumpSegmentContainer->GetInMemSegmentContainer())
{
    mMetricProvider = metricProvider;
    IE_INIT_METRIC_GROUP(metricProvider, oldInMemorySegmentMemoryUse,
                         "build/oldInMemorySegmentMemoryUse", kmonitor::GAUGE, "byte");
    mTaskScheduler.reset(new TaskScheduler());
}

OfflinePartition::OfflinePartition(MetricProviderPtr metricProvider)
    : OfflinePartition("", MemoryQuotaControllerCreator::CreateMemoryQuotaController(),
                       metricProvider)
{
    IE_INIT_METRIC_GROUP(metricProvider, oldInMemorySegmentMemoryUse,
                         "build/oldInMemorySegmentMemoryUse", kmonitor::GAUGE, "byte");
    mTaskScheduler.reset(new TaskScheduler());
}

OfflinePartition::~OfflinePartition()
{
    mClosed = true;
    mBackGroundThreadPtr.reset();
    mWriter.reset();
    IndexPartition::Reset();
}

IndexPartition::OpenStatus OfflinePartition::Open(
        const string& primaryDir, const string& secondaryDir,
        const IndexPartitionSchemaPtr& schema,
        const IndexPartitionOptions& options, versionid_t versionId)
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

IndexPartition::OpenStatus OfflinePartition::DoOpen(
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

    Version version;
    version.SyncSchemaVersionId(schema);
    // read schema for partition path
    if (isNewRootDir || IsEmptyDir(dir, schema))
    {
        //full build
        if (!schema)
        {
            INDEXLIB_FATAL_ERROR(BadParameter,
                    "Invalid parameter, schema is empty");
        }
        mSchema.reset(schema->Clone());
        SchemaAdapter::RewritePartitionSchema(mSchema, dir, mOptions);
        CheckParam(mSchema, mOptions);

        if (mParallelBuildInfo.IsParallelBuild() && !IsFirstIncParallelInstance())
        {
            INDEXLIB_FATAL_ERROR(InitializeFailed,
                    "inc parallel build from empty parent path [%s], "
                    "wait instance[0] init parent path!", dir.c_str());
        }
        StoreSchemaAndFormatVersionFile(dir);
    }
    else
    {
        if (versionId != INVALID_VERSION)
        {
            // offline partition reader
            mOptions.GetOfflineConfig().enableRecoverIndex = false;
        }
        // inc build
        VersionLoader::GetVersion(dir, version, versionId);
        MetaCachePreloader::Load(dir, version.GetVersionId());
        mSchema = SchemaAdapter::LoadAndRewritePartitionSchema(
                dir, mOptions, version.GetSchemaVersionId());
        CheckParam(mSchema, mOptions);
    }
    if (schema)
    {
        if (schema->HasModifyOperations())
        {
            mSchema->EnableThrowAssertCompatibleException();
        }
        mSchema->AssertCompatible(*schema);
    }

    if (schema && schema->HasModifyOperations() &&
        schema->GetSchemaVersionId() > mSchema->GetSchemaVersionId())
    {
        // use new schema
        IndexPartitionSchemaPtr newSchema(schema->Clone());
        SchemaAdapter::RewritePartitionSchema(newSchema, dir, mOptions);
        CheckParam(newSchema, mOptions);
        string normalizeDir = FileSystemWrapper::NormalizeDir(dir);
        string schemaFilePath = FileSystemWrapper::JoinPath(
                dir, Version::GetSchemaFileName(newSchema->GetSchemaVersionId()));
        if (!FileSystemWrapper::IsExist(schemaFilePath))
        {
            if (mParallelBuildInfo.IsParallelBuild() && !IsFirstIncParallelInstance())
            {
                INDEXLIB_FATAL_ERROR(InitializeFailed,
                        "inc parallel build from lagecy schema parent path [%s], "
                        "wait instance[0] init new schema file in parent path!", dir.c_str());
            }
            SchemaAdapter::StoreSchema(normalizeDir, newSchema);
        }
        mSchema = newSchema;
    }

    mPluginManager = IndexPluginLoader::Load(mIndexPluginPath,
            mSchema->GetIndexSchema(), mOptions);
    if (!mPluginManager)
    {
        IE_LOG(ERROR, "load index plugin failed for schema [%s]",
               mSchema->GetSchemaName().c_str());
        return OS_FAIL;
    }

    if (mParallelBuildInfo.IsParallelBuild())
    {
        mParallelBuildInfo.SetBaseVersion(version.GetVersionId());
        mParallelBuildInfo.StoreIfNotExist(mRootDir);
    }

    mCounterMap.reset(new CounterMap());
    InitPartitionData(mRootDir, secondaryDir, version, mCounterMap, mPluginManager);
    PartitionMeta partitionMeta = mPartitionDataHolder.Get()->GetPartitionMeta();
    CheckPartitionMeta(mSchema, partitionMeta);

    PluginResourcePtr resource(new IndexPluginResource(
                    mSchema, mOptions, mCounterMap, partitionMeta, mIndexPluginPath));
    mPluginManager->SetPluginResource(resource);

    if (!mOptions.GetOfflineConfig().enableRecoverIndex)
    {
        // for remote read purpose, no need init writer. etc
        mClosed = false;
        return OS_OK;
    }
    InitWriter(mPartitionDataHolder.Get());
    mClosed = false;
    mBackGroundThreadPtr = Thread::createThread(
            tr1::bind(&OfflinePartition::BackGroundThread, this), "indexBackground");
    return OS_OK;
}

void OfflinePartition::InitPartitionData(
        const string& dir, const string& secondaryDir, Version& version,
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

    mPartitionDataHolder.Reset(PartitionDataCreator::CreateBuildingPartitionData(
                    mFileSystem, mSchema, mOptions, mPartitionMemController,
                    mDumpSegmentContainer, version, mMetricProvider, "",
                    InMemorySegmentPtr(), counterMap, pluginManager));
}

IndexlibFileSystemPtr OfflinePartition::CreateFileSystem(
        const string& primaryDir, const string& secondaryDir)
{
    RewriteLoadConfigs(mOptions);
    return IndexPartition::CreateFileSystem(primaryDir, secondaryDir);
}

void OfflinePartition::RewriteLoadConfigs(IndexPartitionOptions& options) const
{
    LoadConfigList offlineLoadConfigList = options.GetOfflineConfig().GetLoadConfigList();
    if (offlineLoadConfigList.Size() != 0) {
        //todo refactor
        options.GetOnlineConfig().loadConfigList = offlineLoadConfigList;
        return;
    }
    LoadConfigList& loadConfigList = options.GetOnlineConfig().loadConfigList;
    loadConfigList.Clear();

    if (options.GetOfflineConfig().readerConfig.useBlockCache)
    {

        LoadStrategyPtr indexCacheLoadStrategy;
        LoadStrategyPtr summaryCacheLoadStrategy;
        if (mFileBlockCache)
        {
            // Global Cache
            indexCacheLoadStrategy.reset(new CacheLoadStrategy(false, true, false));

            summaryCacheLoadStrategy.reset(new CacheLoadStrategy(false, true, false));
        }
        else
        {
            indexCacheLoadStrategy.reset(
                new CacheLoadStrategy(options.GetOfflineConfig().readerConfig.indexCacheSize,
                    CACHE_DEFAULT_BLOCK_SIZE, false, false, false));

            summaryCacheLoadStrategy.reset(
                new CacheLoadStrategy(options.GetOfflineConfig().readerConfig.summaryCacheSize,
                    CACHE_DEFAULT_BLOCK_SIZE, false, false, false));
        }

        LoadConfig indexLoadConfig;
        LoadConfig summaryLoadConfig;
        LoadConfig::FilePatternStringVector summaryPattern;
        summaryPattern.push_back("_SUMMARY_");
        summaryLoadConfig.SetFilePatternString(summaryPattern);
        summaryLoadConfig.SetLoadStrategyPtr(summaryCacheLoadStrategy);
        summaryLoadConfig.SetName("__OFFLINE_SUMMARY__");

        LoadConfig::FilePatternStringVector indexPattern;
        indexPattern.push_back("_KV_KEY_");
        indexPattern.push_back("_KV_VALUE_");
        indexPattern.push_back("_KKV_PKEY_");
        indexPattern.push_back("_KKV_SKEY_");
        indexPattern.push_back("_KKV_VALUE_");
        indexPattern.push_back("_INDEX_");
        indexLoadConfig.SetFilePatternString(indexPattern);
        indexLoadConfig.SetLoadStrategyPtr(indexCacheLoadStrategy);
        indexLoadConfig.SetName("__OFFLINE_INDEX__");

        loadConfigList.PushFront(indexLoadConfig);
        loadConfigList.PushFront(summaryLoadConfig);
    }
}

void OfflinePartition::ReOpenNewSegment()
{
    PartitionDataPtr partData = mPartitionDataHolder.Get();
    InMemorySegmentPtr oldInMemSegment = partData->GetInMemorySegment();
    partData->CreateNewSegment();
    PartitionModifierPtr modifier = CreateModifier(partData);
    mWriter->ReOpenNewSegment(modifier);
}

//for indexbuilder end version
IndexPartition::OpenStatus OfflinePartition::ReOpen(
        bool forceReopen, versionid_t reopenVersionId)
{
    assert(false);
    return OS_FAIL;
}

void OfflinePartition::Close()
{
    if (mClosed)
    {
        return;
    }

    if (mWriter)
    {
        try
        {
            mWriter->Close(); // will trigger dump version
        }
        catch (...)
        {
            mClosed = true;
            throw;
        }
    }

    mClosed = true;
    mBackGroundThreadPtr.reset();
    // reset sequence: writer -> partitionData -> fileSystsm
    mWriter.reset();
    mPartitionDataHolder.Reset();
    if (mFileSystem)
    {
        mFileSystem->Sync(true);
    }
    mFileSystem.reset();
    IndexPartition::Close();
}

PartitionWriterPtr OfflinePartition::GetWriter() const
{
    return mWriter;
}

IndexPartitionReaderPtr OfflinePartition::GetReader() const
{
    ScopedLock lock(mReaderLock);
    if (mReader)
    {
        return mReader;
    }

    PartitionDataPtr partData = mPartitionDataHolder.Get();
    if (!partData)
    {
        IE_LOG(ERROR, "cannot create reader when partitionData is empty");
        return IndexPartitionReaderPtr();
    }
    Version version = partData->GetVersion();
    mReader.reset(new partition::OfflinePartitionReader(mOptions, mSchema,
                    CreateSearchCacheWrapper()));
    PartitionDataPtr clonedPartitionData(partData->Clone());
    try
    {
        mReader->Open(clonedPartitionData);
    }
    catch (...)
    {
        return IndexPartitionReaderPtr();
    }
    IE_LOG(INFO, "End open offline partition reader (version=[%d])",
           version.GetVersionId());
    return mReader;
}

OfflinePartitionWriterPtr OfflinePartition::CreatePartitionWriter()
{
    return OfflinePartitionWriterPtr(
        new OfflinePartitionWriter(
            mOptions, mDumpSegmentContainer,
            mFlushedLocatorContainer, mMetricProvider, mPartitionName, mPartitionRange));
}

void OfflinePartition::StoreSchemaAndFormatVersionFile(
        const std::string& dir)
{
    IndexFormatVersion binaryVersion(INDEX_FORMAT_VERSION);
    binaryVersion.Store(FileSystemWrapper::JoinPath(dir,
                    INDEX_FORMAT_VERSION_FILE_NAME));

    string normalizeDir = FileSystemWrapper::NormalizeDir(dir);
    SchemaAdapter::StoreSchema(normalizeDir, mSchema);
}

void OfflinePartition::InitWriter(const PartitionDataPtr& partData)
{
    assert(partData);
    partData->CreateNewSegment();
    PartitionModifierPtr modifier = CreateModifier(partData);
    mWriter = CreatePartitionWriter();
    assert(mWriter);
    mWriter->Open(mSchema, partData, modifier);
}

PartitionModifierPtr OfflinePartition::CreateModifier(
        const PartitionDataPtr& partData)
{
    PartitionModifierPtr modifier =
        PartitionModifierCreator::CreateOfflineModifier(
                mSchema, partData, true,
                mOptions.GetBuildConfig().enablePackageFile);
    return modifier;
}

bool OfflinePartition::MakeRootDir(const string& rootDir)
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

bool OfflinePartition::IsEmptyDir(
        const string& dir, const IndexPartitionSchemaPtr& schema) const
{
    Version version;
    VersionLoader::GetVersion(dir, version, INVALID_VERSION);
    if (version.GetVersionId() != INVALID_VERSION)
    {
        return false;
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
        return false;
    }

    if (!schemaExist && !formatVersionExist)
    {
        return true;
    }

    if (mParallelBuildInfo.IsParallelBuild() && !IsFirstIncParallelInstance())
    {
        IE_LOG(WARN, "non-first inc parallel build instance "
               "not allow to operate schema & formatVersion file!");
        return true;
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
    return true;
}

void OfflinePartition::CheckParam(const IndexPartitionSchemaPtr& schema,
                                  const IndexPartitionOptions& options)
{
    if (!options.IsOffline())
    {
        INDEXLIB_FATAL_ERROR(BadParameter,
                             "OfflinePartition only support offline option!");
    }

    options.Check();
    if (!schema)
    {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "schema is NULL");
    }
    schema->Check();
}

void OfflinePartition::BackGroundThread()
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
        IE_REPORT_METRIC(oldInMemorySegmentMemoryUse,
                         mDumpSegmentContainer->GetInMemSegmentContainer()->GetTotalMemoryUse());
        mInMemSegCleaner.Execute();
        usleep(sleepTime);
    }
    IE_LOG(DEBUG, "offline report metrics thread exit");
}

SearchCachePartitionWrapperPtr OfflinePartition::CreateSearchCacheWrapper() const
{
    if (!mOptions.GetOfflineConfig().readerConfig.useSearchCache)
    {
        return SearchCachePartitionWrapperPtr();
    }

    SearchCachePtr searchCache(new SearchCache(
                    mOptions.GetOfflineConfig().readerConfig.searchCacheSize,
                    MemoryQuotaControllerPtr(), TaskSchedulerPtr(), NULL, 6));
    SearchCachePartitionWrapperPtr wrapper(new SearchCachePartitionWrapper(
                    searchCache, 0));
    return wrapper;
}

IE_NAMESPACE_END(partition);
