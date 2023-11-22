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
#include "indexlib/partition/offline_partition.h"

#include "autil/StringTokenizer.h"
#include "autil/legacy/json.h"
#include "autil/legacy/string_tools.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/load_config/CacheLoadStrategy.h"
#include "indexlib/file_system/load_config/MemLoadStrategy.h"
#include "indexlib/index/kkv/kkv_define.h"
#include "indexlib/index/kkv/kkv_index_format.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_plugin_resource.h"
#include "indexlib/index_base/branch_fs.h"
#include "indexlib/index_base/common_branch_hinter_option.h"
#include "indexlib/index_base/index_meta/index_format_version.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/index_base/meta_cache_preloader.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/patch/partition_patch_index_accessor.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/index_base/segment/partition_segment_iterator.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/index_base/segment/segment_iterator.h"
#include "indexlib/partition/building_partition_data.h"
#include "indexlib/partition/index_partition_resource.h"
#include "indexlib/partition/modifier/partition_modifier_creator.h"
#include "indexlib/partition/offline_partition_reader.h"
#include "indexlib/partition/offline_partition_writer.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/partition/partition_resource_calculator.h"
#include "indexlib/partition/segment/dump_segment_container.h"
#include "indexlib/partition/segment/in_memory_segment_cleaner.h"
#include "indexlib/partition/segment/in_memory_segment_container.h"
#include "indexlib/partition/segment/in_memory_segment_releaser.h"
#include "indexlib/partition/table_reader_container_updater.h"
#include "indexlib/plugin/index_plugin_loader.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/TaskScheduler.h"
#include "indexlib/util/cache/SearchCachePartitionWrapper.h"
#include "indexlib/util/counter/CounterMap.h"
#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"
#include "indexlib/util/memory_control/MemoryReserver.h"
#include "indexlib/util/metrics/Monitor.h"

using namespace std;
using namespace autil;

using namespace indexlib::config;
using namespace indexlib::common;
using namespace indexlib::file_system;
using namespace indexlib::index;
using namespace indexlib::plugin;
using namespace indexlib::util;
using namespace indexlib::index_base;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, OfflinePartition);

OfflinePartition::OfflinePartition()
    : OfflinePartition(autil::EnvUtil::getEnv("BRANCH_NAME_POLICY", CommonBranchHinterOption::BNP_LEGACY) ==
                       CommonBranchHinterOption::BNP_LEGACY)
{
}

// only for read-only partition (using in extract doc task and tools) or ut
OfflinePartition::OfflinePartition(bool branchNameLegacy)
    : OfflinePartition(IndexPartitionResource::Create(branchNameLegacy ? IndexPartitionResource::IR_OFFLINE_LEGACY
                                                                       : IndexPartitionResource::IR_OFFLINE_TEST))
{
}

OfflinePartition::OfflinePartition(const IndexPartitionResource& partitionResource)
    : IndexPartition(partitionResource)
    , mClosed(true)
    , mDumpSegmentContainer(new DumpSegmentContainer)
    , mInMemSegCleaner(mDumpSegmentContainer->GetInMemSegmentContainer())
    , mParallelBuildInfo(partitionResource.parallelBuildInfo)
    , mPartitionRange(partitionResource.range)
{
    IE_INIT_METRIC_GROUP(mMetricProvider, oldInMemorySegmentMemoryUse, "build/oldInMemorySegmentMemoryUse",
                         kmonitor::GAUGE, "byte");
    IE_INIT_METRIC_GROUP(mMetricProvider, fixedCostMemoryRatio, "build/fixedCostMemoryRatio", kmonitor::STATUS, "%");
    IE_INIT_METRIC_GROUP(mMetricProvider, buildMemoryQuota, "build/buildMemoryQuota", kmonitor::GAUGE, "byte");
    IE_INIT_METRIC_GROUP(mMetricProvider, temperatureIndexSize, "build/temperatureIndexSize", kmonitor::GAUGE, "byte");
}

OfflinePartition::~OfflinePartition()
{
    mClosed = true;
    mBackGroundThreadPtr.reset();
    mWriter.reset();
    IndexPartition::Reset();
}

IndexPartition::OpenStatus OfflinePartition::Open(const string& primaryDir, const string& secondaryDir,
                                                  const IndexPartitionSchemaPtr& schema,
                                                  const IndexPartitionOptions& options, versionid_t versionId)
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

IndexPartition::OpenStatus OfflinePartition::DoOpen(const string& root, const IndexPartitionSchemaPtr& schema,
                                                    const IndexPartitionOptions& options, versionid_t versionId)
{
    mParallelBuildInfo.CheckValid();
    string outputRoot = mParallelBuildInfo.IsParallelBuild() ? mParallelBuildInfo.GetParallelInstancePath(root) : root;
    IndexPartition::OpenStatus openStatus = IndexPartition::Open(outputRoot, root, schema, options, versionId);
    if (openStatus != OS_OK) {
        return openStatus;
    }

    Version version;
    version.SyncSchemaVersionId(schema);
    if (versionId != INVALID_VERSIONID) {
        // offline partition reader
        mOptions.GetOfflineConfig().enableRecoverIndex = false;
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

    if (mOptions.GetUpdateableSchemaStandards().IsEmpty()) {
        mOptions.SetUpdateableSchemaStandards(version.GetUpdateableSchemaStandards());
    }
    MetaCachePreloader::Load(mRootDirectory, version.GetVersionId());
    mSchema = SchemaAdapter::LoadAndRewritePartitionSchema(mRootDirectory, mOptions, version.GetSchemaVersionId());
    CheckParam(mSchema, mOptions);
    if (schema) {
        if (schema->HasModifyOperations()) {
            mSchema->EnableThrowAssertCompatibleException();
        }
        mSchema->AssertCompatible(*schema);
    }

    if (schema && schema->HasModifyOperations() && schema->GetSchemaVersionId() > mSchema->GetSchemaVersionId()) {
        // use new schema
        IndexPartitionSchemaPtr newSchema(schema->Clone());
        SchemaAdapter::RewritePartitionSchema(newSchema, mRootDirectory, mOptions);
        CheckParam(newSchema, mOptions);
        string schemaFileName = Version::GetSchemaFileName(newSchema->GetSchemaVersionId());
        FenceContextPtr fenceContext(FslibWrapper::CreateFenceContext(root, mHinterOption.epochId));
        if (!FslibWrapper::IsExist(PathUtil::JoinPath(root, schemaFileName)).GetOrThrow()) {
            if (mParallelBuildInfo.IsParallelBuild() && !IsFirstIncParallelInstance()) {
                INDEXLIB_FATAL_ERROR(InitializeFailed,
                                     "inc parallel build from lagecy schema parent path [%s], "
                                     "wait instance[0] init new schema file in parent path!",
                                     root.c_str());
            }
            SchemaAdapter::StoreSchema(root, newSchema, fenceContext.get());
        }
        THROW_IF_FS_ERROR(mFileSystem->MountFile(root, schemaFileName, schemaFileName, FSMT_READ_ONLY, -1, false),
                          "mount schema file failed");
        mSchema = newSchema;
    }

    if (mSchema && mSchema->HasModifyOperations()) {
        MountPatchIndex();
    }

    mPluginManager = IndexPluginLoader::Load(mIndexPluginPath, mSchema->GetIndexSchema(), mOptions);
    if (!mPluginManager) {
        IE_LOG(ERROR, "load index plugin failed for schema [%s]", mSchema->GetSchemaName().c_str());
        return OS_FAIL;
    }

    if (mParallelBuildInfo.IsParallelBuild()) {
        mParallelBuildInfo.SetBaseVersion(version.GetVersionId());
        mParallelBuildInfo.StoreIfNotExist(mRootDirectory);
    }

    mCounterMap.reset(new CounterMap());
    InitPartitionData(version, mCounterMap, mPluginManager);
    PartitionMeta partitionMeta = mPartitionDataHolder.Get()->GetPartitionMeta();
    CheckPartitionMeta(mSchema, partitionMeta);

    PluginResourcePtr resource(
        new IndexPluginResource(mSchema, mOptions, mCounterMap, partitionMeta, mIndexPluginPath, mMetricProvider));
    mPluginManager->SetPluginResource(resource);

    if (!mOptions.GetOfflineConfig().enableRecoverIndex) {
        // for remote read purpose, no need init writer. etc
        mClosed = false;
        return OS_OK;
    }

    if (mSchema->GetTableType() == tt_kkv && mOptions.GetBuildConfig(false).buildPhase != BuildConfig::BP_FULL) {
        CheckSortedKKVIndex(mPartitionDataHolder.Get());
    }

    InitWriter(mPartitionDataHolder.Get());
    mClosed = false;
    mBackGroundThreadPtr =
        autil::Thread::createThread(std::bind(&OfflinePartition::BackGroundThread, this), "indexBackground");
    if (!mBackGroundThreadPtr) {
        IE_LOG(ERROR, "create background thread failed, open failed");
        return OS_FAIL;
    }

    if (mSchema->EnableTemperatureLayer()) {
        ReportTemperatureIndexSize(version);
    }
    return OS_OK;
}

void OfflinePartition::ReportTemperatureIndexSize(const Version& version) const
{
    int64_t hotSize = 0, warmSize = 0, coldSize = 0;
    PartitionResourceCalculator::CalculateTemperatureIndexSize(mPartitionDataHolder.Get(), mSchema, version, hotSize,
                                                               warmSize, coldSize);
    kmonitor::MetricsTags hotTags, warmTags, coldTags;
    hotTags.AddTag("temperature", "HOT");
    warmTags.AddTag("temperature", "WARM");
    coldTags.AddTag("temperature", "COLD");
    IE_REPORT_METRIC_WITH_TAGS(temperatureIndexSize, &hotTags, hotSize);
    IE_REPORT_METRIC_WITH_TAGS(temperatureIndexSize, &warmTags, warmSize);
    IE_REPORT_METRIC_WITH_TAGS(temperatureIndexSize, &coldTags, coldSize);
}

void OfflinePartition::InitPartitionData(Version& version, const CounterMapPtr& counterMap,
                                         const PluginManagerPtr& pluginManager)
{
    if (version.GetVersionId() == INVALID_VERSIONID) {
        indexlibv2::framework::LevelInfo& levelInfo = version.GetLevelInfo();
        const BuildConfig& buildConfig = mOptions.GetBuildConfig();
        levelInfo.Init(buildConfig.levelTopology, buildConfig.levelNum, buildConfig.shardingColumnNum);
    }
    BuildingPartitionParam param(mOptions, mSchema, mPartitionMemController, mDumpSegmentContainer, counterMap,
                                 pluginManager, mMetricProvider, document::SrcSignature());
    mPartitionDataHolder.Reset(
        PartitionDataCreator::CreateBuildingPartitionData(param, mFileSystem, version, "", InMemorySegmentPtr()));
}

IFileSystemPtr OfflinePartition::CreateFileSystem(const string& primaryDir, const string& secondaryDir)
{
    RewriteLoadConfigs(mOptions);
    return IndexPartition::CreateFileSystem(primaryDir, secondaryDir);
}

void OfflinePartition::RewriteLoadConfigs(IndexPartitionOptions& options) const
{
    LoadConfigList offlineLoadConfigList = options.GetOfflineConfig().GetLoadConfigList();
    if (offlineLoadConfigList.Size() != 0) {
        // todo refactor
        options.GetOnlineConfig().loadConfigList = offlineLoadConfigList;
        return;
    }
    LoadConfigList& loadConfigList = options.GetOnlineConfig().loadConfigList;
    loadConfigList.Clear();

    if (options.GetOfflineConfig().readerConfig.useBlockCache) {
        LoadConfig::FilePatternStringVector inmemPatterns;
        LoadStrategyPtr indexCacheLoadStrategy;
        LoadStrategyPtr summaryCacheLoadStrategy;
        if (mFileBlockCacheContainer) {
            // Global Cache
            indexCacheLoadStrategy.reset(new CacheLoadStrategy(false, true));
            summaryCacheLoadStrategy.reset(new CacheLoadStrategy(false, true));
        } else {
            indexCacheLoadStrategy.reset(
                new CacheLoadStrategy(BlockCacheOption::LRU(options.GetOfflineConfig().readerConfig.indexCacheSize,
                                                            options.GetOfflineConfig().readerConfig.cacheBlockSize,
                                                            options.GetOfflineConfig().readerConfig.cacheIOBatchSize),
                                      false, false, true, false));
            summaryCacheLoadStrategy.reset(
                new CacheLoadStrategy(BlockCacheOption::LRU(options.GetOfflineConfig().readerConfig.summaryCacheSize,
                                                            options.GetOfflineConfig().readerConfig.cacheBlockSize,
                                                            options.GetOfflineConfig().readerConfig.cacheIOBatchSize),
                                      false, false, true, false));
        }

        LoadConfig indexLoadConfig;
        LoadConfig summaryLoadConfig;
        LoadConfig::FilePatternStringVector summaryPattern;
        summaryPattern.push_back("_SOURCE_DATA_");
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
        indexPattern.push_back("_PK_ATTRIBUTE_");
        indexPattern.push_back("_INDEX_DICTIONARY_");
        indexPattern.push_back("_INDEX_POSTING_");

        if (options.GetOfflineConfig().readerConfig.readPreference == ReadPreference::RP_SEQUENCE_PREFER) {
            indexPattern.push_back("_ATTRIBUTE_");
            indexPattern.push_back("_SECTION_ATTRIBUTE_");
        } else {
            inmemPatterns.push_back("_ATTRIBUTE_");
            inmemPatterns.push_back("_SECTION_ATTRIBUTE_");
        }
        inmemPatterns.push_back("_INDEX_"); // make sure pk loaded in memory
        indexLoadConfig.SetFilePatternString(indexPattern);
        indexLoadConfig.SetLoadStrategyPtr(indexCacheLoadStrategy);
        indexLoadConfig.SetName("__OFFLINE_INDEX__");

        LoadConfig pkAndAttrLoadConfig;
        LoadStrategyPtr memLoadStrategy(
            new MemLoadStrategy(getpagesize(), 0, /*enableLoadSpeedLimit*/ std::shared_ptr<bool>()));
        pkAndAttrLoadConfig.SetLoadStrategyPtr(memLoadStrategy);
        pkAndAttrLoadConfig.SetName("__OFFLINE_PK_AND_ATTRIBUTE__");
        pkAndAttrLoadConfig.SetFilePatternString(inmemPatterns);

        loadConfigList.PushFront(pkAndAttrLoadConfig);
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

// for indexbuilder end version
IndexPartition::OpenStatus OfflinePartition::ReOpen(bool forceReopen, versionid_t reopenVersionId)
{
    assert(false);
    return OS_FAIL;
}

void OfflinePartition::Close()
{
    if (mClosed) {
        return;
    }

    if (mWriter) {
        try {
            mWriter->Close(); // will trigger dump version
        } catch (...) {
            mClosed = true;
            throw;
        }
    }

    mClosed = true;
    mBackGroundThreadPtr.reset();
    // reset sequence: writer -> partitionData -> fileSystsm
    mWriter.reset();
    mPartitionDataHolder.Reset();
    if (mFileSystem) {
        mFileSystem->Sync(true).GetOrThrow();
    }
    mFileSystem.reset();
    IndexPartition::Close();
}

PartitionWriterPtr OfflinePartition::GetWriter() const { return mWriter; }

IndexPartitionReaderPtr OfflinePartition::GetReader() const
{
    ScopedLock lock(mReaderLock);
    if (mReader) {
        return mReader;
    }

    PartitionDataPtr partData = mPartitionDataHolder.Get();
    if (!partData) {
        IE_LOG(ERROR, "cannot create reader when partitionData is empty");
        return IndexPartitionReaderPtr();
    }
    Version version = partData->GetVersion();
    mReader.reset(new partition::OfflinePartitionReader(mOptions, mSchema, CreateSearchCacheWrapper()));
    PartitionDataPtr clonedPartitionData(partData->Clone());
    try {
        mReader->Open(clonedPartitionData);
    } catch (const ExceptionBase& e) {
        IE_LOG(ERROR, "open offline partition reader caught exception: %s", e.what());
        return IndexPartitionReaderPtr();
    } catch (...) {
        IE_LOG(ERROR, "open offline partition reader caught unknown exception.");
        return IndexPartitionReaderPtr();
    }
    IE_LOG(INFO, "End open offline partition reader (version=[%d])", version.GetVersionId());
    return mReader;
}

OfflinePartitionWriterPtr OfflinePartition::CreatePartitionWriter()
{
    return OfflinePartitionWriterPtr(new OfflinePartitionWriter(
        mOptions, mDumpSegmentContainer, mFlushedLocatorContainer, mMetricProvider, mPartitionName, mPartitionRange));
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

PartitionModifierPtr OfflinePartition::CreateModifier(const PartitionDataPtr& partData)
{
    PartitionModifierPtr modifier = PartitionModifierCreator::CreateOfflineModifier(
        mSchema, partData, true, mOptions.GetBuildConfig().enablePackageFile);
    return modifier;
}

void OfflinePartition::CheckParam(const IndexPartitionSchemaPtr& schema, const IndexPartitionOptions& options) const
{
    if (!options.IsOffline()) {
        INDEXLIB_FATAL_ERROR(BadParameter, "OfflinePartition only support offline option!");
    }

    options.Check();
    if (!schema) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "schema is NULL");
    }
    schema->Check();
}

void OfflinePartition::BackGroundThread()
{
    IE_LOG(DEBUG, "offline report metrics thread start");
    static int32_t sleepTime =
        autil::EnvUtil::getEnv("TEST_QUICK_EXIT", false) ? REPORT_METRICS_INTERVAL / 1000 : REPORT_METRICS_INTERVAL;

    const auto& buildMemoryQuotaControler = mPartitionMemController->GetBuildMemoryQuotaControler();
    int64_t buildMemoryTotalQuota = buildMemoryQuotaControler ? buildMemoryQuotaControler->GetLeftQuota() : 0;
    while (!mClosed) {
        assert(mFileSystem);
        mFileSystem->ReportMetrics();
        mWriter->ReportMetrics();
        IE_REPORT_METRIC(oldInMemorySegmentMemoryUse,
                         mDumpSegmentContainer->GetInMemSegmentContainer()->GetTotalMemoryUse());

        if (buildMemoryTotalQuota > 0) {
            double fixCostMemRatio = (double)mWriter->GetFixedCostMemorySize() / buildMemoryTotalQuota;
            IE_REPORT_METRIC(fixedCostMemoryRatio, fixCostMemRatio);
            IE_REPORT_METRIC(buildMemoryQuota, buildMemoryTotalQuota);
        }
        mInMemSegCleaner.Execute();
        usleep(sleepTime);
    }
    IE_LOG(DEBUG, "offline report metrics thread exit");
}

void OfflinePartition::CheckSortedKKVIndex(const PartitionDataPtr& partData)
{
    KKVIndexConfigPtr kkvConfig = CreateDataKKVIndexConfig(mSchema);
    PartitionSegmentIteratorPtr segIter = partData->CreateSegmentIterator();
    assert(segIter);
    SegmentIteratorPtr builtSegIter = segIter->CreateIterator(SIT_BUILT);
    assert(builtSegIter);
    while (builtSegIter->IsValid()) {
        SegmentData segData = builtSegIter->GetSegmentData();
        if (!segData.GetSegmentInfo()->mergedSegment) {
            builtSegIter->MoveToNext();
            continue;
        }

        if (segData.GetSegmentInfo()->docCount == 0) {
            builtSegIter->MoveToNext();
            continue;
        }
        DirectoryPtr indexDirectory = segData.GetIndexDirectory(kkvConfig->GetIndexName(), true);
        KKVIndexFormat indexFormat;
        indexFormat.Load(indexDirectory);
        if (indexFormat.KeepSortSequence()) {
            INDEXLIB_FATAL_ERROR(UnSupported, "not support inc build with sorted kkv segment [%d]",
                                 segData.GetSegmentId());
        }
        builtSegIter->MoveToNext();
    }
}

SearchCachePartitionWrapperPtr OfflinePartition::CreateSearchCacheWrapper() const
{
    if (!mOptions.GetOfflineConfig().readerConfig.useSearchCache) {
        return SearchCachePartitionWrapperPtr();
    }

    SearchCachePtr searchCache(new SearchCache(mOptions.GetOfflineConfig().readerConfig.searchCacheSize,
                                               MemoryQuotaControllerPtr(), TaskSchedulerPtr(), NULL, 6));
    SearchCachePartitionWrapperPtr wrapper(new SearchCachePartitionWrapper(searchCache, ""));
    return wrapper;
}

void OfflinePartition::MountPatchIndex()
{
    std::vector<schema_opid_t> notReadyOpIds = mOptions.GetOngoingModifyOperationIds();
    std::sort(notReadyOpIds.begin(), notReadyOpIds.end());

    std::vector<schema_opid_t> opIds;
    mSchema->GetModifyOperationIds(opIds);
    std::sort(opIds.begin(), opIds.end());

    std::vector<schema_opid_t> readyOpIds;
    std::set_difference(opIds.begin(), opIds.end(), notReadyOpIds.begin(), notReadyOpIds.end(),
                        inserter(readyOpIds, readyOpIds.begin()));
    std::sort(readyOpIds.begin(), readyOpIds.end());

    for (auto id : readyOpIds) {
        string patchIndexPath = PartitionPatchIndexAccessor::GetPatchRootDirName(id);
        if (mFileSystem->IsExist(patchIndexPath).GetOrThrow(patchIndexPath)) {
            continue;
        }

        auto ec = mFileSystem->MountDir(mFileSystem->GetOutputRootPath(), patchIndexPath, patchIndexPath,
                                        FSMT_READ_ONLY, true);
        if (ec != FSEC_OK && ec != FSEC_NOENT) {
            THROW_IF_FS_ERROR(ec, "mount dir[%s] failed", patchIndexPath.c_str());
        }
        // TODO: mount by segment file list
    }
}
}} // namespace indexlib::partition
