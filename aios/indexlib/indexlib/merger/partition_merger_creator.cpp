#include "indexlib/merger/partition_merger_creator.h"
#include "indexlib/merger/index_partition_merger.h"
#include "indexlib/merger/sorted_index_partition_merger.h"
#include "indexlib/merger/multi_part_segment_directory.h"
#include "indexlib/merger/parallel_partition_data_merger.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/merger/segment_directory.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/merger/inc_parallel_partition_merger.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_plugin_resource.h"
#include "indexlib/index_base/index_meta/index_format_version.h"
#include "indexlib/index_base/meta_cache_preloader.h"
#include "indexlib/table/table_plugin_loader.h"
#include "indexlib/table/table_factory_wrapper.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/sort_pattern_transformer.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/plugin/index_plugin_loader.h"
#include "indexlib/storage/file_system_wrapper.h"

using namespace std;
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(plugin);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(table);

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, PartitionMergerCreator);

PartitionMergerCreator::PartitionMergerCreator() 
{
}

PartitionMergerCreator::~PartitionMergerCreator() 
{
}

PartitionMerger* PartitionMergerCreator::CreateSinglePartitionMerger(
        const string& rootDir, const IndexPartitionOptions& options,
        MetricProviderPtr metricProvider, const string& indexPluginPath,
        const PartitionRange& targetRange)
{
    Version version;
    VersionLoader::GetVersion(rootDir, version, INVALID_VERSION);
    MetaCachePreloader::Load(rootDir, version.GetVersionId());
    IndexPartitionSchemaPtr schema = SchemaAdapter::LoadSchema(
            rootDir, version.GetSchemaVersionId());
    if (!schema)
    {
        ERROR_COLLECTOR_LOG(ERROR, "load schema failed");
        return NULL;
    }
    if (schema->GetTableType() == tt_customized)
    {
        merger::SegmentDirectoryPtr segDir(new SegmentDirectory(rootDir, version));
        segDir->Init(false, false);
        PluginManagerPtr pluginManager = TablePluginLoader::Load(indexPluginPath, schema, options);
        return CreateCustomPartitionMerger(schema, options, segDir, DumpStrategyPtr(),
                                           metricProvider, pluginManager, targetRange);
    }

    schema = SchemaAdapter::RewritePartitionSchema(schema, rootDir, options);
    if (!schema)
    {
        ERROR_COLLECTOR_LOG(ERROR, "rewrite schema failed");
        return NULL;
    }
    
    merger::SegmentDirectoryPtr segDir(new SegmentDirectory(rootDir, version));
    bool hasSub = false;
    if (schema->GetSubIndexPartitionSchema())
    {
        hasSub = true;
    }

    // KV & KKV has no deletionMap
    bool needDeletionMap = schema->GetTableType() == tt_index;
    segDir->Init(hasSub, needDeletionMap);

    PartitionMeta partMeta = PartitionMeta::LoadPartitionMeta(rootDir);
    const SortDescriptions &sortDescs = partMeta.GetSortDescriptions();
    bool needSort = sortDescs.size() > 0;
    
    PluginManagerPtr pluginManager =
        IndexPluginLoader::Load(indexPluginPath,
                                schema->GetIndexSchema(), options);
    if (!pluginManager)
    {
        IE_LOG(ERROR, "load index plugin failed for schema [%s]",
               schema->GetSchemaName().c_str());
        return NULL;
    }

    PluginResourcePtr pluginResource(new IndexPluginResource(
                                         schema, options,
                                         util::CounterMapPtr(),
                                         partMeta, indexPluginPath));

    pluginManager->SetPluginResource(pluginResource);
    if (!needSort)
    {
        return new IndexPartitionMerger(segDir, schema, options,
                                        DumpStrategyPtr(), metricProvider, pluginManager, targetRange);
    }
    else
    {
        return new SortedIndexPartitionMerger(segDir, schema, options,
                        sortDescs, DumpStrategyPtr(), metricProvider, pluginManager, targetRange);
    }
}

PartitionMerger* PartitionMergerCreator::CreateIncParallelPartitionMerger(
        const string& rootDir,
        const ParallelBuildInfo& parallelInfo,
        const IndexPartitionOptions& options,
        MetricProviderPtr metricProvider,
        const string& indexPluginPath,
        const PartitionRange& targetRange)
{
    IndexPartitionSchemaPtr schema = SchemaAdapter::LoadAndRewritePartitionSchema(
            rootDir, options, GetSchemaId(rootDir, parallelInfo));
    if (!schema)
    {
        ERROR_COLLECTOR_LOG(ERROR, "load schema failed");
        return NULL;
    }

    Version version;
    VersionLoader::GetVersion(rootDir, version, INVALID_VERSION);
    if (version.GetVersionId() == INVALID_VERSION)
    {
        version.SyncSchemaVersionId(schema);
    }
    
    merger::SegmentDirectoryPtr segDir(new SegmentDirectory(rootDir, version));
    bool hasSub = false;
    if (schema->GetSubIndexPartitionSchema())
    {
        hasSub = true;
    }

    // KV & KKV has no deletionMap
    bool needDeletionMap = schema->GetTableType() == tt_index;
    segDir->Init(hasSub, needDeletionMap);

    PartitionMeta partMeta = PartitionMeta::LoadPartitionMeta(rootDir);
    const SortDescriptions &sortDescs = partMeta.GetSortDescriptions();
    bool needSort = sortDescs.size() > 0;

    PluginManagerPtr pluginManager;
    IndexPartitionMergerPtr merger;
    if (schema->GetTableType() == tt_customized) {
        pluginManager = TablePluginLoader::Load(indexPluginPath, schema, options);
        merger.reset(CreateCustomPartitionMerger(
                         schema, options, segDir, DumpStrategyPtr(), metricProvider, pluginManager, targetRange));
    } else {
        pluginManager =IndexPluginLoader::Load(indexPluginPath, schema->GetIndexSchema(), options);        
        if (!pluginManager)
        {
            IE_LOG(ERROR, "load index plugin failed for schema [%s]",
                   schema->GetSchemaName().c_str());
            return NULL;
        }
        PluginResourcePtr pluginResource(new IndexPluginResource(
                                             schema, options, util::CounterMapPtr(), partMeta, indexPluginPath));
        pluginManager->SetPluginResource(pluginResource);
        if (!needSort) {
            merger.reset(new IndexPartitionMerger(segDir, schema, options,
                                                  DumpStrategyPtr(), metricProvider,
                                                  pluginManager, targetRange));
        } else {
            merger.reset(new SortedIndexPartitionMerger(
                             segDir, schema, options, sortDescs, DumpStrategyPtr(),
                             metricProvider, pluginManager, targetRange));
        }
    }

    vector<string> mergeSrc =
        ParallelPartitionDataMerger::GetParallelInstancePaths(rootDir, parallelInfo);
    return new IncParallelPartitionMerger(merger, mergeSrc, sortDescs, metricProvider, pluginManager, targetRange);
}

PartitionMerger* PartitionMergerCreator::CreateFullParallelPartitionMerger(
        const vector<string>& mergeSrc,
        const string& destPath,
        const IndexPartitionOptions& options,
        MetricProviderPtr metricProvider,
        const string& indexPluginPath,
        const PartitionRange& targetRange)
{
    if (mergeSrc.size() == 0)
    {
        INDEXLIB_FATAL_ERROR(BadParameter, "No partition to merge."
                             "when merging multi-partition");
    }

    if (destPath == "")
    {
        INDEXLIB_FATAL_ERROR(BadParameter, "No destPath specified."
                             "when merging multi-partition");
    }

    Version firstVersion;
    VersionLoader::GetVersion(mergeSrc[0], firstVersion, INVALID_VERSION);
    IndexPartitionSchemaPtr schema = SchemaAdapter::LoadSchema(
        mergeSrc[0], firstVersion.GetSchemaVersionId());
    if (schema == NULL)
    {
        INDEXLIB_FATAL_ERROR(BadParameter, "No schema file in [%s]."
                             "when merging multi-partition", mergeSrc[0].c_str());
    }

    if (schema->GetTableType() != tt_customized)
    {
        schema = SchemaAdapter::RewritePartitionSchema(schema, mergeSrc[0], options);
        if (!schema)
        {
            ERROR_COLLECTOR_LOG(ERROR, "rewrite schema failed");
            return NULL;
        }        
    }
    CheckIndexFormat(mergeSrc[0]);
    IE_LOG(INFO, "loadAndRewrite schema done");
    PartitionMeta partitionMeta = PartitionMeta::LoadPartitionMeta(mergeSrc[0]);
    IE_LOG(INFO, "load partitionMeta done");

    vector<string> mergeSrcVec(mergeSrc.begin(), mergeSrc.end());
    vector<Version> versions;
    CheckSrcPath(mergeSrcVec, schema, options, partitionMeta, versions);
    SortSubPartitions(mergeSrcVec, versions);
    IE_LOG(INFO, "sort SubPartitions done");
    CreateDestPath(destPath, schema, options, partitionMeta);

    MultiPartSegmentDirectoryPtr multiSegDir(new MultiPartSegmentDirectory(mergeSrcVec, versions));
    if (schema->GetTableType() == tt_customized)
    {
        multiSegDir->Init(false, false);
    }
    else
    {
        // KV & KKV has no deletionMap
        bool needDeletionMap = schema->GetTableType() == tt_index;
        multiSegDir->Init(schema->GetSubIndexPartitionSchema() != NULL, needDeletionMap);
    }
    IE_LOG(INFO, "segDir init done");        
    Version destVersion;
    //TODO: if exist, right?
    VersionLoader::GetVersion(destPath, destVersion, INVALID_VERSION);
    if (destVersion == INVALID_VERSION)
    {
        const LevelInfo& srcLevelInfo = multiSegDir->GetVersion().GetLevelInfo();
        LevelInfo& levelInfo = destVersion.GetLevelInfo();
        levelInfo.Init(srcLevelInfo.GetTopology(), srcLevelInfo.GetLevelCount(),
                       srcLevelInfo.GetColumnCount());
    }
    //TODO: set min ts
    destVersion.SetTimestamp(multiSegDir->GetVersion().GetTimestamp());
    destVersion.SetLocator(multiSegDir->GetVersion().GetLocator());
    destVersion.SetFormatVersion(multiSegDir->GetVersion().GetFormatVersion());
    IE_LOG(INFO, "prepare dest version done");

    DumpStrategyPtr dumpStrategy;
    dumpStrategy.reset(new DumpStrategy(destPath, destVersion));

    if (schema->GetTableType() == tt_customized)
    {
        PluginManagerPtr pluginManager = TablePluginLoader::Load(indexPluginPath, schema, options);        
        return CreateCustomPartitionMerger(schema, options, multiSegDir, 
                                           dumpStrategy, metricProvider, pluginManager, targetRange); 
    }
    PluginManagerPtr pluginManager =
        IndexPluginLoader::Load(indexPluginPath,
                                schema->GetIndexSchema(), options);
    if (!pluginManager)
    {
        INDEXLIB_FATAL_ERROR(Runtime, "load index plugin failed for schema[%s]",
               schema->GetSchemaName().c_str());
    }
    PluginResourcePtr resource(new IndexPluginResource(
                    schema, options, util::CounterMapPtr(),
                    partitionMeta, indexPluginPath));
    pluginManager->SetPluginResource(resource);
    IE_LOG(INFO, "prepare pluginManager done");

    if (partitionMeta.Size() > 0)
    {
        return new SortedIndexPartitionMerger(
                        multiSegDir, schema, options,
                        partitionMeta.GetSortDescriptions(),
                        dumpStrategy, metricProvider, pluginManager, targetRange);
    }
    else
    {
        return new IndexPartitionMerger(
                        multiSegDir, schema, options, dumpStrategy,
                        metricProvider, pluginManager, targetRange);
    }
}

CustomPartitionMerger* PartitionMergerCreator::CreateCustomPartitionMerger(
    const IndexPartitionSchemaPtr& schema,
    const IndexPartitionOptions& options,
    const merger::SegmentDirectoryPtr& segDir,
    const DumpStrategyPtr& dumpStrategy,
    MetricProviderPtr metricProvider,
    const PluginManagerPtr& pluginManager,
    const PartitionRange& targetRange)
{
    if (!pluginManager)
    {
        IE_LOG(ERROR, "load table plugin for schema [%s]",
               schema->GetSchemaName().c_str());
        return NULL;
    }
    auto indexPluginPath = pluginManager->GetModuleRootPath();
    
    PluginResourcePtr resource(new PluginResource(
                    schema, options, util::CounterMapPtr(), indexPluginPath));
    pluginManager->SetPluginResource(resource);

    TableFactoryWrapperPtr tableFactory(new TableFactoryWrapper(schema, options, pluginManager));
    if (!tableFactory->Init())
    {
        IE_LOG(ERROR, "init table factory failed for schema[%s]", schema->GetSchemaName().c_str());
        return NULL;
    }
    return new CustomPartitionMerger(segDir, schema, options,
                                     dumpStrategy, metricProvider, tableFactory, targetRange);
}

void PartitionMergerCreator::CreateDestPath(
        const string& destPath,
        const IndexPartitionSchemaPtr& schema,
        const IndexPartitionOptions& options,
        const PartitionMeta &partMeta)
{
    if (!FileSystemWrapper::IsExist(destPath))
    {
        FileSystemWrapper::MkDir(destPath);
        SchemaAdapter::StoreSchema(destPath, schema);
        IndexFormatVersion::StoreBinaryVersion(destPath);
        partMeta.Store(destPath);
        return;
    }

    string destSchemaPath = destPath + '/' +
                            Version::GetSchemaFileName(schema->GetSchemaVersionId());
    string indexFormatPath = destPath + '/' + INDEX_FORMAT_VERSION_FILE_NAME;
    string partitionMetaPath = destPath + '/' + INDEX_PARTITION_META_FILE_NAME;

    if (FileSystemWrapper::IsExist(destSchemaPath))
    {
        CheckSchema(destPath, schema, options);
    }
    else
    {
        SchemaAdapter::StoreSchema(destPath, schema);
    }

    if (FileSystemWrapper::IsExist(indexFormatPath))
    {
        CheckIndexFormat(destPath);
    }
    else
    {
        IndexFormatVersion::StoreBinaryVersion(destPath);
    }

    if (FileSystemWrapper::IsExist(partitionMetaPath))
    {
        CheckPartitionMeta(destPath, partMeta);
    }
    else
    {
        partMeta.Store(destPath);
    }
}

void PartitionMergerCreator::CheckSrcPath(
        const vector<string>& mergeSrc,
        const IndexPartitionSchemaPtr& schema,
        const IndexPartitionOptions& options,
        const PartitionMeta &partMeta,
        vector<Version>& versions)
{
    versions.resize(mergeSrc.size());
    for (size_t i = 0; i < mergeSrc.size(); ++i)
    {
        string partPath = FileSystemWrapper::NormalizeDir(mergeSrc[i]);
        if (i > 0)
        {
            CheckPartitionConsistence(partPath, schema, options, partMeta);
        }
        VersionLoader::GetVersion(partPath, versions[i], INVALID_VERSION);
    }
}

void PartitionMergerCreator::CheckPartitionConsistence(
        const string& rootDir,
        const IndexPartitionSchemaPtr& schema,
        const IndexPartitionOptions& options,
        const PartitionMeta &partMeta)
{
    string root = FileSystemWrapper::NormalizeDir(rootDir);

    // check index format
    CheckIndexFormat(root);

    // check schema
    CheckSchema(root, schema, options);

    // check partition meta
    if (schema->GetTableType() != tt_customized)
    {
        CheckPartitionMeta(root, partMeta);
    }
}

void PartitionMergerCreator::CheckSchema(const string& rootDir,
        const IndexPartitionSchemaPtr& schema,
        const IndexPartitionOptions& options)
{
    IndexPartitionSchemaPtr pSchema;
    if (schema->GetTableType() == tt_customized)
    {
         pSchema = SchemaAdapter::LoadSchema(
            rootDir, schema->GetSchemaVersionId());
    }
    else
    {
        pSchema = SchemaAdapter::LoadAndRewritePartitionSchema(
            rootDir, options, schema->GetSchemaVersionId());
    }
    schema->AssertEqual(*pSchema);
}

void PartitionMergerCreator::CheckIndexFormat(const string& rootDir)
{
    IndexFormatVersion onDiskFormatVersion;
    string formatVersionPath = FileSystemWrapper::JoinPath(
            rootDir, INDEX_FORMAT_VERSION_FILE_NAME);
    onDiskFormatVersion.Load(formatVersionPath);

    IndexFormatVersion binaryFormatVersion(INDEX_FORMAT_VERSION);
    if (onDiskFormatVersion != binaryFormatVersion)
    {
        INDEXLIB_FATAL_ERROR(BadParameter, "index format version in [%s]"
                             " not equal with binaryversion [%s]",
                             rootDir.c_str(), INDEX_FORMAT_VERSION);
    }
}

void PartitionMergerCreator::CheckPartitionMeta(
        const string& rootDir, const PartitionMeta &partMeta)
{
    PartitionMeta partitionMeta = PartitionMeta::LoadPartitionMeta(rootDir);
    partitionMeta.AssertEqual(partMeta);
}


void PartitionMergerCreator::SortSubPartitions(
        vector<string>& mergeSrcVec, vector<Version>& versions)
{
    assert(mergeSrcVec.size() == versions.size());
    typedef pair<size_t, Version> VersionInfo;
    typedef pair<string, VersionInfo> SubPartitionInfo;
    typedef vector<SubPartitionInfo> SubPartitionInfos;

    SubPartitionInfos partInfos;
    for (size_t i = 0; i < mergeSrcVec.size(); i++)
    {
        IE_LOG(INFO, "src path [%s], ts [%ld]", mergeSrcVec[i].c_str(),
               versions[i].GetTimestamp());
        partInfos.push_back(
                make_pair(mergeSrcVec[i], make_pair(i, versions[i])));
    }
    stable_sort(partInfos.begin(), partInfos.end(),
                [](const pair<string, pair<size_t, Version>>& lft,
                   const pair<string, pair<size_t, Version>>& rht) -> bool
                {
                    int64_t lftTs = lft.second.second.GetTimestamp();
                    int64_t rhtTs = rht.second.second.GetTimestamp();
                    if (lftTs == rhtTs)
                    {
                        return lft.second.first < rht.second.first;
                    }
                    if (lftTs == INVALID_TIMESTAMP)
                    {
                        return true;
                    }
                    if (rhtTs == INVALID_TIMESTAMP)
                    {
                        return false;
                    }
                    return lftTs > rhtTs;
                });

    mergeSrcVec.clear();
    versions.clear();
    for (size_t i = 0; i < partInfos.size(); i++)
    {
        IE_LOG(INFO, "sequence [%lu], root [%s], version ts[%ld]",
               i, partInfos[i].first.c_str(),
               partInfos[i].second.second.GetTimestamp());
        mergeSrcVec.push_back(partInfos[i].first);
        versions.push_back(partInfos[i].second.second);
    }
}

schemavid_t PartitionMergerCreator::GetSchemaId(const string& rootDir,
        const ParallelBuildInfo& parallelInfo)
{
    Version version;
    VersionLoader::GetVersion(rootDir, version, INVALID_VERSION);
    MetaCachePreloader::Load(rootDir, version.GetVersionId());
    if (version.GetVersionId() != INVALID_VERSION)
    {
        return version.GetSchemaVersionId();
    }

    vector<string> mergeSrc =
        ParallelPartitionDataMerger::GetParallelInstancePaths(rootDir, parallelInfo);
    for (auto src : mergeSrc)
    {
        Version instVersion;
        VersionLoader::GetVersion(src, instVersion, INVALID_VERSION);
        if (instVersion.GetVersionId() != INVALID_VERSION)
        {
            return instVersion.GetSchemaVersionId();
        }
    }
    assert(false);
    return DEFAULT_SCHEMAID;
}


IE_NAMESPACE_END(merger);

