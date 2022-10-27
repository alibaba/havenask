#include "indexlib/merger/multi_partition_merger.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/merger/sorted_index_partition_merger.h"
#include "indexlib/merger/multi_part_segment_directory.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_plugin_resource.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/sort_pattern_transformer.h"
#include "indexlib/index_base/index_meta/index_format_version.h"
#include "indexlib/index_base/meta_cache_preloader.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/plugin/index_plugin_loader.h"

using namespace std;
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(plugin);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(merger);

IE_LOG_SETUP(merger, MultiPartitionMerger);

MultiPartitionMerger::MultiPartitionMerger(
        const IndexPartitionOptions& options,
        MetricProviderPtr metricProvider,
        const string& indexPluginPath)
    : mOptions(options)
    , mMetricProvider(metricProvider)
    , mIndexPluginPath(indexPluginPath)
{
}

MultiPartitionMerger::~MultiPartitionMerger()
{
}

void MultiPartitionMerger::GetFirstVersion(
    const std::string& partPath, index_base::Version& version)
{
    VersionLoader::GetVersion(partPath, version, INVALID_VERSION);
    MetaCachePreloader::Load(partPath, version.GetVersionId());
}

IndexPartitionMergerPtr MultiPartitionMerger::CreatePartitionMerger(
        const std::vector<std::string>& mergeSrc,
        const std::string& destPath)
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

    if (!FileSystemWrapper::IsExist(mergeSrc[0]))
    {
        INDEXLIB_FATAL_ERROR(BadParameter, "Partition not exsit : [%s]",
                             mergeSrc[0].c_str());
    }

    Version firstVersion;
    GetFirstVersion(mergeSrc[0], firstVersion);
    mSchema = SchemaAdapter::LoadAndRewritePartitionSchema(
        mergeSrc[0], mOptions, firstVersion.GetSchemaVersionId());
    if (mSchema == NULL)
    {
        INDEXLIB_FATAL_ERROR(BadParameter, "No schema file in [%s]."
                             "when merging multi-partition", mergeSrc[0].c_str());
    }
    IE_LOG(INFO, "loadAndRewrite schema done");
    PartitionMeta partitionMeta = PartitionMeta::LoadPartitionMeta(mergeSrc[0]);
    IE_LOG(INFO, "load partitionMeta done");

    vector<string> mergeSrcVec(mergeSrc.begin(), mergeSrc.end());
    vector<Version> versions;
    CheckSrcPath(mergeSrcVec, mSchema, partitionMeta, versions);
    CreateDestPath(destPath, mSchema, partitionMeta);
    mSegmentDirectory.reset(new MultiPartSegmentDirectory(
                    mergeSrcVec, versions));
    // KV & KKV has no deletionMap
    bool needDeletionMap = mSchema->GetTableType() == tt_index;
    mSegmentDirectory->Init(mSchema->GetSubIndexPartitionSchema() != NULL, needDeletionMap);
    IE_LOG(INFO, "segDir init done"); 
    Version destVersion;
    if (!versions.empty())
    {
        destVersion.SetFormatVersion(versions[0].GetFormatVersion());
    }
    //TODO: if exist, right?
    VersionLoader::GetVersion(destPath, destVersion, INVALID_VERSION);
    if (destVersion == INVALID_VERSION)
    {
        const LevelInfo& srcLevelInfo = mSegmentDirectory->GetVersion().GetLevelInfo();
        LevelInfo& levelInfo = destVersion.GetLevelInfo();
        levelInfo.Init(srcLevelInfo.GetTopology(), srcLevelInfo.GetLevelCount(),
                       srcLevelInfo.GetColumnCount());
    }
    //TODO: set min ts
    destVersion.SetTimestamp(mSegmentDirectory->GetVersion().GetTimestamp());
    destVersion.SetLocator(mSegmentDirectory->GetVersion().GetLocator());
    destVersion.SetFormatVersion(mSegmentDirectory->GetVersion().GetFormatVersion());
    IE_LOG(INFO, "prepare dest version done");

    DumpStrategyPtr dumpStrategy;
    dumpStrategy.reset(new DumpStrategy(destPath, destVersion));

    PluginManagerPtr pluginManager =
        IndexPluginLoader::Load(mIndexPluginPath,
                                mSchema->GetIndexSchema(), mOptions);
    if (!pluginManager)
    {
        INDEXLIB_FATAL_ERROR(Runtime, "load index plugin failed for schema[%s]",
               mSchema->GetSchemaName().c_str());
    }
    PluginResourcePtr resource(new IndexPluginResource(
                    mSchema, mOptions, util::CounterMapPtr(),
                    partitionMeta, mIndexPluginPath));
    pluginManager->SetPluginResource(resource);
    IE_LOG(INFO, "prepare pluginManager done"); 

    if (partitionMeta.Size() > 0)
    {
        return IndexPartitionMergerPtr(new SortedIndexPartitionMerger(
                        mSegmentDirectory, mSchema, mOptions,
                        partitionMeta.GetSortDescriptions(),
                        dumpStrategy, mMetricProvider, pluginManager));
    }
    else
    {
        return IndexPartitionMergerPtr(new IndexPartitionMerger(
                        mSegmentDirectory, mSchema, mOptions, dumpStrategy, 
                        mMetricProvider, pluginManager));
    }
}

void MultiPartitionMerger::Merge(const vector<string>& mergeSrc,
                                 const string& destPath)
{
    IndexPartitionMergerPtr merger = CreatePartitionMerger(mergeSrc, destPath);
    merger->Merge(true);
}

void MultiPartitionMerger::CreateDestPath(
        const string& destPath,
        const IndexPartitionSchemaPtr& schema,
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
        CheckSchema(destPath, schema);
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

void MultiPartitionMerger::CheckSrcPath(const vector<string>& mergeSrc,
                                        const IndexPartitionSchemaPtr& schema,
                                        const PartitionMeta &partMeta,
                                        vector<Version>& versions)
{
    versions.resize(mergeSrc.size());
    for (size_t i = 0; i < mergeSrc.size(); ++i)
    {
        string partPath = FileSystemWrapper::NormalizeDir(mergeSrc[i]);
        if (!FileSystemWrapper::IsExist(partPath))
        {
            INDEXLIB_FATAL_ERROR(BadParameter, "Partition not exsit : [%s]", mergeSrc[i].c_str());
        }

        VersionLoader::GetVersion(partPath, versions[i], INVALID_VERSION);
        MetaCachePreloader::Load(partPath, versions[i].GetVersionId());
        CheckPartitionConsistence(partPath, schema, partMeta);
    }
}

void MultiPartitionMerger::CheckPartitionConsistence(
        const string& rootDir,
        const IndexPartitionSchemaPtr& schema,
        const PartitionMeta &partMeta)
{
    string root = FileSystemWrapper::NormalizeDir(rootDir);

    // check index format
    CheckIndexFormat(root);

    // check schema
    CheckSchema(root, schema);

    // check partition meta
    CheckPartitionMeta(root, partMeta);
}

void MultiPartitionMerger::CheckSchema(const string& rootDir,
                                       const IndexPartitionSchemaPtr& schema)
{
    IndexPartitionSchemaPtr pSchema = 
        SchemaAdapter::LoadAndRewritePartitionSchema(rootDir, mOptions,
                schema->GetSchemaVersionId());
    schema->AssertEqual(*pSchema);
}

void MultiPartitionMerger::CheckIndexFormat(const string& rootDir)
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

void MultiPartitionMerger::CheckPartitionMeta(
        const string& rootDir, const PartitionMeta &partMeta)
{
    PartitionMeta partitionMeta = PartitionMeta::LoadPartitionMeta(rootDir);
    partitionMeta.AssertEqual(partMeta);
}

IE_NAMESPACE_END(merger);
