#include "indexlib/partition/remote_access/partition_resource_provider.h"
#include "indexlib/index_base/patch/partition_patch_index_accessor.h"
#include "indexlib/index_base/index_meta/segment_file_list_wrapper.h"
#include "indexlib/partition/offline_partition.h"
#include "indexlib/partition/index_partition_resource.h"
#include "indexlib/index_base/version_committer.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_plugin_resource.h"
#include "indexlib/util/path_util.h"
#include "indexlib/partition/index_partition_creator.h"
#include "indexlib/plugin/index_plugin_loader.h"
#include "indexlib/util/counter/counter_map.h"
#include "indexlib/util/task_scheduler.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"

using namespace std;
using namespace fslib;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(plugin);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, PartitionResourceProvider);

PartitionResourceProvider::PartitionResourceProvider(const config::IndexPartitionOptions& options)
    : mOptions(options)
{
    mOptions.SetIsOnline(false);
    mOptions.GetOfflineConfig().enableRecoverIndex = false;
    mOptions.GetOfflineConfig().readerConfig.loadIndex = false;
    mOptions.GetOfflineConfig().readerConfig.lazyLoadAttribute = false;
    mOptions.GetBuildConfig(false).keepVersionCount = INVALID_KEEP_VERSION_COUNT;
    mPluginManagerPair.first = -1;
}

bool PartitionResourceProvider::Init(
    const string& indexPath,
    versionid_t targetVersionId, const string& pluginPath)
{
    if (targetVersionId == INVALID_VERSION)
    {
        IE_LOG(ERROR, "targetVersionId not valid!");
        return false;
    }
    util::MemoryQuotaControllerPtr memController(new util::MemoryQuotaController(0));
    IndexPartitionResource partitionResource;
    partitionResource.memoryQuotaController = memController;
    partitionResource.indexPluginPath = pluginPath;
    partitionResource.taskScheduler.reset(new util::TaskScheduler());
    OfflinePartitionPtr offlinePartition = DYNAMIC_POINTER_CAST(
        OfflinePartition,
        IndexPartitionCreator::Create("", mOptions, partitionResource));
    if (offlinePartition->Open(indexPath, "", IndexPartitionSchemaPtr(),
                               mOptions, targetVersionId) != IndexPartition::OS_OK)
    {
        IE_LOG(ERROR, "Open OfflinePartition fail!");
        return false;
    }

    mOfflinePartition = offlinePartition;
    mVersion = mOfflinePartition->GetPartitionData()->GetOnDiskVersion();
    mSchema = mOfflinePartition->GetSchema();
    mPluginPath = pluginPath;
    // if (mSchema->GetSubIndexPartitionSchema())
    // {
    //     IE_LOG(ERROR, "not support sub schema!");
    //     return false;
    // }
    
    mPath = PathUtil::NormalizePath(indexPath);
    IE_LOG(INFO, "Init PartitionResourceProvider for path [%s] with version id [%d]",
           mPath.c_str(), targetVersionId);
    return true;
}

PartitionIteratorPtr PartitionResourceProvider::CreatePartitionIterator()
{
    PartitionIteratorPtr iterator(new PartitionIterator);
    if (!iterator->Init(mOfflinePartition))
    {
        IE_LOG(ERROR, "init partition iterator fail!");
        return PartitionIteratorPtr();
    }

    IE_LOG(INFO, "Create PartitionIterator for path [%s]", mPath.c_str());
    return iterator;
}

PartitionSeekerPtr PartitionResourceProvider::CreatePartitionSeeker()
{
    PartitionSeekerPtr seeker(new PartitionSeeker);
    if (!seeker->Init(mOfflinePartition))
    {
        IE_LOG(ERROR, "init partition seeker fail!");
        return PartitionSeekerPtr();
    }
    IE_LOG(INFO, "Create PartitionSeeker for path [%s]", mPath.c_str());
    return seeker;   
}
    
PartitionPatcherPtr PartitionResourceProvider::CreatePartitionPatcher(
        const IndexPartitionSchemaPtr& newSchema, const string& patchDir)
{
    int32_t newId = GetNewSchemaVersionId(newSchema);
    IE_LOG(INFO, "CreatePartitionPatcher for new schema version [%d]", newId);
    PluginManagerPtr pluginManager;
    if (mPluginManagerPair.first != newId) {
        IndexPartitionOptions options = mOptions;
        options.GetOfflineConfig().readerConfig.loadIndex = true;
        pluginManager = IndexPluginLoader::Load(
            mPluginPath, newSchema->GetIndexSchema(), options);
        CounterMapPtr counterMap(new CounterMap());
        PartitionMeta partitionMeta = mOfflinePartition->GetPartitionMeta();
        PluginResourcePtr resource(new IndexPluginResource(newSchema, mOptions,
                        counterMap, partitionMeta, mPluginPath));
        pluginManager->SetPluginResource(resource);
        mPluginManagerPair.first = newId;
        mPluginManagerPair.second = pluginManager;
    }
    else
    {
        pluginManager = mPluginManagerPair.second;
    }
    PartitionPatcherPtr patcher(new PartitionPatcher);
    if (!patcher->Init(mOfflinePartition, newSchema, patchDir, pluginManager))
    {
        IE_LOG(ERROR, "init partition patcher fail!");
        return PartitionPatcherPtr();
    }
    IE_LOG(INFO, "CreatePartitionPatcher for path [%s], patch dir path [%s]",
           mPath.c_str(), patchDir.c_str());
    return patcher;
}

IndexPartitionReaderPtr PartitionResourceProvider::GetReader() const
{
    if (mOfflinePartition)
    {
        return mOfflinePartition->GetReader();
    }
    return IndexPartitionReaderPtr();
}

PartitionSegmentIteratorPtr PartitionResourceProvider::CreatePartitionSegmentIterator() const
{
    if (mOfflinePartition)
    {
        return mOfflinePartition->GetPartitionData()->CreateSegmentIterator();
    }
    return PartitionSegmentIteratorPtr();
}

string PartitionResourceProvider::GetPatchRootDirName(
        const IndexPartitionSchemaPtr& newSchema)
{
    int32_t newId = GetNewSchemaVersionId(newSchema);
    return PartitionPatchIndexAccessor::GetPatchRootDirName(newId);
}

int32_t PartitionResourceProvider::GetNewSchemaVersionId(
        const IndexPartitionSchemaPtr& newSchema)
{
    if (newSchema->HasModifyOperations()) {
        return newSchema->GetSchemaVersionId();
    }

    schemavid_t oldId = mSchema->GetSchemaVersionId();
    schemavid_t newId = newSchema->GetSchemaVersionId();
    // if (newId == DEFAULT_SCHEMAID)
    // {
    //     // newSchema verionid : auto inc 1
    //     return oldId + 1;
    // }
    
    if (oldId >= newId)
    {
        INDEXLIB_FATAL_ERROR(BadParameter,
                             "versionid in new schema [%u] should be greater than old schema [%u]",
                             newSchema->GetSchemaVersionId(), mSchema->GetSchemaVersionId());
    }
    return newId;
}

void PartitionResourceProvider::StoreVersion(
        const IndexPartitionSchemaPtr& newSchema, versionid_t versionId)
{
    Version newVersion = GetNewVersion(newSchema, versionId);
    IndexPartitionSchemaPtr schema(newSchema->Clone());
    int32_t newId = GetNewSchemaVersionId(newSchema);
    schema->SetSchemaVersionId(newId);
    string newSchemaPath = PathUtil::JoinPath(mPath, newVersion.GetSchemaFileName());
    if (FileSystemWrapper::IsExist(newSchemaPath))
    {
        IE_LOG(INFO, "[%s] already exist, auto remove it!", newSchemaPath.c_str());
        FileSystemWrapper::DeleteFile(newSchemaPath);
    }
    IE_LOG(INFO, "store new schema [%s]", newSchemaPath.c_str());
    SchemaAdapter::StoreSchema(mPath, schema);
    DumpDeployIndexForPatchSegments(schema);

    IE_LOG(INFO, "commit new version [%d] file", newVersion.GetVersionId());
    VersionCommitter committer(
            mPath, newVersion, mOptions.GetBuildConfig().keepVersionCount);
    committer.Commit();
}

Version PartitionResourceProvider::GetNewVersion(
        const IndexPartitionSchemaPtr& newSchema, versionid_t versionId)
{
    if (versionId <= mVersion.GetVersionId())
    {
        INDEXLIB_FATAL_ERROR(InconsistentState,
                             "invalid target version id [%d], should greater than [%d]",
                             versionId, mVersion.GetVersionId());
    }

    int32_t newId = GetNewSchemaVersionId(newSchema);
    Version newVersion = mVersion;
    newVersion.SetSchemaVersionId(newId);
    newVersion.SetVersionId(versionId);
    return newVersion;
}

void PartitionResourceProvider::DumpDeployIndexForPatchSegments(
        const IndexPartitionSchemaPtr& newSchema)
{
    string patchRootPath = PathUtil::JoinPath(mPath, GetPatchRootDirName(newSchema));
    if (!FileSystemWrapper::IsExist(patchRootPath))
    {
        INDEXLIB_FATAL_ERROR(InconsistentState, "patch root dir [%s] not exist!",
                             patchRootPath.c_str());
    }

    Version::Iterator iter = mVersion.CreateIterator();
    while (iter.HasNext())
    {
        segmentid_t segId = iter.Next();
        string segPath = PathUtil::JoinPath(patchRootPath, mVersion.GetSegmentDirName(segId));
        if (!FileSystemWrapper::IsExist(segPath))
        {
            continue;
        }
        SegmentFileListWrapper::Dump(segPath);
    }
}

IE_NAMESPACE_END(partition);

