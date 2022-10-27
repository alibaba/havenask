#include <autil/StringUtil.h>
#include <autil/legacy/json.h>
#include <autil/legacy/string_tools.h>
#include <fslib/fs/FileSystem.h>
#include <fslib/cache/FSCacheModule.h>
#include "indexlib/partition/index_partition.h"
#include "indexlib/partition/index_partition_resource.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/partition/flushed_locator_container.h"
#include "indexlib/partition/local_index_cleaner.h"
#include "indexlib/file_system/indexlib_file_system.h"
#include "indexlib/file_system/directory_creator.h"
#include "indexlib/file_system/indexlib_file_system_metrics.h"
#include "indexlib/file_system/file_block_cache.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/partition/partition_writer.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/index_base/index_meta/index_format_version.h"
#include "indexlib/common/file_system_factory.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/misc/exception.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/plugin/index_plugin_loader.h"
#include "indexlib/util/task_scheduler.h"
#include "indexlib/util/counter/counter_map.h"
#include "indexlib/util/memory_control/memory_quota_controller.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(plugin);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, IndexPartition);

#define IE_LOG_PREFIX mPartitionName.c_str()

IndexPartition::IndexPartition()
    : mReaderHashId(0)
    , mNeedReload(false)
    , mForceSeekInfo(-1, -1)
{
    mFlushedLocatorContainer.reset(new partition::FlushedLocatorContainer(10)); 
}

IndexPartition::IndexPartition(const MemoryQuotaControllerPtr& memQuotaController,
                               const string& partitionName)
    : mReaderHashId(0)
    , mPartitionName(partitionName)
    , mMemQuotaController(memQuotaController)
    , mNeedReload(false)
    , mForceSeekInfo(-1, -1)
{
    mFlushedLocatorContainer.reset(new partition::FlushedLocatorContainer(10));
}

IndexPartition::IndexPartition(const std::string& partitionName,
                               const IndexPartitionResource& partitionResource)
    : mReaderHashId(0)
    , mPartitionName(partitionName)
    , mMemQuotaController(partitionResource.memoryQuotaController)
    , mFileBlockCache(partitionResource.fileBlockCache)
    , mTaskScheduler(partitionResource.taskScheduler)
    , mMetricProvider(partitionResource.metricProvider)
    , mIndexPluginPath(partitionResource.indexPluginPath)
    , mNeedReload(false)
    , mForceSeekInfo(-1, -1)
{
    assert(mTaskScheduler);
    assert(partitionResource.memoryQuotaController);
    mFlushedLocatorContainer.reset(new partition::FlushedLocatorContainer(10));
}

IndexPartition::~IndexPartition()
{
    if (!mOpenIndexPrimaryDir.empty()) {
        FileSystem::getCacheModule()->invalidatePath(mOpenIndexPrimaryDir);
    }

    if (!mOpenIndexSecondaryDir.empty()) {
        FileSystem::getCacheModule()->invalidatePath(mOpenIndexSecondaryDir);
    }
}

const file_system::DirectoryPtr& IndexPartition::GetRootDirectory() const
{ 
    ScopedLock lock(mDataLock);
    PartitionDataPtr partData = mPartitionDataHolder.Get();
    if (!partData)
    {
        static file_system::DirectoryPtr nullDirectory;
        return nullDirectory;
    }

    return partData->GetRootDirectory();
}

file_system::DirectoryPtr IndexPartition::GetFileSystemRootDirectory() const
{
    assert(mFileSystem);
    return DirectoryCreator::Get(mFileSystem, mFileSystem->GetRootPath(), true);
}

file_system::IndexlibFileSystemPtr IndexPartition::CreateFileSystem(
        const std::string& primaryDir, const std::string& secondaryDir)
{
    return FileSystemFactory::Create(
            primaryDir, secondaryDir, mOptions, mMetricProvider,
            mPartitionMemController, mFileBlockCache);
}

void IndexPartition::CheckPartitionMeta(
    const IndexPartitionSchemaPtr& schema,
    const PartitionMeta& partitionMeta) const
{
    assert(schema);
    const AttributeSchemaPtr& attrSchema = schema->GetAttributeSchema();

    const SortDescriptions& sortDescriptions =
        partitionMeta.GetSortDescriptions();

    if (!attrSchema && sortDescriptions.size() > 0)
    {
        INDEXLIB_FATAL_ERROR(InconsistentState,
                             "attribute schema should not be empty.");
    }
    
    for (size_t i = 0; i < sortDescriptions.size(); ++i)
    {
        string fieldName = sortDescriptions[i].sortFieldName;
        const AttributeConfigPtr& attrConfig =
            attrSchema->GetAttributeConfig(fieldName);
        if (!attrConfig)
        {
            INDEXLIB_FATAL_ERROR(InconsistentState, "No attribute config specified "
                                 "for sort field[%s] in partition meta.",
                                 fieldName.c_str());
        }
        if (attrConfig->GetPackAttributeConfig() != NULL)
        {
            INDEXLIB_FATAL_ERROR(UnSupported, 
                                 "sort field[%s] should not be in pack attribute.",
                                 fieldName.c_str());
        }
        if (!attrConfig->IsNormal())
        {
            INDEXLIB_FATAL_ERROR(InconsistentState, 
                                 "sort field[%s] should not be disable.",
                                 fieldName.c_str());
        }
    }
}

PartitionMeta IndexPartition::GetPartitionMeta() const
{
    PartitionDataPtr partitionData = mPartitionDataHolder.Get();
    assert(partitionData); 
    return partitionData->GetPartitionMeta();
}

IndexlibFileSystemMetrics IndexPartition::TEST_GetFileSystemMetrics() const
{
    autil::ScopedLock lock(mDataLock); 
    return mFileSystem->GetFileSystemMetrics();
}

void IndexPartition::Reset()
{
    mFlushedLocatorContainer->Clear();
    mPartitionDataHolder.Reset();
    mFileSystem.reset();
}

IndexPartition::OpenStatus IndexPartition::Open(const string& primaryDir,
                                                const string& secondaryDir,
                                                const IndexPartitionSchemaPtr& schema,
                                                const IndexPartitionOptions& options,
                                                versionid_t versionId)
{
    mOpenIndexPrimaryDir = primaryDir;
    mOpenIndexSecondaryDir = secondaryDir;
    mOpenSchema = schema;
    mOpenOptions = options;
    mOptions = options;

    assert(mMemQuotaController);
    mPartitionMemController.reset(
        new util::PartitionMemoryQuotaController(mMemQuotaController, mPartitionName));
    IE_PREFIX_LOG(INFO, "current free memory is %ld MB", 
                  mPartitionMemController->GetFreeQuota() / (1024 * 1024));
    mPartitionMemController->SetBuildMemoryQuotaControler(mBuildMemoryQuotaController);
    mFileSystem = CreateFileSystem(primaryDir, secondaryDir);
    return OS_OK;
}

document::Locator IndexPartition::GetLastFlushedLocator()
{
    return mFlushedLocatorContainer->GetLastFlushedLocator();
}

bool IndexPartition::HasFlushingLocator()
{
    return mFlushedLocatorContainer->HasFlushingLocator();
}

void IndexPartition::SetBuildMemoryQuotaControler(
    const util::QuotaControlPtr& buildMemoryQuotaControler)
{
    mBuildMemoryQuotaController = buildMemoryQuotaControler;
    if (mPartitionMemController)
    {
        mPartitionMemController->SetBuildMemoryQuotaControler(buildMemoryQuotaControler);
    }
}

void IndexPartition::Close()
{
    mPartitionMemController.reset();
}

bool IndexPartition::CleanIndexFiles(const string& primaryPath, const string& secondaryPath,
                                     const vector<versionid_t>& keepVersionIds)
{
    try
    {
        auto fs = IndexlibFileSystemCreator::Create(primaryPath, secondaryPath);
        DirectoryPtr rootDir = DirectoryCreator::Get(fs, primaryPath, false);
        if (!rootDir)
        {
            return false;
        }
        LocalIndexCleaner cleaner(rootDir);
        return cleaner.Clean(keepVersionIds);
    }
    catch (const exception& e)
    {
        IE_LOG(ERROR, "caught exception: %s", e.what());
    }
    catch (...)
    {
        IE_LOG(ERROR, "caught unknown exception");
    }
    return false;
}

int64_t IndexPartition::GetUsedMemory() const
{
    return mPartitionMemController->GetUsedQuota();
}

#undef IE_LOG_PREFIX

IE_NAMESPACE_END(partition);

