#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/index_base/segment/segment_directory_creator.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/index_base/index_meta/index_format_version.h"
#include "indexlib/partition/custom_partition_data.h"
#include "indexlib/file_system/indexlib_file_system.h"
#include "indexlib/file_system/indexlib_file_system_impl.h"
#include "indexlib/file_system/directory_creator.h"
#include "indexlib/util/memory_control/memory_quota_controller_creator.h"
#include "indexlib/util/counter/counter_map.h"
#include "indexlib/plugin/plugin_manager.h"

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(plugin);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, PartitionDataCreator);

PartitionDataCreator::PartitionDataCreator() 
{
}

PartitionDataCreator::~PartitionDataCreator() 
{
}

BuildingPartitionDataPtr PartitionDataCreator::CreateBuildingPartitionData(
        const IndexlibFileSystemPtr& fileSystem, 
        const IndexPartitionSchemaPtr& schema,
        const IndexPartitionOptions& options,
        const util::PartitionMemoryQuotaControllerPtr& memController,
        const DumpSegmentContainerPtr& container,
        index_base::Version version,
        misc::MetricProviderPtr metricProvider,
        const string& dir,
        const InMemorySegmentPtr& inMemSegment,
        const CounterMapPtr& counterMap,
        const plugin::PluginManagerPtr& pluginManager)
{
    bool hasSub = false;
    IndexPartitionSchemaPtr subSchema = schema->GetSubIndexPartitionSchema();
    if (subSchema)
    {
        hasSub = true;
    }
    SegmentDirectoryPtr segDir = SegmentDirectoryCreator::Create(
            fileSystem, version, dir, &options, hasSub);
    if (options.IsOffline())
    {
        segDir->UpdateSchemaVersionId(schema->GetSchemaVersionId());
        segDir->SetOngoingModifyOperations(options.GetOngoingModifyOperationIds());
    }

    // check index format version compatible
    IndexFormatVersion onDiskFormatVersion = segDir->GetIndexFormatVersion();
    onDiskFormatVersion.CheckCompatible(INDEX_FORMAT_VERSION);

    BuildingPartitionDataPtr partitionData(new BuildingPartitionData(
                    options, schema, memController, container,
                    metricProvider, counterMap, pluginManager));
    partitionData->Open(segDir);
    if (inMemSegment)
    {
        partitionData->InheritInMemorySegment(inMemSegment);
    }
    return partitionData;
}

OnDiskPartitionDataPtr PartitionDataCreator::CreateOnDiskPartitionData(
        const file_system::IndexlibFileSystemPtr& fileSystem, 
        index_base::Version version,
        const std::string& dir, 
        bool hasSubSegment,
        bool needDeletionMap,
        const plugin::PluginManagerPtr& pluginManager)
{
    // use NULL: will not trigger offline recover, for merger
    SegmentDirectoryPtr segDir = SegmentDirectoryCreator::Create(
            fileSystem, version, dir, NULL, hasSubSegment);
    OnDiskPartitionDataPtr partitionData(new OnDiskPartitionData(pluginManager));
    partitionData->Open(segDir, needDeletionMap);
    return partitionData;
}

OnDiskPartitionDataPtr PartitionDataCreator::CreateOnDiskPartitionData(
        const file_system::IndexlibFileSystemPtr& fileSystem, 
        const config::IndexPartitionSchemaPtr& schema,
        index_base::Version version,
        const std::string& dir,
        bool needDeletionMap,
        const plugin::PluginManagerPtr& pluginManager)
{
    bool hasSubSegment = false;
    if (schema->GetSubIndexPartitionSchema())
    {
        hasSubSegment = true;
    }
    return CreateOnDiskPartitionData(fileSystem, version, dir, 
            hasSubSegment, needDeletionMap, pluginManager);
}

OnDiskPartitionDataPtr PartitionDataCreator::CreateOnDiskPartitionData(
	const IndexPartitionOptions& options,
	const std::vector<std::string>& roots,
	const std::vector<index_base::Version>& versions,
        bool hasSubSegment,
        bool needDeletionMap,
        const plugin::PluginManagerPtr& pluginManager)
{
    DirectoryVector directoryVec = CreateDirectoryVector(options, roots);
    SegmentDirectoryPtr segDir = 
      SegmentDirectoryCreator::Create(directoryVec, versions, 
				      hasSubSegment);
    OnDiskPartitionDataPtr onDiskPartitionData(new OnDiskPartitionData(pluginManager));
    onDiskPartitionData->Open(segDir, needDeletionMap);
    return onDiskPartitionData;
}

OnDiskPartitionDataPtr PartitionDataCreator::CreateOnDiskPartitionData(const IndexPartitionOptions& options,
        const std::vector<std::string>& roots,
        bool hasSubSegment,
        bool needDeletionMap,
        const plugin::PluginManagerPtr& pluginManager)
{
    DirectoryVector directoryVec = CreateDirectoryVector(options, roots);
    SegmentDirectoryPtr segDir = SegmentDirectoryCreator::Create(directoryVec, 
            hasSubSegment);
    OnDiskPartitionDataPtr onDiskPartitionData(new OnDiskPartitionData(pluginManager));
    onDiskPartitionData->Open(segDir, needDeletionMap);
    return onDiskPartitionData;
}

file_system::DirectoryVector PartitionDataCreator::CreateDirectoryVector(
    const IndexPartitionOptions& options,
    const std::vector<std::string>& roots)
{
    //TODO:
    DirectoryVector directoryVec;
    for (size_t i = 0; i < roots.size(); ++i)
    {
        //simple file system
        util::PartitionMemoryQuotaControllerPtr quotaController =
            util::MemoryQuotaControllerCreator::CreatePartitionMemoryController();
        IndexlibFileSystemPtr fileSystem(new IndexlibFileSystemImpl(roots[i]));
        FileSystemOptions fileSystemOptions;
        fileSystemOptions.needFlush = false;
        fileSystemOptions.useCache = false;
        fileSystemOptions.enablePathMetaContainer = true;
        fileSystemOptions.memoryQuotaController = quotaController;
        fileSystemOptions.loadConfigList = 
            options.GetOnlineConfig().loadConfigList;
        fileSystem->Init(fileSystemOptions);

        DirectoryPtr rootDirectory = DirectoryCreator::Get(fileSystem, 
                fileSystem->GetRootPath(), true);
        if (!rootDirectory)
        {
            INDEXLIB_FATAL_ERROR(IndexCollapsed, "Failed to get directory [%s]",
                    roots[i].c_str());
        }
        directoryVec.push_back(rootDirectory);
    }
    return directoryVec;
}

CustomPartitionDataPtr PartitionDataCreator::CreateCustomPartitionData(
        const IndexlibFileSystemPtr& fileSystem, 
        const IndexPartitionSchemaPtr& schema,
        const IndexPartitionOptions& options,
        const PartitionMemoryQuotaControllerPtr& memController,
        const BlockMemoryQuotaControllerPtr& tableWriterMemController,
        index_base::Version version,
        misc::MetricProviderPtr metricProvider,
        const CounterMapPtr& counterMap,
        const PluginManagerPtr& pluginManager,
        const DumpSegmentQueuePtr& dumpSegmentQueue, 
        int64_t reclaimTimestamp,
        bool ignoreInMemVersion)
{
    SegmentDirectoryPtr segDir;
    if (ignoreInMemVersion)
    {
        segDir = SegmentDirectoryCreator::Create(
            fileSystem, version, "", NULL, false);
    }
    else
    {
        segDir = SegmentDirectoryCreator::Create(
            fileSystem, version, "", &options, false);
    }
    CustomPartitionDataPtr partitionData(new CustomPartitionData(
                                             options, schema, memController,
                                             tableWriterMemController, metricProvider, 
                                             counterMap, pluginManager, dumpSegmentQueue, reclaimTimestamp));
    if (!partitionData->Open(segDir))
    {
        return CustomPartitionDataPtr();
    }
    return partitionData;
}

IE_NAMESPACE_END(partition);

