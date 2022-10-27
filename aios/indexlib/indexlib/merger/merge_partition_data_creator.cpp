#include "indexlib/merger/merge_partition_data_creator.h"
#include "indexlib/index_base/segment/segment_directory_creator.h"
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
IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, MergePartitionDataCreator);

MergePartitionDataCreator::MergePartitionDataCreator() 
{
}

MergePartitionDataCreator::~MergePartitionDataCreator() 
{
}

MergePartitionDataPtr MergePartitionDataCreator::CreateMergePartitionData(
        const file_system::IndexlibFileSystemPtr& fileSystem, 
        index_base::Version version,
        const std::string& dir, 
        bool hasSubSegment,
        const plugin::PluginManagerPtr& pluginManager)
{
    // use NULL: will not trigger offline recover, for merger
    index_base::SegmentDirectoryPtr segDir = SegmentDirectoryCreator::Create(
            fileSystem, version, dir, NULL, hasSubSegment);
    MergePartitionDataPtr partitionData(new MergePartitionData(pluginManager));
    partitionData->Open(segDir);
    return partitionData;
}

MergePartitionDataPtr MergePartitionDataCreator::CreateMergePartitionData(
        const file_system::IndexlibFileSystemPtr& fileSystem, 
        const config::IndexPartitionSchemaPtr& schema,
        index_base::Version version,
        const std::string& dir,
        const plugin::PluginManagerPtr& pluginManager)
{
    bool hasSubSegment = false;
    if (schema->GetSubIndexPartitionSchema())
    {
        hasSubSegment = true;
    }
    return CreateMergePartitionData(fileSystem, version, dir, 
            hasSubSegment, pluginManager);
}

MergePartitionDataPtr MergePartitionDataCreator::CreateMergePartitionData(
        const std::vector<std::string>& roots,
        const std::vector<index_base::Version>& versions,
        bool hasSubSegment,
        const plugin::PluginManagerPtr& pluginManager)
{
    assert(roots.size() == versions.size());
    DirectoryVector directoryVec = CreateDirectoryVector(roots);
    index_base::SegmentDirectoryPtr segDir = SegmentDirectoryCreator::Create(
            directoryVec, versions, hasSubSegment);
    MergePartitionDataPtr onDiskPartitionData(new MergePartitionData(pluginManager));
    onDiskPartitionData->Open(segDir);
    return onDiskPartitionData;    
}

file_system::DirectoryVector MergePartitionDataCreator::CreateDirectoryVector(
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

IE_NAMESPACE_END(merger);

