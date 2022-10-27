#ifndef __INDEXLIB_PARTITION_DATA_CREATOR_H
#define __INDEXLIB_PARTITION_DATA_CREATOR_H

#include <tr1/memory>
#include "indexlib/misc/metric_provider.h"
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/segment/in_memory_segment_container.h"
#include "indexlib/partition/building_partition_data.h"
#include "indexlib/partition/custom_partition_data.h"
#include "indexlib/partition/on_disk_partition_data.h"
#include "indexlib/partition/segment/dump_segment_container.h"
#include "indexlib/partition/segment/dump_segment_queue.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/partition_data.h"

DECLARE_REFERENCE_CLASS(util, CounterMap);
DECLARE_REFERENCE_CLASS(plugin, PluginManager);
DECLARE_REFERENCE_CLASS(file_system, IndexlibFileSystem);
DECLARE_REFERENCE_CLASS(file_system, Directory);

IE_NAMESPACE_BEGIN(partition);

class PartitionDataCreator
{
public:
    PartitionDataCreator();
    ~PartitionDataCreator();
 
public:
    // by default:
    // use file system's root as partition root
    static BuildingPartitionDataPtr CreateBuildingPartitionData(
            const file_system::IndexlibFileSystemPtr& fileSystem, 
            const config::IndexPartitionSchemaPtr& schema,
            const config::IndexPartitionOptions& options,
            const util::PartitionMemoryQuotaControllerPtr& memController,
            const DumpSegmentContainerPtr& container,
            index_base::Version version = INVALID_VERSION,
            misc::MetricProviderPtr metricProvider = misc::MetricProviderPtr(),
            const std::string& dir = "",
            const index_base::InMemorySegmentPtr& inMemSegment = index_base::InMemorySegmentPtr(),
            const util::CounterMapPtr& counterMap = util::CounterMapPtr(),
            const plugin::PluginManagerPtr& pluginManager = plugin::PluginManagerPtr());

    // by default:
    // use file system's root as partition root
    // use SegmentDirectory as default segment directory
    static OnDiskPartitionDataPtr CreateOnDiskPartitionData(
            const file_system::IndexlibFileSystemPtr& fileSystem, 
            index_base::Version version = INVALID_VERSION,
            const std::string& dir = "",
            bool hasSubSegment = false,
            bool needDeletionMap = false,
            const plugin::PluginManagerPtr& pluginManager
            = plugin::PluginManagerPtr());

    static OnDiskPartitionDataPtr CreateOnDiskPartitionData(
            const file_system::IndexlibFileSystemPtr& fileSystem, 
            const config::IndexPartitionSchemaPtr& schema,
            index_base::Version version = INVALID_VERSION,
            const std::string& dir = "",
            bool needDeletionMap = false,
            const plugin::PluginManagerPtr& pluginManager = plugin::PluginManagerPtr());
    
    static OnDiskPartitionDataPtr CreateOnDiskPartitionData(
            const config::IndexPartitionOptions& options,
            const std::vector<std::string>& roots,
            const std::vector<index_base::Version>& versions,
            bool hasSubSegment = false,
            bool needDeletionMap = false,
            const plugin::PluginManagerPtr& pluginManager = plugin::PluginManagerPtr());
    static OnDiskPartitionDataPtr CreateOnDiskPartitionData(
            const config::IndexPartitionOptions& options,
            const std::vector<std::string>& roots,
            bool hasSubSegment = false,
            bool needDeletionMap = false,
            const plugin::PluginManagerPtr& pluginManager = plugin::PluginManagerPtr());

    static CustomPartitionDataPtr CreateCustomPartitionData(
        const file_system::IndexlibFileSystemPtr& fileSystem, 
        const config::IndexPartitionSchemaPtr& schema,
        const config::IndexPartitionOptions& options,
        const util::PartitionMemoryQuotaControllerPtr& memController,
        const util::BlockMemoryQuotaControllerPtr& tableWriterMemController,
        index_base::Version version,
        misc::MetricProviderPtr metricProvider,
        const util::CounterMapPtr& counterMap,
        const plugin::PluginManagerPtr& pluginManager,
        const DumpSegmentQueuePtr& dumpSegmentQueue,
        int64_t reclaimTimestamp = -1,
        bool ignoreInMemVersion = false);
    
private:
    static std::vector<file_system::DirectoryPtr> CreateDirectoryVector(const config::IndexPartitionOptions& options,
            const std::vector<std::string>& roots);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PartitionDataCreator);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_PARTITION_DATA_CREATOR_H
