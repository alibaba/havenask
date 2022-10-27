#ifndef __INDEXLIB_MERGE_PARTITION_DATA_CREATOR_H
#define __INDEXLIB_MERGE_PARTITION_DATA_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/merger/merge_partition_data.h"
#include "indexlib/misc/metric_provider.h"

DECLARE_REFERENCE_CLASS(util, CounterMap);
DECLARE_REFERENCE_CLASS(plugin, PluginManager);

IE_NAMESPACE_BEGIN(merger);

class MergePartitionDataCreator
{
public:
    MergePartitionDataCreator();
    ~MergePartitionDataCreator();
 
public:
    // by default:
    // use file system's root as partition root
    // use SegmentDirectory as default segment directory
    static MergePartitionDataPtr CreateMergePartitionData(
            const file_system::IndexlibFileSystemPtr& fileSystem, 
            index_base::Version version = INVALID_VERSION,
            const std::string& dir = "",
            bool hasSubSegment = false,
            const plugin::PluginManagerPtr& pluginManager
            = plugin::PluginManagerPtr());

    static MergePartitionDataPtr CreateMergePartitionData(
            const file_system::IndexlibFileSystemPtr& fileSystem, 
            const config::IndexPartitionSchemaPtr& schema,
            index_base::Version version = INVALID_VERSION,
            const std::string& dir = "",
            const plugin::PluginManagerPtr& pluginManager = plugin::PluginManagerPtr());

    static MergePartitionDataPtr CreateMergePartitionData(
            const std::vector<std::string>& roots,
            const std::vector<index_base::Version>& versions,
            bool hasSubSegment = false,
            const plugin::PluginManagerPtr& pluginManager = plugin::PluginManagerPtr());

private:
    static file_system::DirectoryVector CreateDirectoryVector(
            const std::vector<std::string>& roots);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MergePartitionDataCreator);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_MERGE_PARTITION_DATA_CREATOR_H
