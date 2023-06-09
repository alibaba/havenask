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
#include "indexlib/merger/merge_partition_data_creator.h"

#include "indexlib/index_base/segment/segment_directory_creator.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/util/counter/CounterMap.h"
#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"

using namespace std;
using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::index;
using namespace indexlib::common;
using namespace indexlib::plugin;
using namespace indexlib::index_base;

using namespace indexlib::util;
namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, MergePartitionDataCreator);

MergePartitionDataCreator::MergePartitionDataCreator() {}

MergePartitionDataCreator::~MergePartitionDataCreator() {}

MergePartitionDataPtr MergePartitionDataCreator::CreateMergePartitionData(const file_system::DirectoryPtr& dir,
                                                                          index_base::Version version,
                                                                          bool hasSubSegment,
                                                                          const plugin::PluginManagerPtr& pluginManager)
{
    // use NULL: will not trigger offline recover, for merger
    index_base::SegmentDirectoryPtr segDir = SegmentDirectoryCreator::Create(dir, version, NULL, hasSubSegment);
    MergePartitionDataPtr partitionData(new MergePartitionData(pluginManager));
    partitionData->Open(segDir);
    return partitionData;
}

MergePartitionDataPtr MergePartitionDataCreator::CreateMergePartitionData(const file_system::DirectoryPtr& dir,
                                                                          const config::IndexPartitionSchemaPtr& schema,
                                                                          index_base::Version version,
                                                                          const plugin::PluginManagerPtr& pluginManager)
{
    bool hasSubSegment = false;
    if (schema->GetSubIndexPartitionSchema()) {
        hasSubSegment = true;
    }
    return CreateMergePartitionData(dir, version, hasSubSegment, pluginManager);
}

MergePartitionDataPtr
MergePartitionDataCreator::CreateMergePartitionData(const std::vector<file_system::DirectoryPtr>& rootDirs,
                                                    const std::vector<index_base::Version>& versions,
                                                    bool hasSubSegment, const plugin::PluginManagerPtr& pluginManager)
{
    assert(rootDirs.size() == versions.size());
    index_base::SegmentDirectoryPtr segDir = SegmentDirectoryCreator::Create(rootDirs, versions, hasSubSegment);
    MergePartitionDataPtr onDiskPartitionData(new MergePartitionData(pluginManager));
    onDiskPartitionData->Open(segDir);
    return onDiskPartitionData;
}
}} // namespace indexlib::merger
