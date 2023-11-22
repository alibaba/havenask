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
#pragma once

#include <vector>

#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/indexlib.h"
#include "indexlib/merger/merge_partition_data.h"
#include "indexlib/misc/common.h"
#include "indexlib/misc/log.h"

namespace indexlib { namespace merger {

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
        const file_system::DirectoryPtr& dir, index_base::Version version = index_base::Version(INVALID_VERSIONID),
        bool hasSubSegment = false, const plugin::PluginManagerPtr& pluginManager = plugin::PluginManagerPtr());

    static MergePartitionDataPtr
    CreateMergePartitionData(const file_system::DirectoryPtr& dir, const config::IndexPartitionSchemaPtr& schema,
                             index_base::Version version = index_base::Version(INVALID_VERSIONID),
                             const plugin::PluginManagerPtr& pluginManager = plugin::PluginManagerPtr());

    static MergePartitionDataPtr
    CreateMergePartitionData(const std::vector<file_system::DirectoryPtr>& rootDirs,
                             const std::vector<index_base::Version>& versions, bool hasSubSegment = false,
                             const plugin::PluginManagerPtr& pluginManager = plugin::PluginManagerPtr());

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MergePartitionDataCreator);
}} // namespace indexlib::merger
