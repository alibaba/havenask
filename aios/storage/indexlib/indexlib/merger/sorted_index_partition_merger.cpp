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
#include "indexlib/merger/sorted_index_partition_merger.h"

#include "indexlib/document/locator.h"
#include "indexlib/index/merger_util/reclaim_map/sorted_reclaim_map_creator.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/plugin/plugin_manager.h"

using namespace std;

using namespace indexlib::config;
using namespace indexlib::common;
using namespace indexlib::index;
using namespace indexlib::index_base;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, SortedIndexPartitionMerger);

SortedIndexPartitionMerger::SortedIndexPartitionMerger(
    const SegmentDirectoryPtr& segDir, const IndexPartitionSchemaPtr& schema, const IndexPartitionOptions& options,
    const indexlibv2::config::SortDescriptions& sortDescs, DumpStrategyPtr dumpStrategy,
    util::MetricProviderPtr metricProvider, const plugin::PluginManagerPtr& pluginManager,
    const CommonBranchHinterOption& branchOption, const PartitionRange& targetRange)
    : IndexPartitionMerger(segDir, schema, options, dumpStrategy, metricProvider, pluginManager, branchOption,
                           targetRange)
    , mSortDescs(sortDescs)
{
}

SortedIndexPartitionMerger::~SortedIndexPartitionMerger() {}

ReclaimMapCreatorPtr SortedIndexPartitionMerger::CreateReclaimMapCreator()
{
    return ReclaimMapCreatorPtr(new SortedReclaimMapCreator(mMergeConfig.truncateOptionConfig != NULL,
                                                            mAttrReaderContainer, mSchema, mSortDescs));
}
}} // namespace indexlib::merger
