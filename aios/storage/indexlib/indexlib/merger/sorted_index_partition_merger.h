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
#ifndef __INDEXLIB_SORTED_INDEX_PARTITION_MERGER_H
#define __INDEXLIB_SORTED_INDEX_PARTITION_MERGER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/indexlib.h"
#include "indexlib/merger/index_partition_merger.h"

DECLARE_REFERENCE_CLASS(plugin, PluginManager);
namespace indexlib { namespace merger {

class SortedIndexPartitionMerger : public IndexPartitionMerger
{
public:
    SortedIndexPartitionMerger(const SegmentDirectoryPtr& segDir, const config::IndexPartitionSchemaPtr& schema,
                               const config::IndexPartitionOptions& options,
                               const indexlibv2::config::SortDescriptions& sortDesc, DumpStrategyPtr dumpStrategy,
                               util::MetricProviderPtr metricProvider, const plugin::PluginManagerPtr& pluginManager,
                               const index_base::CommonBranchHinterOption& branchOption,
                               const PartitionRange& targetRange);
    ~SortedIndexPartitionMerger();

protected:
    bool IsSortMerge() const override { return true; }

    index::ReclaimMapCreatorPtr CreateReclaimMapCreator() override;

private:
    indexlibv2::config::SortDescriptions mSortDescs;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SortedIndexPartitionMerger);
}} // namespace indexlib::merger

#endif //__INDEXLIB_SORTED_INDEX_PARTITION_MERGER_H
