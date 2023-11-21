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

#include "indexlib/config/SortDescription.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/index_base/common_branch_hinter_option.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/indexlib.h"
#include "indexlib/merger/dump_strategy.h"
#include "indexlib/merger/index_partition_merger.h"
#include "indexlib/merger/segment_directory.h"
#include "indexlib/misc/common.h"
#include "indexlib/misc/log.h"
#include "indexlib/util/metrics/MetricProvider.h"

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
