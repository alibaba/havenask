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
#include "indexlib/merger/split_strategy/default_range_split_strategy.h"

using namespace std;
using namespace autil::legacy::json;

using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::index_base;
using namespace indexlib::util;

namespace indexlib { namespace merger {

IE_LOG_SETUP(merger, DefaultRangeSplitStrategy);

DefaultRangeSplitStrategy::DefaultRangeSplitStrategy(
    SegmentDirectoryPtr segDir, IndexPartitionSchemaPtr schema, OfflineAttributeSegmentReaderContainerPtr attrReaders,
    const SegmentMergeInfos& segMergeInfos, const MergePlan& plan,
    std::map<segmentid_t, index::SegmentMetricsUpdaterPtr> hintDocInfos, const util::MetricProviderPtr& metrics)
    : SplitSegmentStrategy(segDir, schema, attrReaders, segMergeInfos, plan, hintDocInfos, metrics)
{
}

DefaultRangeSplitStrategy::~DefaultRangeSplitStrategy() {}
}} // namespace indexlib::merger
