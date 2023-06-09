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
#include "indexlib/merger/merge_strategy/merge_strategy_factory.h"

#include <sstream>

#include "indexlib/merger/merge_strategy/align_version_merge_strategy.h"
#include "indexlib/merger/merge_strategy/balance_tree_merge_strategy.h"
#include "indexlib/merger/merge_strategy/large_time_range_selection_merge_strategy.h"
#include "indexlib/merger/merge_strategy/optimize_merge_strategy.h"
#include "indexlib/merger/merge_strategy/optimize_merge_strategy_creator.h"
#include "indexlib/merger/merge_strategy/priority_queue_merge_strategy.h"
#include "indexlib/merger/merge_strategy/realtime_merge_strategy.h"
#include "indexlib/merger/merge_strategy/segment_count_merge_strategy.h"
#include "indexlib/merger/merge_strategy/shard_based_merge_strategy.h"
#include "indexlib/merger/merge_strategy/specific_segments_merge_strategy.h"
#include "indexlib/merger/merge_strategy/temperature_merge_strategy.h"
#include "indexlib/merger/merge_strategy/time_series_merge_strategy.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil::legacy;

using namespace indexlib::common;
using namespace indexlib::config;

namespace indexlib { namespace merger {

MergeStrategyFactory::MergeStrategyFactory() { Init(); }

MergeStrategyFactory::~MergeStrategyFactory() {}

void MergeStrategyFactory::Init()
{
    RegisterCreator(MergeStrategyCreatorPtr(new OptimizeMergeStrategyCreator()));
    RegisterCreator(MergeStrategyCreatorPtr(new BalanceTreeMergeStrategy::Creator()));
    RegisterCreator(MergeStrategyCreatorPtr(new RealtimeMergeStrategy::Creator()));
    RegisterCreator(MergeStrategyCreatorPtr(new PriorityQueueMergeStrategy::Creator()));
    RegisterCreator(MergeStrategyCreatorPtr(new ShardBasedMergeStrategy::Creator()));
    RegisterCreator(MergeStrategyCreatorPtr(new AlignVersionMergeStrategy::Creator()));
    RegisterCreator(MergeStrategyCreatorPtr(new TimeSeriesMergeStrategy::Creator()));
    RegisterCreator(MergeStrategyCreatorPtr(new LargeTimeRangeSelectionMergeStrategy::Creator()));
    RegisterCreator(MergeStrategyCreatorPtr(new TemperatureMergeStrategy::Creator()));
    RegisterCreator(MergeStrategyCreatorPtr(new SegmentCountMergeStrategy::Creator()));

    // for test purpose
    RegisterCreator(MergeStrategyCreatorPtr(new SpecificSegmentsMergeStrategy::Creator()));
}

void MergeStrategyFactory::RegisterCreator(MergeStrategyCreatorPtr creator)
{
    assert(creator != NULL);

    autil::ScopedLock l(mLock);
    string identifier = creator->GetIdentifier();
    CreatorMap::iterator it = mCreatorMap.find(identifier);
    if (it != mCreatorMap.end()) {
        mCreatorMap.erase(it);
    }
    mCreatorMap.insert(std::make_pair(identifier, creator));
}

MergeStrategyPtr MergeStrategyFactory::CreateMergeStrategy(const string& identifier, const SegmentDirectoryPtr& segDir,
                                                           const IndexPartitionSchemaPtr& schema)
{
    autil::ScopedLock l(mLock);
    CreatorMap::const_iterator it = mCreatorMap.find(identifier);
    if (it != mCreatorMap.end()) {
        return it->second->Create(segDir, schema);
    } else {
        stringstream s;
        s << "Merge strategy identifier " << identifier << " not implemented yet!";
        INDEXLIB_THROW(util::UnImplementException, "%s", s.str().c_str());
    }
    return MergeStrategyPtr();
}
}} // namespace indexlib::merger
