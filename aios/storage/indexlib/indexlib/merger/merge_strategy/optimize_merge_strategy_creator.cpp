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
#include "indexlib/merger/merge_strategy/optimize_merge_strategy_creator.h"

#include "indexlib/merger/merge_strategy/key_value_optimize_merge_strategy.h"
#include "indexlib/merger/merge_strategy/optimize_merge_strategy.h"

using namespace std;
using namespace indexlib::config;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, OptimizeMergeStrategyCreator);

OptimizeMergeStrategyCreator::OptimizeMergeStrategyCreator() {}

OptimizeMergeStrategyCreator::~OptimizeMergeStrategyCreator() {}

string OptimizeMergeStrategyCreator::GetIdentifier() const { return OPTIMIZE_MERGE_STRATEGY_STR; }

MergeStrategyPtr OptimizeMergeStrategyCreator::Create(const SegmentDirectoryPtr& segDir,
                                                      const IndexPartitionSchemaPtr& schema) const
{
    TableType tableType = schema->GetTableType();
    switch (tableType) {
    case tt_kv:
    case tt_kkv:
        return MergeStrategyPtr(new KeyValueOptimizeMergeStrategy(segDir, schema));
    default:
        return MergeStrategyPtr(new OptimizeMergeStrategy(segDir, schema));
    }
}
}} // namespace indexlib::merger
