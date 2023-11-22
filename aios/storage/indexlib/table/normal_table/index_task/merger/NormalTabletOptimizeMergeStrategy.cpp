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
#include "indexlib/table/normal_table/index_task/merger/NormalTabletOptimizeMergeStrategy.h"

#include "indexlib/table/normal_table/index_task/merger/NormalTableMergeStrategyUtil.h"

namespace indexlibv2 { namespace table {
AUTIL_LOG_SETUP(indexlib.table, NormalTabletOptimizeMergeStrategy);

NormalTabletOptimizeMergeStrategy::NormalTabletOptimizeMergeStrategy() : OptimizeMergeStrategy() {}

NormalTabletOptimizeMergeStrategy::~NormalTabletOptimizeMergeStrategy() {}
std::pair<Status, int64_t>
NormalTabletOptimizeMergeStrategy::GetSegmentValidDocCount(const std::shared_ptr<framework::Segment>& segment) const
{
    auto [st, deleteDocCount] = NormalTableMergeStrategyUtil::GetDeleteDocCount(segment.get());
    RETURN2_IF_STATUS_ERROR(st, -1, "get deleted doc count failed");
    return {Status::OK(), segment->GetSegmentInfo()->docCount - deleteDocCount};
}

}} // namespace indexlibv2::table
