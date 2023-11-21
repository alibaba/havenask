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
#include "indexlib/merger/merge_strategy/align_version_merge_strategy.h"

#include <iosfwd>

#include "indexlib/config/merge_strategy_parameter.h"
#include "indexlib/framework/LevelInfo.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/merger/merge_task.h"
#include "indexlib/merger/segment_directory.h"

using namespace std;

using namespace indexlib::config;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, AlignVersionMergeStrategy);

AlignVersionMergeStrategy::AlignVersionMergeStrategy(const SegmentDirectoryPtr& segDir,
                                                     const IndexPartitionSchemaPtr& schema)
    : MergeStrategy(segDir, schema)
{
}

AlignVersionMergeStrategy::~AlignVersionMergeStrategy() {}

void AlignVersionMergeStrategy::SetParameter(const MergeStrategyParameter& param) {}

string AlignVersionMergeStrategy::GetParameter() const { return ""; }

MergeTask AlignVersionMergeStrategy::CreateMergeTask(const index_base::SegmentMergeInfos& segMergeInfos,
                                                     const indexlibv2::framework::LevelInfo& levelInfo)
{
    MergeTask mergeTask;
    return mergeTask;
}

MergeTask AlignVersionMergeStrategy::CreateMergeTaskForOptimize(const index_base::SegmentMergeInfos& segMergeInfos,
                                                                const indexlibv2::framework::LevelInfo& levelInfo)
{
    return CreateMergeTask(segMergeInfos, levelInfo);
}
}} // namespace indexlib::merger
