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
#ifndef __INDEXLIB_SPECIFIC_SEGMENTS_MERGE_STRATEGY_H
#define __INDEXLIB_SPECIFIC_SEGMENTS_MERGE_STRATEGY_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/merger/merge_strategy/merge_strategy.h"
#include "indexlib/merger/merge_strategy/merge_strategy_creator.h"

namespace indexlib { namespace merger {

// for test
class SpecificSegmentsMergeStrategy : public MergeStrategy
{
public:
    SpecificSegmentsMergeStrategy(const merger::SegmentDirectoryPtr& segDir,
                                  const config::IndexPartitionSchemaPtr& schema);

    ~SpecificSegmentsMergeStrategy();

    DECLARE_MERGE_STRATEGY_CREATOR(SpecificSegmentsMergeStrategy, SPECIFIC_SEGMENTS_MERGE_STRATEGY_STR);

public:
    void SetParameter(const config::MergeStrategyParameter& param) override;

    std::string GetParameter() const override;

    MergeTask CreateMergeTask(const index_base::SegmentMergeInfos& segMergeInfos,
                              const indexlibv2::framework::LevelInfo& levelInfo) override;

    MergeTask CreateMergeTaskForOptimize(const index_base::SegmentMergeInfos& segMergeInfos,
                                         const indexlibv2::framework::LevelInfo& levelInfo) override;

private:
    bool GetSegmentMergeInfo(segmentid_t segmentId, const index_base::SegmentMergeInfos& segMergeInfos,
                             index_base::SegmentMergeInfo& segMergeInfo);

private:
    std::vector<std::vector<segmentid_t>> mTargetSegmentIds;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SpecificSegmentsMergeStrategy);
}} // namespace indexlib::merger

#endif //__INDEXLIB_SPECIFIC_SEGMENTS_MERGE_STRATEGY_H
