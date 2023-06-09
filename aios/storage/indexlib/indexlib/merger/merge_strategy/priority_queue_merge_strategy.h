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
#ifndef __INDEXLIB_PRIORITY_QUEUE_MERGE_STRATEGY_H
#define __INDEXLIB_PRIORITY_QUEUE_MERGE_STRATEGY_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/merger/merge_strategy/merge_strategy.h"
#include "indexlib/merger/merge_strategy/merge_strategy_factory.h"
#include "indexlib/merger/merge_strategy/priority_queue_merge_strategy_define.h"

namespace indexlib { namespace merger {

class PriorityQueueMergeStrategy : public MergeStrategy
{
public:
    PriorityQueueMergeStrategy(const merger::SegmentDirectoryPtr& segDir,
                               const config::IndexPartitionSchemaPtr& schema);

    ~PriorityQueueMergeStrategy();

    DECLARE_MERGE_STRATEGY_CREATOR(PriorityQueueMergeStrategy, PRIORITY_QUEUE_MERGE_STRATEGY_STR);

public:
    void SetParameter(const config::MergeStrategyParameter& param) override;

    std::string GetParameter() const override;

    MergeTask CreateMergeTask(const index_base::SegmentMergeInfos& segMergeInfos,
                              const indexlibv2::framework::LevelInfo& levelInfo) override;

    MergeTask CreateMergeTaskForOptimize(const index_base::SegmentMergeInfos& segMergeInfos,
                                         const indexlibv2::framework::LevelInfo& levelInfo) override;

private:
    void PutSegMergeInfosToQueue(const index_base::SegmentMergeInfos& segMergeInfos, PriorityQueue& priorityQueue);

    void GenerateMergeSegments(PriorityQueue& queue, index_base::SegmentMergeInfos& needMergeSegments);

    MergeTask GenerateMergeTask(const index_base::SegmentMergeInfos& mergeInfos);

    bool DecodeStrategyConditions(const std::string& conditions, PriorityDescription& priorityDesc,
                                  MergeTriggerDescription& mergeTriggerDesc);

    void ExtractMergeSegmentsForOnePlan(index_base::SegmentMergeInfos& needMergeSegments,
                                        index_base::SegmentMergeInfos& inPlanSegments, uint64_t curTotalMergeDocCount,
                                        uint64_t curTotalMergedSize);

    bool NeedSkipCurrentSegmentByDocCount(uint64_t curTotalMergeDocCount, uint64_t inPlanMergeDocCount,
                                          uint32_t curSegmentValidDocCount);

    bool NeedSkipCurrentSegmentBySize(uint64_t curTotalMergedSize, uint64_t inPlanMergedSize,
                                      uint64_t curSegmentValidSize);

    bool LargerThanDelPercent(const index_base::SegmentMergeInfo& segMergeInfo);
    QueueItem MakeQueueItem(const index_base::SegmentMergeInfo& mergeInfo);
    uint32_t GetMergedDocCount(const index_base::SegmentMergeInfo& info);
    uint64_t GetValidSegmentSize(const index_base::SegmentMergeInfo& info);
    void UpdateMergedInfo(const index_base::SegmentMergeInfos& mergeInfos, uint64_t& mergedDocCount,
                          uint64_t& mergedSize);
    bool IsPlanValid(const index_base::SegmentMergeInfos& planSegments);
    MergePlan ConstructMergePlan(index_base::SegmentMergeInfos& mergeInfos);

private:
    InputLimits mInputLimits;
    PriorityDescription mPriorityDesc;
    MergeTriggerDescription mMergeTriggerDesc;
    OutputLimits mOutputLimits;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PriorityQueueMergeStrategy);
}} // namespace indexlib::merger

#endif //__INDEXLIB_PRIORITY_QUEUE_MERGE_STRATEGY_H
