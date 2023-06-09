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

#include <memory>

#include "autil/Log.h"
#include "indexlib/config/MergeStrategyParameter.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/framework/index_task/IndexTaskContext.h"
#include "indexlib/index/IIndexMerger.h"
#include "indexlib/table/index_task/merger/MergePlan.h"
#include "indexlib/table/index_task/merger/MergeStrategy.h"
#include "indexlib/table/index_task/merger/MergeStrategyDefine.h"
#include "indexlib/table/normal_table/index_task/merger/NormalTabletMergeStrategyDefine.h"

namespace indexlibv2 { namespace table {

struct PriorityQueueMergeParams {
    // segment参与merge的前置限制条件，不配置默认为空（无前置限制条件，所有segment尽快参与merge）
    InputLimits inputLimits;
    // 生成mergePlan过程的后置限制条件，不配置默认为空（无后置限制条件，触发merge条件后的索引优先segment均参与merge）
    OutputLimits outputLimits;
    // merge策略的具体逻辑参数
    PriorityDescription priorityDesc;
    MergeTriggerDescription mergeTriggerDesc;
    std::string DebugString() const
    {
        std::stringstream ss;
        ss << "inputLimits[" << inputLimits.ToString() << "],"
           << "mergeTriggerDesc[" << mergeTriggerDesc.ToString() << "],"
           << "priorityDesc[" << priorityDesc.ToString() << "],"
           << "outputLimit[" << outputLimits.ToString() << "]";
        return ss.str();
    }
};

// 把segment按照配置中inputLimits进行选择过滤，之后按照priorityDesc的规则排序放入priority
// queue，之后按照优先级以及outputLimits输入需要进行merge的merge plan.
class PriorityQueueMergeStrategy : public MergeStrategy
{
public:
    PriorityQueueMergeStrategy() {}
    ~PriorityQueueMergeStrategy() {}

public:
    std::string GetName() const override { return MergeStrategyDefine::PRIORITY_QUEUE_MERGE_STRATEGY_NAME; }
    std::pair<Status, std::shared_ptr<MergePlan>> CreateMergePlan(const framework::IndexTaskContext* context) override;
    std::pair<Status, std::shared_ptr<MergePlan>> DoCreateMergePlan(const framework::IndexTaskContext* context);

public:
    static std::pair<Status, struct PriorityQueueMergeParams>
    ExtractParams(const config::MergeStrategyParameter& param);

    static bool CheckSegment(segmentid_t segmentId, uint32_t docCount, int64_t deleteDocCount, uint64_t segmentSize,
                             const InputLimits& inputLimits, const OutputLimits& outputLimits);

private:
    std::pair<Status, std::shared_ptr<MergePlan>>
    CreateMergePlanForOneGroup(const framework::IndexTaskContext* context,
                               const std::vector<segmentid_t>& candidateSegmentIds);
    std::priority_queue<QueueItem, std::vector<QueueItem>, QueueItemComparator>
    CreatePriorityQueueFromSrcSegments(const std::vector<std::shared_ptr<framework::Segment>>& srcSegments,
                                       const std::map<segmentid_t, uint64_t>& segmentSizeMap);
    QueueItem MakeQueueItem(std::shared_ptr<framework::Segment> srcSegment, uint32_t docCount, int64_t deleteDocCount);

    void FilterSegments(const std::map<segmentid_t, uint64_t>& segmentSizeMap,
                        std::priority_queue<QueueItem, std::vector<QueueItem>, QueueItemComparator>* queue,
                        std::vector<std::shared_ptr<framework::Segment>>* needMergeSegments);

    void ExtractMergeSegmentsForOnePlan(const uint64_t curTotalMergeDocCount, const uint64_t curTotalMergedSize,
                                        const std::map<segmentid_t, uint64_t>& segmentSizeMap,
                                        std::vector<std::shared_ptr<framework::Segment>>* needMergeSegments,
                                        std::vector<std::shared_ptr<framework::Segment>>* inPlanSegments);

    Status CalculateSegmentSizeMap(const std::vector<std::shared_ptr<framework::Segment>>& segments,
                                   std::map<segmentid_t, size_t>& segmentSizeMap);
    std::vector<std::shared_ptr<framework::Segment>>
    CollectSrcSegments(const std::shared_ptr<framework::TabletData>& tabletData);

    bool LargerThanDelPercent(const uint32_t docCount, const int64_t deleteDocCount);
    bool NeedSkipCurrentSegmentByDocCount(uint64_t curTotalMergeDocCount, uint64_t inPlanMergeDocCount,
                                          uint32_t curSegmentValidDocCount);
    bool NeedSkipCurrentSegmentBySize(uint64_t curTotalMergedSize, uint64_t inPlanMergedSize,
                                      uint64_t curSegmentValidSize);
    bool IsPlanValid(const std::vector<std::shared_ptr<framework::Segment>>& inPlanSegments);

private:
    static uint64_t GetValidSegmentSize(uint32_t docCount, int64_t deleteDocCount, uint64_t segmentSize);
    static uint32_t GetMergedDocCount(uint32_t docCount, int64_t deleteDocCount);

    static void UpdateMergeTotalStats(const std::vector<std::shared_ptr<framework::Segment>>& inPlanSegments,
                                      const std::map<segmentid_t, uint64_t>& segmentSizeMap, uint64_t* mergedDocCount,
                                      uint64_t* mergedSize);

    static bool DecodeStrategyConditions(const std::string& conditions, PriorityDescription* priorityDesc,
                                         MergeTriggerDescription* mergeTriggerDesc);

private:
    PriorityQueueMergeParams _params;

private:
    AUTIL_LOG_DECLARE();
};

}} // namespace indexlibv2::table
