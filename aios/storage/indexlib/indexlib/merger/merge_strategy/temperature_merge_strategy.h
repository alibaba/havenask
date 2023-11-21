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

#include <stdint.h>
#include <string>
#include <vector>

#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/merge_strategy_parameter.h"
#include "indexlib/framework/LevelInfo.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/index_define.h"
#include "indexlib/merger/merge_strategy/merge_strategy.h"
#include "indexlib/merger/merge_strategy/merge_strategy_creator.h"
#include "indexlib/merger/merge_strategy/priority_queue_merge_strategy_define.h"
#include "indexlib/merger/merge_task.h"
#include "indexlib/merger/segment_directory.h"
#include "indexlib/misc/common.h"
#include "indexlib/misc/log.h"

namespace indexlib { namespace merger {

class TemperatureMergeStrategy : public MergeStrategy
{
public:
    TemperatureMergeStrategy(const merger::SegmentDirectoryPtr& segDir = SegmentDirectoryPtr(),
                             const config::IndexPartitionSchemaPtr& schema = config::IndexPartitionSchemaPtr())
        : MergeStrategy(segDir, schema)
        , mMaxHotSegmentSize(DEFAULT_MAX_HOT_SEGMENT_SIZE)
        , mMaxWarmSegmentSize(DEFAULT_MAX_WARM_SEGMENT_SIZE)
        , mMaxColdSegmentSize(DEFAULT_MAX_COLD_SEGMENT_SIZE)
    {
    }
    ~TemperatureMergeStrategy();

    DECLARE_MERGE_STRATEGY_CREATOR(TemperatureMergeStrategy, TEMPERATURE_MERGE_STRATEGY_STR);

public:
    void SetParameter(const config::MergeStrategyParameter& param) override;
    std::string GetParameter() const override;
    MergeTask CreateMergeTask(const index_base::SegmentMergeInfos& segMergeInfos,
                              const indexlibv2::framework::LevelInfo& levelInfo) override;
    MergeTask CreateMergeTaskForOptimize(const index_base::SegmentMergeInfos& segMergeInfos,
                                         const indexlibv2::framework::LevelInfo& levelInfo) override;

private:
    void AnalyseCondition(const std::string& condition);
    void AnalyseOuputLimit(const std::string& outputLimit);
    void AnalyseLifeCycleDetail(const std::string& detail, int64_t& totalDocCount, int64_t& hotDocCount,
                                int64_t& warmDocCount, int64_t& coldDocCount);

    void GetValidSegmentSize(const index_base::SegmentMergeInfo& info, int64_t& hotSegmentSize,
                             int64_t& warmSegmentSize, int64_t& coldSegmentSize);
    MergeTask GenerateMergeTask(std::vector<PriorityQueue>& queues);
    index::TemperatureProperty GetSegmentTemperature(const index_base::SegmentMergeInfo& mergeInfo);
    void PutSegMergeInfosToQueue(const index_base::SegmentMergeInfos& segMergeInfos,
                                 std::vector<PriorityQueue>& priorityQueues);
    int32_t FindQueueIndex(const std::string& temperature);
    int64_t CaculatorTemperatureDiff(const index_base::SegmentMergeInfo& mergeInfo);
    QueueItem MakeQueueItem(const index_base::SegmentMergeInfo& mergeInfo);

private:
    static const int64_t DEFAULT_MAX_HOT_SEGMENT_SIZE;
    static const int64_t DEFAULT_MAX_WARM_SEGMENT_SIZE;
    static const int64_t DEFAULT_MAX_COLD_SEGMENT_SIZE;
    static const int64_t SIZE_MB;

private:
    int64_t mMaxHotSegmentSize;
    int64_t mMaxWarmSegmentSize;
    int64_t mMaxColdSegmentSize;
    PriorityDescription mPriority;
    std::vector<std::string> mSeq;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TemperatureMergeStrategy);
}} // namespace indexlib::merger
