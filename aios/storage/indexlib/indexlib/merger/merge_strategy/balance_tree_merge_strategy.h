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
#ifndef __INDEXLIB_BALANCE_TREE_MERGE_STRATEGY_H
#define __INDEXLIB_BALANCE_TREE_MERGE_STRATEGY_H

#include <memory>
#include <queue>
#include <vector>

#include "indexlib/common_define.h"
#include "indexlib/index_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/merger/merge_strategy/merge_strategy.h"
#include "indexlib/merger/merge_strategy/merge_strategy_factory.h"

namespace indexlib { namespace merger {

class SegmentMergeInfoComparator
{
public:
    bool operator()(const index_base::SegmentMergeInfo& node1, const index_base::SegmentMergeInfo& node2)
    {
        return node1.segmentId > node2.segmentId;
    }
};

typedef std::priority_queue<index_base::SegmentMergeInfo, std::vector<index_base::SegmentMergeInfo>,
                            SegmentMergeInfoComparator>
    SegmentQueue;

class BalanceTreeMergeStrategy : public MergeStrategy
{
public:
    BalanceTreeMergeStrategy(const SegmentDirectoryPtr& segDir = SegmentDirectoryPtr(),
                             const config::IndexPartitionSchemaPtr& schema = config::IndexPartitionSchemaPtr());
    ~BalanceTreeMergeStrategy();

    DECLARE_MERGE_STRATEGY_CREATOR(BalanceTreeMergeStrategy, BALANCE_TREE_MERGE_STRATEGY_STR);

public:
    void SetParameter(const config::MergeStrategyParameter& param) override;

    std::string GetParameter() const override;

    MergeTask CreateMergeTask(const index_base::SegmentMergeInfos& segMergeInfos,
                              const indexlibv2::framework::LevelInfo& levelInfo) override;

    MergeTask CreateMergeTaskForOptimize(const index_base::SegmentMergeInfos& segMergeInfos,
                                         const indexlibv2::framework::LevelInfo& levelInfo) override;

public:
    void SetParameter(const std::string& paramStr)
    {
        config::MergeStrategyParameter param;
        param.SetLegacyString(paramStr);
        SetParameter(param);
    }

private:
    int32_t GetLayer(uint64_t docCount);
    bool HasLargeDelPercentSegment(SegmentQueue& segQueue);
    bool LargerThanDelPercent(const index_base::SegmentMergeInfo& segMergeInfo);
    void CreateTopLayerMergePlan(MergeTask& mergeTask, const index_base::SegmentMergeInfos& segMergeInfos);

private:
    uint32_t mBaseDocCount;
    uint32_t mMaxDocCount;
    uint32_t mConflictSegNum;
    uint32_t mConflictDelPercent;
    uint32_t mMaxValidDocCount;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BalanceTreeMergeStrategy);
}} // namespace indexlib::merger

#endif //__INDEXLIB_BALANCE_TREE_MERGE_STRATEGY_H
