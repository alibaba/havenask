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
#include <queue>
#include <vector>

#include "autil/Log.h"
#include "indexlib/config/MergeStrategyParameter.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/framework/index_task/IndexTaskContext.h"
#include "indexlib/table/index_task/merger/MergePlan.h"
#include "indexlib/table/index_task/merger/MergeStrategy.h"
#include "indexlib/table/index_task/merger/MergeStrategyDefine.h"

namespace indexlibv2::table {

struct BalanceTreeMergeParams {
    static constexpr uint32_t DEFAULT_CONFLICT_SEGMENT_NUM = 2;
    static constexpr uint32_t DEFAULT_CONFLICT_DEL_PERCENT = 70;
    static constexpr uint32_t DEFAULT_BASE_DOC_COUNT = std::numeric_limits<uint32_t>::max();
    static constexpr uint64_t DEFAULT_MAX_DOC_COUNT = std::numeric_limits<uint64_t>::max();
    static constexpr uint64_t DEFAULT_MAX_VALID_DOC_COUNT = std::numeric_limits<uint64_t>::max();

    uint32_t baseDocCount = DEFAULT_BASE_DOC_COUNT;
    uint32_t conflictSegmentNum = DEFAULT_CONFLICT_SEGMENT_NUM;
    uint32_t conflictDelPercent = DEFAULT_CONFLICT_DEL_PERCENT;
    uint64_t maxDocCount = DEFAULT_MAX_DOC_COUNT;
    uint64_t maxValidDocCount = DEFAULT_MAX_VALID_DOC_COUNT;
    std::string DebugString() const
    {
        std::stringstream ss;
        ss << "baseDocCount[" << baseDocCount << "],"
           << "maxDocCount[" << maxDocCount << "],"
           << "conflictSegmentNum[" << conflictSegmentNum << "],"
           << "conflictDelPercent[" << conflictDelPercent << "],"
           << "maxValidDocCount[" << maxValidDocCount << "]";
        return ss.str();
    }
};

class BalanceTreeMergeStrategy : public MergeStrategy
{
public:
    BalanceTreeMergeStrategy() = default;
    ~BalanceTreeMergeStrategy() = default;

    struct SegmentMergeInfo {
        segmentid_t segmentId = INVALID_SEGMENTID;
        uint64_t docCount = 0;
        uint64_t deletedDocCount = 0;
        SegmentMergeInfo(segmentid_t id, uint64_t docCnt, uint64_t delDocCnt)
            : segmentId(id)
            , docCount(docCnt)
            , deletedDocCount(delDocCnt)
        {
        }
        SegmentMergeInfo() = default;
    };

    struct SegmentIdComparator {
    public:
        bool operator()(const SegmentMergeInfo& left, const SegmentMergeInfo& right)
        {
            return left.segmentId > right.segmentId;
        }
    };
    using SegmentQueue = std::priority_queue<SegmentMergeInfo, std::vector<SegmentMergeInfo>, SegmentIdComparator>;

public:
    std::string GetName() const override { return MergeStrategyDefine::BALANCE_TREE_MERGE_STRATEGY_NAME; }
    std::pair<Status, std::shared_ptr<MergePlan>> DoCreateMergePlan(const framework::IndexTaskContext* context);
    std::pair<Status, std::shared_ptr<MergePlan>> CreateMergePlan(const framework::IndexTaskContext* context) override;

private:
    std::pair<Status, BalanceTreeMergeParams> ExtractParams(const config::MergeStrategyParameter& param) const;
    Status CheckParam(const framework::IndexTaskContext* context);
    void InitBalanceTree(const framework::IndexTaskContext* context, std::vector<SegmentMergeInfo>& segmentMergeInfos,
                         std::vector<SegmentQueue>& balanceTree) const;

    std::vector<SegmentMergeInfo>
    CollectSegmentMergeInfos(const std::shared_ptr<framework::TabletData>& tabletData) const;
    int32_t GetLayer(uint64_t docCount) const;
    [[maybe_unused]] int32_t TryInsertBalanceTree(const SegmentMergeInfo& segmentMergeInfo,
                                                  std::vector<SegmentQueue>& balanceTree) const;
    bool HasLargeDelPercentSegment(SegmentQueue& segQueue) const;
    bool LargerThanDelPercent(const SegmentMergeInfo& segMergeInfo) const;
    void CreateTopLayerMergePlan(const std::shared_ptr<MergePlan>& mergePlan,
                                 const std::vector<SegmentMergeInfo>& segmentMergeInfos) const;
    int32_t TrySetNextTreeLevel(int32_t mergedLayerId, uint32_t currentId, int32_t& minLayerId) const;
    void TryAddSegmentMergePlan(const std::shared_ptr<MergePlan>& mergePlan, const SegmentMergePlan& segmentMergePlan,
                                bool isConflictDelPercent) const;

private:
    std::pair<Status, BalanceTreeMergeParams> TEST_ExtractParams(const std::string& paramStr);

private:
    BalanceTreeMergeParams _params;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
