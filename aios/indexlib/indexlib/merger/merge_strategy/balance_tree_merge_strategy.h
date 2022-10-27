#ifndef __INDEXLIB_BALANCE_TREE_MERGE_STRATEGY_H
#define __INDEXLIB_BALANCE_TREE_MERGE_STRATEGY_H

#include <vector>
#include <queue>
#include <tr1/memory>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include "indexlib/index_define.h"
#include "indexlib/merger/merge_strategy/merge_strategy.h"
#include "indexlib/merger/merge_strategy/merge_strategy_factory.h"

IE_NAMESPACE_BEGIN(merger);

class SegmentMergeInfoComparator
{
public:
    bool operator()(const index_base::SegmentMergeInfo& node1,
                    const index_base::SegmentMergeInfo& node2)
    {
        return node1.segmentId > node2.segmentId;
    }
};

typedef std::priority_queue<index_base::SegmentMergeInfo, std::vector<index_base::SegmentMergeInfo>, SegmentMergeInfoComparator> SegmentQueue;

class BalanceTreeMergeStrategy : public MergeStrategy
{
public:
    BalanceTreeMergeStrategy(
            const SegmentDirectoryPtr &segDir = SegmentDirectoryPtr(),
            const config::IndexPartitionSchemaPtr &schema = config::IndexPartitionSchemaPtr());
    ~BalanceTreeMergeStrategy();

    DECLARE_MERGE_STRATEGY_CREATOR(BalanceTreeMergeStrategy, BALANCE_TREE_MERGE_STRATEGY_STR);

public:
    
    void SetParameter(const config::MergeStrategyParameter& param) override;
    
    std::string GetParameter() const override;
    
    MergeTask CreateMergeTask(const index_base::SegmentMergeInfos& segMergeInfos,
                              const index_base::LevelInfo& levelInfo) override;
    
    MergeTask CreateMergeTaskForOptimize(const index_base::SegmentMergeInfos& segMergeInfos,
                                         const index_base::LevelInfo& levelInfo) override;

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
    void CreateTopLayerMergePlan(MergeTask& mergeTask,
                                 const index_base::SegmentMergeInfos& segMergeInfos);

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

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_BALANCE_TREE_MERGE_STRATEGY_H
