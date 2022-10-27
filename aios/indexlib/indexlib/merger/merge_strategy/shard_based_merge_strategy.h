#ifndef __INDEXLIB_SHARD_BASED_MERGE_STRATEGY_H
#define __INDEXLIB_SHARD_BASED_MERGE_STRATEGY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/merger/merge_strategy/merge_strategy.h"
#include "indexlib/merger/merge_strategy/merge_strategy_creator.h"

IE_NAMESPACE_BEGIN(merger);

class ShardBasedMergeStrategy : public MergeStrategy
{
public:
    ShardBasedMergeStrategy(const merger::SegmentDirectoryPtr& segDir,
                            const config::IndexPartitionSchemaPtr& schema);
    ~ShardBasedMergeStrategy();

    DECLARE_MERGE_STRATEGY_CREATOR(ShardBasedMergeStrategy, SHARD_BASED_MERGE_STRATEGY_STR);

public:
    void SetParameter(const config::MergeStrategyParameter& param) override;
    std::string GetParameter() const override;
    MergeTask CreateMergeTask(const index_base::SegmentMergeInfos& segMergeInfos,
                              const index_base::LevelInfo& levelInfo) override;
    MergeTask CreateMergeTaskForOptimize(
            const index_base::SegmentMergeInfos& segMergeInfos,
            const index_base::LevelInfo& levelInfo) override;
private:
    std::vector<std::vector<bool>> GenerateMergeTag(
            const index_base::LevelInfo& levelInfo,
            const std::map<segmentid_t, index_base::SegmentMergeInfo>& segMergeInfoMap);
    
    MergeTask GenerateMergeTask(
            const index_base::LevelInfo& levelInfo,
            const std::map<segmentid_t, index_base::SegmentMergeInfo>& segMergeInfoMap,
            const std::vector<std::vector<bool>>& mergeTag);
    uint32_t CalculateLevelPercent(double spaceAmplification, uint32_t levelNum) const;
    double CalculateSpaceAmplification(uint32_t percent, uint32_t levelNum) const;
    void GetLevelSizeInfo(const index_base::LevelInfo& levelInfo,
                          const std::map<segmentid_t, index_base::SegmentMergeInfo>& segMergeInfoMap,
                          std::vector<std::vector<size_t>>& segmentsSize,
                          std::vector<size_t>& actualLevelSize);
    void GetLevelThreshold(uint32_t levelNum, size_t bottomLevelSize,
                           std::vector<size_t>& levelsThreshold);
private:
    double mSpaceAmplification;
private:
    friend class ShardBasedMergeStrategyTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ShardBasedMergeStrategy);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_SHARD_BASED_MERGE_STRATEGY_H
