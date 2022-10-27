#ifndef __INDEXLIB_SPECIFIC_SEGMENTS_MERGE_STRATEGY_H
#define __INDEXLIB_SPECIFIC_SEGMENTS_MERGE_STRATEGY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index_define.h"
#include "indexlib/merger/merge_strategy/merge_strategy.h"
#include "indexlib/merger/merge_strategy/merge_strategy_creator.h"

IE_NAMESPACE_BEGIN(merger);

// for test
class SpecificSegmentsMergeStrategy : public MergeStrategy
{
public:
    SpecificSegmentsMergeStrategy(const merger::SegmentDirectoryPtr &segDir,
                                  const config::IndexPartitionSchemaPtr &schema);

    ~SpecificSegmentsMergeStrategy();

    DECLARE_MERGE_STRATEGY_CREATOR(SpecificSegmentsMergeStrategy, 
                                   SPECIFIC_SEGMENTS_MERGE_STRATEGY_STR);

public:
    
    void SetParameter(const config::MergeStrategyParameter& param) override;
    
    std::string GetParameter() const override;
    
    MergeTask CreateMergeTask(const index_base::SegmentMergeInfos& segMergeInfos,
                              const index_base::LevelInfo& levelInfo) override;
    
    MergeTask CreateMergeTaskForOptimize(
            const index_base::SegmentMergeInfos& segMergeInfos,
            const index_base::LevelInfo& levelInfo) override;

private:
    bool GetSegmentMergeInfo(segmentid_t segmentId,
                             const index_base::SegmentMergeInfos& segMergeInfos,
                             index_base::SegmentMergeInfo& segMergeInfo);

private:
    std::vector<std::vector<segmentid_t> > mTargetSegmentIds;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SpecificSegmentsMergeStrategy);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_SPECIFIC_SEGMENTS_MERGE_STRATEGY_H
