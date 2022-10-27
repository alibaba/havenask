#ifndef __INDEXLIB_OPTIMIZE_MERGE_STRATEGY_H
 #define __INDEXLIB_OPTIMIZE_MERGE_STRATEGY_H

#include <tr1/memory>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include "indexlib/index_define.h"
#include "indexlib/merger/merge_strategy/merge_strategy.h"
#include "indexlib/merger/merge_strategy/merge_strategy_creator.h"
#include "indexlib/merger/multi_part_segment_directory.h"

IE_NAMESPACE_BEGIN(merger);

class OptimizeMergeStrategy : public MergeStrategy
{
public:
    OptimizeMergeStrategy(const SegmentDirectoryPtr &segDir,
                          const config::IndexPartitionSchemaPtr &schema);
    ~OptimizeMergeStrategy();

public:
    std::string GetIdentifier() const override
    { return OPTIMIZE_MERGE_STRATEGY_STR; }
    
    void SetParameter(const config::MergeStrategyParameter& param) override;
    
    std::string GetParameter() const override;
    
    MergeTask CreateMergeTask(const index_base::SegmentMergeInfos& segMergeInfos,
                              const index_base::LevelInfo& levelInfo) override;
    
    MergeTask CreateMergeTaskForOptimize(
            const index_base::SegmentMergeInfos& segMergeInfos,
            const index_base::LevelInfo& levelInfo) override;
    bool IsMergeToMultiSegments();

public:
    void SetParameter(const std::string& paramStr)
    {
        config::MergeStrategyParameter param;
        param.SetLegacyString(paramStr);
        SetParameter(param);
    }

private:
    index_base::SegmentMergeInfos FilterSegment(uint32_t maxDocCountInSegment, 
                                    const index_base::SegmentMergeInfos& segMergeInfos);

    MergeTask DoCreateMergeTask(const index_base::SegmentMergeInfos& segMergeInfos);

    uint64_t SplitDocCount(const index_base::SegmentMergeInfos& segMergeInfos) const;

    bool NeedMerge(const std::vector<MergePlan> &plans,
                   const index_base::SegmentMergeInfos& segMergeInfos);

    bool IsMergedSegment(segmentid_t segmentId,
                         const index_base::SegmentMergeInfos& segMergeInfos);
 
private:
    static const uint32_t DEFAULT_MAX_DOC_COUNT;
    static const uint64_t DEFAULT_AFTER_MERGE_MAX_DOC_COUNT;
    static const uint32_t INVALID_AFTER_MERGE_MAX_SEGMENT_COUNT;
    uint32_t mMaxDocCount;
    uint64_t mAfterMergeMaxDocCount;
    uint32_t mAfterMergeMaxSegmentCount;
    bool mSkipSingleMergedSegment;
private:
    friend class OptimizeMergeStrategyTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OptimizeMergeStrategy);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_OPTIMIZE_MERGE_STRATEGY_H
