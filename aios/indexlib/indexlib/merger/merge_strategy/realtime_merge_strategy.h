#ifndef __INDEXLIB_REALTIME_MERGE_STRATEGY_H
#define __INDEXLIB_REALTIME_MERGE_STRATEGY_H

#include <tr1/memory>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/index_define.h"
#include "indexlib/merger/merge_strategy/merge_strategy.h"
#include "indexlib/merger/merge_strategy/merge_strategy_factory.h"

IE_NAMESPACE_BEGIN(merger);

class RealtimeMergeStrategy : public MergeStrategy
{
public:
    static const std::string MAX_SMALL_SEGMENT_COUNT;
    static const std::string DO_MERGE_SIZE_THRESHOLD;
    static const std::string DONT_MERGE_SIZE_THRESHOLD;

public:
    RealtimeMergeStrategy(
        const merger::SegmentDirectoryPtr &segDir = merger::SegmentDirectoryPtr(),
        const config::IndexPartitionSchemaPtr &schema = config::IndexPartitionSchemaPtr());
    ~RealtimeMergeStrategy();

    DECLARE_MERGE_STRATEGY_CREATOR(RealtimeMergeStrategy, REALTIME_MERGE_STRATEGY_STR);

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
    int32_t mMaxSmallSegmentCount;
    int64_t mDoMergeSizeThreshold;
    int64_t mDontMergeSizeThreshold;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(RealtimeMergeStrategy);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_REALTIME_MERGE_STRATEGY_H
