#ifndef __INDEXLIB_ALIGN_VERSION_MERGE_STRATEGY_H
#define __INDEXLIB_ALIGN_VERSION_MERGE_STRATEGY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index_define.h"
#include "indexlib/merger/merge_strategy/merge_strategy.h"
#include "indexlib/merger/merge_strategy/merge_strategy_factory.h"

IE_NAMESPACE_BEGIN(merger);

class AlignVersionMergeStrategy : public MergeStrategy
{
public:
    AlignVersionMergeStrategy(
        const SegmentDirectoryPtr &segDir,
        const config::IndexPartitionSchemaPtr &schema);
    ~AlignVersionMergeStrategy();

    DECLARE_MERGE_STRATEGY_CREATOR(AlignVersionMergeStrategy,
                                   ALIGN_VERSION_MERGE_STRATEGY_STR);

public:
    void SetParameter(const config::MergeStrategyParameter& param) override;
    
    std::string GetParameter() const override;
    
    MergeTask CreateMergeTask(
        const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::LevelInfo& levelInfo) override;
    
    MergeTask CreateMergeTaskForOptimize(
        const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::LevelInfo& levelInfo) override;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AlignVersionMergeStrategy);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_ALIGN_VERSION_MERGE_STRATEGY_H
