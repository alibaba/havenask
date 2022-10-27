#ifndef __INDEXLIB_OPTIMIZE_MERGE_STRATEGY_CREATOR_H
#define __INDEXLIB_OPTIMIZE_MERGE_STRATEGY_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/merger/merge_strategy/merge_strategy_creator.h"

IE_NAMESPACE_BEGIN(merger);

class OptimizeMergeStrategyCreator : public MergeStrategyCreator
{
public:
    OptimizeMergeStrategyCreator();
    ~OptimizeMergeStrategyCreator();

public:
    std::string GetIdentifier() const override;
    MergeStrategyPtr Create(
            const SegmentDirectoryPtr &segDir,
            const config::IndexPartitionSchemaPtr &schema) const override;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OptimizeMergeStrategyCreator);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_OPTIMIZE_MERGE_STRATEGY_CREATOR_H
