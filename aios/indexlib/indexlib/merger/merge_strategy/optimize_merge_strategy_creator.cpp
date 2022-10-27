#include "indexlib/merger/merge_strategy/optimize_merge_strategy_creator.h"
#include "indexlib/merger/merge_strategy/optimize_merge_strategy.h"

using namespace std;
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, OptimizeMergeStrategyCreator);

OptimizeMergeStrategyCreator::OptimizeMergeStrategyCreator()
{
}

OptimizeMergeStrategyCreator::~OptimizeMergeStrategyCreator()
{
}

string OptimizeMergeStrategyCreator::GetIdentifier() const
{
    return OPTIMIZE_MERGE_STRATEGY_STR;
}

MergeStrategyPtr OptimizeMergeStrategyCreator::Create(
            const SegmentDirectoryPtr &segDir,
            const IndexPartitionSchemaPtr &schema) const
{
    return MergeStrategyPtr(new OptimizeMergeStrategy(segDir, schema));
}

IE_NAMESPACE_END(merger);
