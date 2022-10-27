#include "indexlib/merger/merge_strategy/merge_strategy_factory.h"
#include "indexlib/merger/merge_strategy/optimize_merge_strategy.h"
#include "indexlib/merger/merge_strategy/balance_tree_merge_strategy.h"
#include "indexlib/merger/merge_strategy/realtime_merge_strategy.h"
#include "indexlib/merger/merge_strategy/specific_segments_merge_strategy.h"
#include "indexlib/merger/merge_strategy/priority_queue_merge_strategy.h"
#include "indexlib/merger/merge_strategy/optimize_merge_strategy_creator.h"
#include "indexlib/merger/merge_strategy/time_series_merge_strategy.h"
#include "indexlib/merger/merge_strategy/large_time_range_selection_merge_strategy.h"
#include "indexlib/merger/merge_strategy/shard_based_merge_strategy.h"
#include "indexlib/merger/merge_strategy/align_version_merge_strategy.h"
#include "indexlib/misc/exception.h"
#include <sstream>

using namespace std;
using namespace autil::legacy;

IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(merger);

MergeStrategyFactory::MergeStrategyFactory() 
{
    Init();
}

MergeStrategyFactory::~MergeStrategyFactory() 
{
}

void MergeStrategyFactory::Init()
{
    RegisterCreator(MergeStrategyCreatorPtr(new OptimizeMergeStrategyCreator()));
    RegisterCreator(MergeStrategyCreatorPtr(new BalanceTreeMergeStrategy::Creator()));
    RegisterCreator(MergeStrategyCreatorPtr(new RealtimeMergeStrategy::Creator()));
    RegisterCreator(MergeStrategyCreatorPtr(new PriorityQueueMergeStrategy::Creator()));
    RegisterCreator(MergeStrategyCreatorPtr(new ShardBasedMergeStrategy::Creator()));
    RegisterCreator(MergeStrategyCreatorPtr(new AlignVersionMergeStrategy::Creator()));
    RegisterCreator(MergeStrategyCreatorPtr(new TimeSeriesMergeStrategy::Creator()));
    RegisterCreator(MergeStrategyCreatorPtr(new LargeTimeRangeSelectionMergeStrategy::Creator()));

    // for test purpose
    RegisterCreator(MergeStrategyCreatorPtr(new SpecificSegmentsMergeStrategy::Creator()));
}

void MergeStrategyFactory::RegisterCreator(MergeStrategyCreatorPtr creator)
{
    assert (creator != NULL);

    autil::ScopedLock l(mLock);
    string identifier = creator->GetIdentifier();
    CreatorMap::iterator it = mCreatorMap.find(identifier);
    if(it != mCreatorMap.end())
    {
        mCreatorMap.erase(it);
    }
    mCreatorMap.insert(std::make_pair(identifier, creator));
}

MergeStrategyPtr MergeStrategyFactory::CreateMergeStrategy(
        const string& identifier,
        const SegmentDirectoryPtr &segDir,
        const IndexPartitionSchemaPtr &schema)
{
    autil::ScopedLock l(mLock);
    CreatorMap::const_iterator it = mCreatorMap.find(identifier);
    if (it != mCreatorMap.end())
    {
        return it->second->Create(segDir, schema);
    }
    else
    {
        stringstream s;
        s << "Merge strategy identifier " << identifier <<" not implemented yet!";
        INDEXLIB_THROW(misc::UnImplementException, "%s", s.str().c_str());
    }
    return MergeStrategyPtr();
}

IE_NAMESPACE_END(merger);

