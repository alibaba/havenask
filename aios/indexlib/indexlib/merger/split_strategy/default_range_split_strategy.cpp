#include "indexlib/merger/split_strategy/default_range_split_strategy.h"

using namespace std;
using namespace autil::legacy::json;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(merger);

IE_LOG_SETUP(merger, DefaultRangeSplitStrategy);

DefaultRangeSplitStrategy::DefaultRangeSplitStrategy(SegmentDirectoryPtr segDir,
    IndexPartitionSchemaPtr schema, OfflineAttributeSegmentReaderContainerPtr attrReaders,
    const SegmentMergeInfos& segMergeInfos, const MergePlan& plan)
    : SplitSegmentStrategy(segDir, schema, attrReaders, segMergeInfos, plan)
{
}

DefaultRangeSplitStrategy::~DefaultRangeSplitStrategy() {}

IE_NAMESPACE_END(merger);
