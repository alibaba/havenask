#ifndef __INDEXLIB_DEFAULT_SPLIT_STRATEGY_H
#define __INDEXLIB_DEFAULT_SPLIT_STRATEGY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/merger/split_strategy/split_segment_strategy.h"

IE_NAMESPACE_BEGIN(merger);

class DefaultRangeSplitStrategy : public SplitSegmentStrategy
{
public:
    DefaultRangeSplitStrategy(merger::SegmentDirectoryPtr segDir,
        config::IndexPartitionSchemaPtr schema,
        index::OfflineAttributeSegmentReaderContainerPtr attrReaders,
        const index_base::SegmentMergeInfos& segMergeInfos, const MergePlan& plan);

    ~DefaultRangeSplitStrategy();

public:
    bool Init(const util::KeyValueMap& parameters) override { return true; }
    segmentindex_t DoProcess(segmentid_t oldSegId, docid_t oldLocalId) override
    {
        return 0;
    }

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DefaultRangeSplitStrategy);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_DEFAULT_SPLIT_STRATEGY_H
