#ifndef __INDEXLIB_SIMPLE_SPLIT_STRATEGY_H
#define __INDEXLIB_SIMPLE_SPLIT_STRATEGY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/merger/split_strategy/split_segment_strategy.h"

IE_NAMESPACE_BEGIN(merger);

class SimpleSplitStrategy : public SplitSegmentStrategy
{
public:
    SimpleSplitStrategy(index::SegmentDirectoryBasePtr segDir,
        config::IndexPartitionSchemaPtr schema,
        index::OfflineAttributeSegmentReaderContainerPtr attrReaders,
        const index_base::SegmentMergeInfos& segMergeInfos, const MergePlan& plan)
        : SplitSegmentStrategy(segDir, schema, attrReaders, segMergeInfos, plan)
    {
    }

    ~SimpleSplitStrategy() {}

public:
    static const std::string STRATEGY_NAME;

public:
    bool Init(const util::KeyValueMap& parameters) override { return true; }
    segmentindex_t DoProcess(segmentid_t oldSegId, docid_t oldLocalId) override
    {
        return 0;
    }

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SimpleSplitStrategy);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_SIMPLE_SPLIT_STRATEGY_H
