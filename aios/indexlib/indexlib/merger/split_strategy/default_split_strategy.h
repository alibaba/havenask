#ifndef __INDEXLIB_DEFAULT_SPLIT_STRATEGY_H
#define __INDEXLIB_DEFAULT_SPLIT_STRATEGY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/merger/split_strategy/split_segment_strategy.h"

IE_NAMESPACE_BEGIN(merger);

class DefaultSplitStrategy : public SplitSegmentStrategy
{
public:
    DefaultSplitStrategy(index::SegmentDirectoryBasePtr segDir,
        config::IndexPartitionSchemaPtr schema,
        index::OfflineAttributeSegmentReaderContainerPtr attrReaders,
        const index_base::SegmentMergeInfos& segMergeInfos, const MergePlan& plan);

    ~DefaultSplitStrategy();

public:
    bool Init(const util::KeyValueMap& parameters) override;
    segmentindex_t DoProcess(segmentid_t oldSegId, docid_t oldLocalId) override;

public:
    static const std::string STRATEGY_NAME;

private:
    size_t mTotalValidDocCount;
    size_t mSplitDocCount;
    size_t mCurrentDocCount;
    size_t mCurrentSegIndex;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DefaultSplitStrategy);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_DEFAULT_SPLIT_STRATEGY_H
