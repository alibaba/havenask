#ifndef __INDEXLIB_TEST_SPLIT_STRATEGY_H
#define __INDEXLIB_TEST_SPLIT_STRATEGY_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/merger/split_strategy/split_segment_strategy.h"

namespace indexlib { namespace merger {

class TestSplitStrategy : public SplitSegmentStrategy
{
public:
    TestSplitStrategy(index::SegmentDirectoryBasePtr segDir, config::IndexPartitionSchemaPtr schema,
                      index::OfflineAttributeSegmentReaderContainerPtr attrReaders,
                      const index_base::SegmentMergeInfos& segMergeInfos, const MergePlan& plan,
                      std::map<segmentid_t, index::SegmentMetricsUpdaterPtr> hintDocInfos,
                      const util::MetricProviderPtr& metrics)
        : SplitSegmentStrategy(segDir, schema, attrReaders, segMergeInfos, plan, hintDocInfos, metrics)
        , mSegmentCount(1)
        , mDocCount(0)
        , mUseInvaildDoc(false)
    {
    }
    ~TestSplitStrategy() {}

public:
    static const std::string STRATEGY_NAME;

public:
    bool Init(const util::KeyValueMap& parameters) override;

    segmentindex_t DoProcess(segmentid_t oldSegId, docid_t oldLocalId, int64_t& hintValue) override
    {
        if (mUseInvaildDoc) {
            return mDocCount++ % mSegmentCount;
        }
        return oldLocalId % mSegmentCount;
    }

private:
    size_t mSegmentCount;
    size_t mDocCount;
    bool mUseInvaildDoc;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TestSplitStrategy);
}} // namespace indexlib::merger

#endif //__INDEXLIB_TEST_SPLIT_STRATEGY_H
