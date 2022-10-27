#ifndef __INDEXLIB_DEMO_INDEX_REDUCER_H
#define __INDEXLIB_DEMO_INDEX_REDUCER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_reducer.h"

IE_NAMESPACE_BEGIN(index);

class DemoIndexReducer : public IndexReducer
{
public:
    DemoIndexReducer(const util::KeyValueMap& parameters)
        : IndexReducer(parameters)
    {}
    
    ~DemoIndexReducer() {}
public:
    IndexReduceItemPtr CreateReduceItem() override;

    bool DoInit(const IndexerResourcePtr& resource) override;

    bool Reduce(
            const std::vector<IndexReduceItemPtr>& reduceItems,
            const index_base::OutputSegmentMergeInfos& outputSegMergeInfos, // target segments
            bool isSortMerge,
            const ReduceResource& resource) override;

    int64_t EstimateMemoryUse(
            const std::vector<file_system::DirectoryPtr>& indexDirs,
            const index_base::SegmentMergeInfos& segmentInfos,
            const index_base::OutputSegmentMergeInfos& outputSegMergeInfos, // target segments            
            bool isSortMerge) const override;

private:
    util::StateCounterPtr mMergedItemCounter;
    typedef std::map<std::string, std::string> KVMap;
    KVMap mMeta;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DemoIndexReducer);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DEMO_INDEX_REDUCER_H
