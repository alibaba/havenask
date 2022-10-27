#ifndef __INDEXLIB_SEGMENT_MAPPER_H
#define __INDEXLIB_SEGMENT_MAPPER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include <algorithm>

IE_NAMESPACE_BEGIN(index_base);

class SegmentMapper
{
public:
    SegmentMapper()
        : mNeedId2SegIdx(true)
        , mMaxSegIndex(0)
    {
    }

public:
    void Collect(size_t id, segmentindex_t index) {
        if (mNeedId2SegIdx)
        {
            assert(id < mData.size());
            mData[id] = index;
        }
        ++mSegDocCounts[index];
        mMaxSegIndex = std::max(mMaxSegIndex, index);
    }
    segmentindex_t GetMaxSegmentIndex() const { return mMaxSegIndex; }
    segmentindex_t GetSegmentIndex(size_t id) const
    {
        assert(mNeedId2SegIdx);
        if (Empty())
        {
            return 0;
        }
        assert(id < mData.size());
        return mData[id];
    }
    size_t GetSegmentDocCount(segmentindex_t segIdx) const
    {
        assert(segIdx < mSegDocCounts.size());
        return mSegDocCounts[segIdx];
    }
    void Init(size_t size, bool needId2SegIdx = true)
    {
        mNeedId2SegIdx = needId2SegIdx;
        if (needId2SegIdx)
        {
            mData.resize(size, 0);            
        }
        mSegDocCounts.resize(MAX_SPLIT_SEGMENT_COUNT, 0);
    }
    void Clear()
    {
        mData.clear();
        mSegDocCounts.clear();

        mData.shrink_to_fit();
        mSegDocCounts.shrink_to_fit();
    }
    bool Empty() const { return mData.empty(); }

private:
    bool mNeedId2SegIdx;
    segmentindex_t mMaxSegIndex;
    std::vector<segmentindex_t> mData;
    std::vector<uint32_t> mSegDocCounts;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SegmentMapper);

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_SEGMENT_MAPPER_H
