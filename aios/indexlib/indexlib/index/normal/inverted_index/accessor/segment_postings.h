#ifndef __INDEXLIB_SEGMENT_POSTINGS_H
#define __INDEXLIB_SEGMENT_POSTINGS_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/segment_posting.h"

IE_NAMESPACE_BEGIN(index);

class SegmentPostings
{
public:
    SegmentPostings()
        : mBaseDocId(0)
        , mDocCount(0)
    {}
    ~SegmentPostings() {}
    typedef std::vector<SegmentPosting> SegmentPostingVec;
public:
    void AddSegmentPosting(const SegmentPosting& segmentPosting)
    {
        mSegmentPostings.push_back(segmentPosting);
        mBaseDocId = segmentPosting.GetBaseDocId();
        mDocCount = segmentPosting.GetDocCount();
    }
    SegmentPostingVector& GetSegmentPostings()
    {
        return mSegmentPostings;
    }

    docid_t GetBaseDocId() const { return mBaseDocId; }
    size_t GetDocCount() const { return mDocCount; }
public:
    SegmentPostingVec mSegmentPostings;
    docid_t mBaseDocId;
    uint32_t mDocCount;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SegmentPostings);
//TODO: optimize this
typedef std::vector<SegmentPostingsPtr> SegmentPostingsVec;

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SEGMENT_POSTINGS_H
