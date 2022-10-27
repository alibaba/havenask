#ifndef __INDEXLIB_BUILT_SEGMENT_ITERATOR_H
#define __INDEXLIB_BUILT_SEGMENT_ITERATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index_base/segment/segment_iterator.h"

IE_NAMESPACE_BEGIN(index_base);

class BuiltSegmentIterator : public SegmentIterator
{
public:
    BuiltSegmentIterator();
    ~BuiltSegmentIterator();
public:
    void Init(const std::vector<SegmentData>& segmentDatas);
    bool IsValid() const override;
    
    void MoveToNext() override;

    InMemorySegmentPtr GetInMemSegment() const override;
    
    SegmentData GetSegmentData() const override;
       
    exdocid_t GetBaseDocId() const override;
    
    segmentid_t GetSegmentId() const override;
    
    SegmentIteratorType GetType() const override;

    void Reset() override;
    
private:
    std::vector<SegmentData> mSegmentDatas;
    exdocid_t mCurrentBaseDocId;
    size_t mCursor;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BuiltSegmentIterator);

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_BUILT_SEGMENT_ITERATOR_H
