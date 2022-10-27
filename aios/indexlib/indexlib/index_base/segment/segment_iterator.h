#ifndef __INDEXLIB_SEGMENT_ITERATOR_H
#define __INDEXLIB_SEGMENT_ITERATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(index_base, InMemorySegment);
DECLARE_REFERENCE_CLASS(index_base, SegmentData);

IE_NAMESPACE_BEGIN(index_base);

enum SegmentIteratorType
{
    SIT_BUILDING,
    SIT_BUILT,
};
    
class SegmentIterator
{
public:
    SegmentIterator() {}
    virtual ~SegmentIterator() {}
    
public:
    virtual bool IsValid() const = 0;
    virtual void MoveToNext() = 0;
    virtual InMemorySegmentPtr GetInMemSegment() const = 0;
    virtual SegmentData GetSegmentData() const = 0;
    virtual exdocid_t GetBaseDocId() const = 0;
    virtual segmentid_t GetSegmentId() const = 0;
    virtual SegmentIteratorType GetType() const = 0;
    virtual void Reset() = 0;
        
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SegmentIterator);

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_SEGMENT_ITERATOR_H
