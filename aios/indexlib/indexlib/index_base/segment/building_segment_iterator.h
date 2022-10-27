#ifndef __INDEXLIB_BUILDING_SEGMENT_ITERATOR_H
#define __INDEXLIB_BUILDING_SEGMENT_ITERATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index_base/segment/segment_iterator.h"

IE_NAMESPACE_BEGIN(index_base);

class BuildingSegmentIterator : public SegmentIterator
{
public:
    BuildingSegmentIterator();
    ~BuildingSegmentIterator();
    
public:
    void Init(const std::vector<InMemorySegmentPtr>& inMemSegments,
              exdocid_t baseDocId);
            
    bool IsValid() const override;
     
    void MoveToNext() override;
    
    InMemorySegmentPtr GetInMemSegment() const override;
    
    SegmentData GetSegmentData() const override;
    
    exdocid_t GetBaseDocId() const override;
    
    segmentid_t GetSegmentId() const override;
    
    SegmentIteratorType GetType() const override;
  
    void Reset() override;
    
private:
    std::vector<InMemorySegmentPtr> mInMemSegments;
    exdocid_t mCurrentBaseDocId;
    size_t mCursor;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BuildingSegmentIterator);

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_BUILDING_SEGMENT_ITERATOR_H
