#ifndef __INDEXLIB_PARTITION_SEGMENT_ITERATOR_H
#define __INDEXLIB_PARTITION_SEGMENT_ITERATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index_base/segment/segment_iterator.h"

IE_NAMESPACE_BEGIN(index_base);

class PartitionSegmentIterator : public SegmentIterator
{
public:
    PartitionSegmentIterator(bool isOnline = true);
    ~PartitionSegmentIterator();
    
public:
    void Init(const std::vector<SegmentData>& onDiskSegments,
              const std::vector<InMemorySegmentPtr>& dumpingInMemSegments,
              const InMemorySegmentPtr& currentBuildingSegment,
              segmentid_t nextBuildingSegmentId = INVALID_SEGMENTID);

    bool IsValid() const override
    { return mCurrentIter->IsValid(); }
    
    void MoveToNext() override;
    void Reset() override;

    InMemorySegmentPtr GetInMemSegment() const override
    { return mCurrentIter->GetInMemSegment(); }
    
    SegmentData GetSegmentData() const override;
    
    exdocid_t GetBaseDocId() const override
    { return mCurrentIter->GetBaseDocId(); }
    
    segmentid_t GetSegmentId() const override
    { return mCurrentIter->GetSegmentId(); }
    
    SegmentIteratorType GetType() const override
    { return mCurrentIter->GetType(); }

    PartitionSegmentIterator* CreateSubPartitionSegmentItertor() const;
    
public:
    SegmentIteratorPtr CreateIterator(SegmentIteratorType type);
    bool GetLastSegmentData(SegmentData& segData) const;
    segmentid_t GetLastBuiltSegmentId() const;

    exdocid_t GetBuildingBaseDocId() const { return mBuildingBaseDocId; }
    
    size_t GetSegmentCount() const;

    segmentid_t GetNextBuildingSegmentId() const { return mNextBuildingSegmentId; }
    segmentid_t GetBuildingSegmentId() const { return mBuildingSegmentId; }
    
private:
    void InitNextSegmentId(const std::vector<SegmentData>& onDiskSegments,
                           const std::vector<InMemorySegmentPtr>& dumpingInMemSegments,
                           const InMemorySegmentPtr& currentBuildingSegment);

private:
    std::vector<SegmentData> mOnDiskSegments;
    std::vector<InMemorySegmentPtr> mBuildingSegments;
    exdocid_t mBuildingBaseDocId;
    SegmentIteratorPtr mCurrentIter;
    segmentid_t mNextBuildingSegmentId;
    segmentid_t mBuildingSegmentId;
    bool mIsOnline;
    
private:
    friend class BuildingPartitionDataTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PartitionSegmentIterator);

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_PARTITION_SEGMENT_ITERATOR_H
