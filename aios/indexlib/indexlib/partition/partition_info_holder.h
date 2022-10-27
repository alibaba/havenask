#ifndef __INDEXLIB_PARTITION_INFO_HOLDER_H
#define __INDEXLIB_PARTITION_INFO_HOLDER_H

#include <tr1/memory>
#include <autil/Lock.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(index, DeletionMapReader);
DECLARE_REFERENCE_CLASS(index_base, InMemorySegment);
DECLARE_REFERENCE_CLASS(index_base, SegmentData);
DECLARE_REFERENCE_CLASS(index_base, Version);
DECLARE_REFERENCE_CLASS(index_base, PartitionMeta);
DECLARE_REFERENCE_CLASS(index, PartitionInfo);

IE_NAMESPACE_BEGIN(partition);

class PartitionInfoHolder;
DEFINE_SHARED_PTR(PartitionInfoHolder);

class PartitionInfoHolder
{
public:
    PartitionInfoHolder();
    ~PartitionInfoHolder();
public:
    void Init(const index_base::Version& version, 
              const index_base::PartitionMeta& partitionMeta,
              const std::vector<index_base::SegmentData>& segmentDatas,
              const std::vector<index_base::InMemorySegmentPtr>& dumpingSegments,
              const index::DeletionMapReaderPtr& deletionMapReader);

    void SetSubPartitionInfoHolder(
            const PartitionInfoHolderPtr& subPartitionInfoHolder);

    PartitionInfoHolderPtr GetSubPartitionInfoHolder() const
    { return mSubPartitionInfoHolder; }

    void AddInMemorySegment(const index_base::InMemorySegmentPtr& inMemSegment);

    void UpdatePartitionInfo(const index_base::InMemorySegmentPtr& inMemSegment);

    index::PartitionInfoPtr GetPartitionInfo() const;
    void SetPartitionInfo(const index::PartitionInfoPtr& partitionInfo);

private:
    mutable autil::ReadWriteLock mPartitionInfoLock;
    index::PartitionInfoPtr mPartitionInfo;
    PartitionInfoHolderPtr mSubPartitionInfoHolder;

private:
    IE_LOG_DECLARE();
};

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_PARTITION_INFO_HOLDER_H
