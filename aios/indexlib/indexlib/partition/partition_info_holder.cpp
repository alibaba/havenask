#include "indexlib/partition/partition_info_holder.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, PartitionInfoHolder);

PartitionInfoHolder::PartitionInfoHolder() 
{
}

PartitionInfoHolder::~PartitionInfoHolder() 
{
}

void PartitionInfoHolder::Init(const Version& version, 
                               const PartitionMeta& partitionMeta,
                               const SegmentDataVector& segmentDatas,
                               const std::vector<InMemorySegmentPtr>& dumpingSegments,
                               const index::DeletionMapReaderPtr& deletionMapReader)
{
    mPartitionInfo.reset(new index::PartitionInfo);
    PartitionMetaPtr partitionMetaPtr(new PartitionMeta);
    *partitionMetaPtr = partitionMeta;
    mPartitionInfo->Init(version, partitionMetaPtr, segmentDatas,
                         dumpingSegments, deletionMapReader);
}

void PartitionInfoHolder::SetSubPartitionInfoHolder(
        const PartitionInfoHolderPtr& subPartitionInfoHolder)
{
    mSubPartitionInfoHolder = subPartitionInfoHolder;
    mPartitionInfo->SetSubPartitionInfo(
            subPartitionInfoHolder->GetPartitionInfo());
}

void PartitionInfoHolder::AddInMemorySegment(const InMemorySegmentPtr& inMemSegment)
{
    PartitionInfoPtr clonePartitionInfo(mPartitionInfo->Clone());
    clonePartitionInfo->AddInMemorySegment(inMemSegment);
    
    SetPartitionInfo(clonePartitionInfo);
}

void PartitionInfoHolder::UpdatePartitionInfo(
        const InMemorySegmentPtr& inMemSegment)
{
    if (!mPartitionInfo->NeedUpdate(inMemSegment))
    {
        return;
    }
    PartitionInfoPtr clonePartitionInfo(mPartitionInfo->Clone());
    clonePartitionInfo->UpdateInMemorySegment(inMemSegment);

    if (mSubPartitionInfoHolder)
    {
        const PartitionInfoPtr& subPartitionInfo = 
            clonePartitionInfo->GetSubPartitionInfo();
        assert(subPartitionInfo);
        mSubPartitionInfoHolder->SetPartitionInfo(subPartitionInfo);
    }

    SetPartitionInfo(clonePartitionInfo);
}

PartitionInfoPtr PartitionInfoHolder::GetPartitionInfo() const
{
    ScopedReadWriteLock lock(mPartitionInfoLock, 'r');
    return mPartitionInfo;
}

void PartitionInfoHolder::SetPartitionInfo(
        const index::PartitionInfoPtr& partitionInfo)
{
    ScopedReadWriteLock lock(mPartitionInfoLock, 'w');
    mPartitionInfo = partitionInfo;
}

IE_NAMESPACE_END(partition);

