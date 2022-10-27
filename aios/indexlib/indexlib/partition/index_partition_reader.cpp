#include <autil/StringUtil.h>
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/common/index_locator.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/util/key_hasher_typed.h"
#include "indexlib/index/online_join_policy.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, IndexPartitionReader);

index::SummaryReaderPtr IndexPartitionReader::RET_EMPTY_SUMMARY_READER = index::SummaryReaderPtr();
index::PrimaryKeyIndexReaderPtr IndexPartitionReader::RET_EMPTY_PRIMARY_KEY_READER = index::PrimaryKeyIndexReaderPtr();
index::DeletionMapReaderPtr IndexPartitionReader::RET_EMPTY_DELETIONMAP_READER = index::DeletionMapReaderPtr();
index::IndexReaderPtr IndexPartitionReader::RET_EMPTY_INDEX_READER = index::IndexReaderPtr();
index::KKVReaderPtr IndexPartitionReader::RET_EMPTY_KKV_READER = index::KKVReaderPtr();
index::KVReaderPtr IndexPartitionReader::RET_EMPTY_KV_READER = index::KVReaderPtr();

uint64_t PartitionVersion::GetHashId() const
{
    string str = mVersion.ToString() +
        StringUtil::toString(mBuildingSegmentId) +
        StringUtil::toString(mLastLinkRtSegId);

    uint64_t hashKey = 0;
    MurmurHasher hasher;
    hasher.GetHashKey(str.c_str(), str.size(), hashKey);
    return hashKey;
}

bool IndexPartitionReader::IsUsingSegment(segmentid_t segmentId) const
{
    return mPartitionVersion.IsUsingSegment(segmentId, mSchema->GetTableType());
}

int64_t IndexPartitionReader::GetLatestDataTimestamp() const
{
    index_base::PartitionDataPtr partData = GetPartitionData();
    if (!partData)
    {
        return INVALID_TIMESTAMP;
    }
    std::string locatorStr = partData->GetLastLocator();


    index_base::Version incVersion = partData->GetOnDiskVersion();
    OnlineJoinPolicy joinPolicy(incVersion, mSchema->GetTableType()); 
    common::IndexLocator indexLocator;
    int64_t incSeekTs = joinPolicy.GetRtSeekTimestampFromIncVersion();
    
    if (!locatorStr.empty() && !indexLocator.fromString(locatorStr))
    {
        return incSeekTs;
    }

    if (indexLocator == common::INVALID_INDEX_LOCATOR)
    {
        return incSeekTs;
    }
    bool fromInc;
    return OnlineJoinPolicy::GetRtSeekTimestamp(
        incSeekTs, indexLocator.getOffset(), fromInc);
}

versionid_t IndexPartitionReader::GetIncVersionId() const
{
    return GetPartitionData()->GetOnDiskVersion().GetVersionId();
}

PartitionDataPtr IndexPartitionReader::GetPartitionData() const
{
    assert(false);
    return index_base::PartitionDataPtr();
}

void IndexPartitionReader::InitPartitionVersion(const index_base::PartitionDataPtr& partitionData,
                                                segmentid_t lastLinkSegId) 
{
    assert(partitionData);
    index_base::InMemorySegmentPtr inMemSegment =
        partitionData->GetInMemorySegment();
    segmentid_t buildingSegId =
        inMemSegment ? inMemSegment->GetSegmentId() : INVALID_SEGMENTID;
    mPartitionVersion = PartitionVersion(
        partitionData->GetVersion(), buildingSegId, lastLinkSegId);
    mPartitionVersionHashId = mPartitionVersion.GetHashId();
}

IE_NAMESPACE_END(partition);

