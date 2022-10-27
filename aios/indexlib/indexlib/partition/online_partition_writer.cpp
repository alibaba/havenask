#include <autil/TimeUtility.h>
#include "indexlib/partition/online_partition_writer.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/document/document.h"
#include "indexlib/util/key_hasher_typed.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index/online_join_policy.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(index_base);

DECLARE_REFERENCE_CLASS(partition, FlushedLocatorContainer);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, OnlinePartitionWriter);

#define IE_LOG_PREFIX mPartitionName.c_str()

OnlinePartitionWriter::OnlinePartitionWriter(
        const IndexPartitionOptions& options,
        const DumpSegmentContainerPtr& container,
        OnlinePartitionMetrics* onlinePartMetrics,
        const string& partitionName)
    : PartitionWriter(options, partitionName)
    , mDumpSegmentContainer(container)
    , mRtFilterTimestamp(INVALID_TIMESTAMP)
    , mLastDumpTs(INVALID_TIMESTAMP)
    , mOnlinePartMetrics(onlinePartMetrics)
    , mIsClosed(true)
{
    mLastDumpTs = TimeUtility::currentTimeInSeconds();
    if (options.GetOnlineConfig().EnableIntervalDump())
    {
        int64_t shuffleTime = GetShuffleTime(options, mLastDumpTs);
        IE_PREFIX_LOG(INFO, "OnlinePartitionWriter use shuffleTime: [%ld] seconds", shuffleTime);
        mLastDumpTs -= shuffleTime;
    }
}

OnlinePartitionWriter::~OnlinePartitionWriter() 
{
}

//for open and inc data open
void OnlinePartitionWriter::Open(
        const IndexPartitionSchemaPtr& schema,
        const PartitionDataPtr& partitionData,
        const PartitionModifierPtr& modifier)
{
    IE_PREFIX_LOG(INFO, "OnlinePartitionWriter open begin");
    assert(schema);
    mSchema = schema;

    mPartitionData = partitionData;

    MetricProviderPtr metricProvider = NULL;
    if (mOnlinePartMetrics)
    {
        metricProvider = mOnlinePartMetrics->GetMetricProvider();
    }

    ScopedLock lock(mWriterLock);
    mWriter.reset(new OfflinePartitionWriter(mOptions, mDumpSegmentContainer,
                    FlushedLocatorContainerPtr(), metricProvider, mPartitionName));
    mWriter->Open(mSchema, partitionData, modifier);
    Version version = partitionData->GetOnDiskVersion();
    OnlineJoinPolicy joinPolicy(version, mSchema->GetTableType());
    mRtFilterTimestamp = joinPolicy.GetRtFilterTimestamp();
    mIsClosed = false;
    IE_PREFIX_LOG(INFO, "OnlinePartitionWriter open end");
}

void OnlinePartitionWriter::ReOpenNewSegment(const PartitionModifierPtr& modifier)
{
    mWriter->ReOpenNewSegment(modifier);
}

bool OnlinePartitionWriter::BuildDocument(const DocumentPtr& doc)
{
    assert(doc);
    assert(doc->GetMessageCount() == 1u);
    mLastConsumedMessageCount = 0;
    if (!CheckTimestamp(doc))
    {
        return false;
    }

    if (!mWriter->BuildDocument(doc))
    {
        return false;
    }
    mLastConsumedMessageCount = 1;
    mPartitionData->UpdatePartitionInfo();
    return true;
}

bool OnlinePartitionWriter::NeedDump(size_t memoryQuota) const
{
    return mWriter->NeedDump(memoryQuota);
}

bool OnlinePartitionWriter::DumpSegmentWithMemLimit()
{
    IE_PREFIX_LOG(INFO, "dump segment begin");
    if (mWriter && mWriter->DumpSegmentWithMemLimit())
    {
        //one segment with one version
        mLastDumpTs = TimeUtility::currentTimeInSeconds();
        IE_PREFIX_LOG(INFO, "dump segment end");
        return true;
    }
    IE_PREFIX_LOG(INFO, "dump segment fail, no writer or enough memory");
    return false; 
}

void OnlinePartitionWriter::Reset()
{
    ScopedLock lock(mWriterLock);
    mIsClosed = true;
    MEMORY_BARRIER();
    mWriter.reset();
    mPartitionData.reset();
    mRtFilterTimestamp = INVALID_TIMESTAMP;
}

void OnlinePartitionWriter::DumpSegment()
{
    IE_PREFIX_LOG(INFO, "dump segment begin");
    if (mWriter)
    {
        //one segment with one version
        mWriter->DumpSegment();
        mLastDumpTs = TimeUtility::currentTimeInSeconds();
    }
    IE_PREFIX_LOG(INFO, "dump segment end");
}

void OnlinePartitionWriter::Close()
{
    ScopedLock lock(mWriterLock);
    DumpSegment();
    mIsClosed = true;
    MEMORY_BARRIER();
    mWriter.reset();
    mPartitionData.reset();
}

void OnlinePartitionWriter::RewriteDocument(const DocumentPtr& doc)
{
    mWriter->RewriteDocument(doc);
}

bool OnlinePartitionWriter::CheckTimestamp(const DocumentPtr& doc) const
{
    int64_t timestamp = doc->GetTimestamp();
    bool ret = false;
    ret = (timestamp >= mRtFilterTimestamp);
    if (!ret)
    {
        IE_PREFIX_LOG(DEBUG, "rt doc with ts[%ld], incTs[%ld], drop rt doc",
               timestamp, mRtFilterTimestamp);
        if (mOnlinePartMetrics)
        {
            mOnlinePartMetrics->IncreateObsoleteDocQps();
        }
    }
    return ret;
}

uint64_t OnlinePartitionWriter::GetTotalMemoryUse() const
{
    if (mIsClosed)
    {
        return 0;
    }

    ScopedLock lock(mWriterLock);
    return mWriter ? mWriter->GetTotalMemoryUse() : 0;
}

uint64_t OnlinePartitionWriter::GetBuildingSegmentDumpExpandSize() const
{
    if (mIsClosed)
    {
        return 0;
    }
    if (mWriterLock.trylock() == 0)
    {
        using UnlockerType =
            std::unique_ptr<autil::ThreadMutex, std::function<void(ThreadMutex*)>>;

        UnlockerType unlocker(&mWriterLock, [](ThreadMutex *lock){ lock->unlock();} );
        if (mWriter)
        {
            return mWriter->GetBuildingSegmentDumpExpandSize();
        }
    }
    return 0;
}

void OnlinePartitionWriter::ReportMetrics() const
{
    if (mIsClosed)
    {
        return;
    }
    if (mWriterLock.trylock() == 0)
    {
        using UnlockerType =
            std::unique_ptr<autil::ThreadMutex, std::function<void(ThreadMutex*)>>;

        UnlockerType unlocker(&mWriterLock, [](ThreadMutex *lock){ lock->unlock();} );
        if (mWriter)
        {
            mWriter->ReportMetrics();
        }
    }

}

int64_t OnlinePartitionWriter::GetShuffleTime(
        const IndexPartitionOptions& options, int64_t randomSeed)
{
    string str = mPartitionName + "." + StringUtil::toString(randomSeed);
    uint64_t hashValue = 0;
    MurmurHasher hasher;
    hasher.GetHashKey(str.c_str(), str.size(), hashValue);

    uint32_t totalRange = options.GetOnlineConfig().maxRealtimeDumpInterval;
    uint32_t shuffleRange = (uint32_t)(totalRange * DEFAULT_SHUFFLE_RANGE_RATIO);
    return shuffleRange == 0 ? 0 : (hashValue % shuffleRange);
}

void OnlinePartitionWriter::SetEnableReleaseOperationAfterDump(bool releaseOperations)
{
    if (mWriter)
    {
        mWriter->SetEnableReleaseOperationAfterDump(releaseOperations);
    }
}

bool OnlinePartitionWriter::NeedRewriteDocument(const DocumentPtr& doc) 
{
    return mWriter->NeedRewriteDocument(doc);
}

misc::Status OnlinePartitionWriter::GetStatus() const
{
    return mWriter->GetStatus();
}
#undef IE_LOG_PREFIX

IE_NAMESPACE_END(partition);

