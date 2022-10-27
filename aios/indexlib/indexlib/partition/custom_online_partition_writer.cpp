#include "indexlib/partition/custom_online_partition_writer.h"
#include "indexlib/partition/custom_partition_data.h"
#include "indexlib/partition/modifier/partition_modifier.h"
#include "indexlib/partition/online_partition_metrics.h"
#include "indexlib/table/table_factory_wrapper.h"
#include "indexlib/document/document.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index/online_join_policy.h"
#include "indexlib/misc/log.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(table);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, CustomOnlinePartitionWriter);

#define IE_LOG_PREFIX mPartitionName.c_str()

CustomOnlinePartitionWriter::CustomOnlinePartitionWriter(
    const IndexPartitionOptions& options,
    const TableFactoryWrapperPtr& tableFactory,
    const FlushedLocatorContainerPtr& flushedLocatorContainer,
    OnlinePartitionMetrics* onlinePartMetrics,
    const string& partitionName, const PartitionRange& range)
    : CustomOfflinePartitionWriter(
        options, tableFactory, flushedLocatorContainer,
        onlinePartMetrics->GetMetricProvider(), partitionName, range)
    , mOnlinePartMetrics(onlinePartMetrics)
    , mIsClosed(true)
{
}

CustomOnlinePartitionWriter::~CustomOnlinePartitionWriter() 
{
}

void CustomOnlinePartitionWriter::Open(const config::IndexPartitionSchemaPtr& schema,
              const PartitionDataPtr& partitionData,
              const PartitionModifierPtr& modifier)
{
    ScopedLock lock(mOnlineWriterLock); 
    Version incVersion = partitionData->GetOnDiskVersion();
    OnlineJoinPolicy joinPolicy(incVersion, schema->GetTableType());
    mRtFilterTimestamp = joinPolicy.GetRtFilterTimestamp();
    CustomOfflinePartitionWriter::Open(schema, partitionData, PartitionModifierPtr());
    mIsClosed = false;
}

bool CustomOnlinePartitionWriter::CheckTimestamp(const document::DocumentPtr& doc) const
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

bool CustomOnlinePartitionWriter::BuildDocument(const DocumentPtr& doc)
{
    if (mIsClosed)
    {
        IE_LOG(ERROR, "online writer is closed in partition[%s]", mPartitionName.c_str());
        mLastConsumedMessageCount = 0;
        return false;
    }
    // TODO: consider if table plugin need to customized filterTimestamp? 
    // if (!CheckTimestamp(doc))
    // {
    //     return false;
    // }
    return CustomOfflinePartitionWriter::BuildDocument(doc);
}

void CustomOnlinePartitionWriter::Close()
{
    ScopedLock lock(mOnlineWriterLock); 
    if (mIsClosed)
    {
        return;
    }
    // will trigger dumpSegment
    CustomOfflinePartitionWriter::DoClose();
    CustomOfflinePartitionWriter::Release(); 
    mIsClosed = true;
}

void CustomOnlinePartitionWriter::ReportMetrics() const
{
    if (mIsClosed)
    {
        return;
    }
    if (mOnlineWriterLock.trylock() == 0)
    {
        using UnlockerType =
            std::unique_ptr<autil::ThreadMutex, std::function<void(ThreadMutex*)>>;

        UnlockerType unlocker(&mOnlineWriterLock, [](ThreadMutex *lock){ lock->unlock();} );
        CustomOfflinePartitionWriter::ReportMetrics();
    }
}

IE_NAMESPACE_END(partition);

