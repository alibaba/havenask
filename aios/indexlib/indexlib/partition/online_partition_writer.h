#ifndef __ONLINE_PARTITION_WRITER_H
#define __ONLINE_PARTITION_WRITER_H

#include <tr1/memory>
#include <autil/Lock.h>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/index_define.h"
#include "indexlib/partition/offline_partition_writer.h"
#include "indexlib/partition/online_partition_metrics.h"
#include "indexlib/misc/metric_provider.h"

IE_NAMESPACE_BEGIN(partition);

class OnlinePartitionWriter : public PartitionWriter
{
public:
    OnlinePartitionWriter(
            const config::IndexPartitionOptions& options,
            const DumpSegmentContainerPtr& container,
            OnlinePartitionMetrics* onlinePartMetrics = NULL,
            const std::string& partitionName = "");

    ~OnlinePartitionWriter();

public:
    void Open(const config::IndexPartitionSchemaPtr& schema,
              const index_base::PartitionDataPtr& partitionData,
                            const PartitionModifierPtr& modifier) override;
    void ReOpenNewSegment(const PartitionModifierPtr& modifier) override;
    bool BuildDocument(const document::DocumentPtr& doc) override; 
    /* bool AddDocument(const document::DocumentPtr& doc) override; */
    /* bool UpdateDocument(const document::DocumentPtr& doc) override; */
    /* bool RemoveDocument(const document::DocumentPtr& doc) override; */
    bool NeedDump(size_t memoryQuota) const override;
    void DumpSegment() override;
    void Close() override;
    void RewriteDocument(const document::DocumentPtr& doc) override;
    uint64_t GetTotalMemoryUse() const override;
    void ReportMetrics() const override;

    void SetEnableReleaseOperationAfterDump(bool releaseOperations) override;
    uint64_t GetBuildingSegmentDumpExpandSize() const override;
    bool IsClosed() const { return mIsClosed; }
    bool DumpSegmentWithMemLimit();
    bool NeedRewriteDocument(const document::DocumentPtr& doc) override final;
    misc::Status GetStatus() const override final;
    void Reset();

    int64_t GetLastDumpTimestamp() const { return mLastDumpTs; }
private:
    virtual bool CheckTimestamp(const document::DocumentPtr& doc) const;
    int64_t GetShuffleTime(const config::IndexPartitionOptions& options,
                           int64_t randomSeed);

protected:
    config::IndexPartitionSchemaPtr mSchema;
    index_base::PartitionDataPtr mPartitionData;
    DumpSegmentContainerPtr mDumpSegmentContainer;
    OfflinePartitionWriterPtr mWriter;
    int64_t mRtFilterTimestamp;
    int64_t mLastDumpTs;
    OnlinePartitionMetrics* mOnlinePartMetrics;
    volatile bool mIsClosed;
    mutable autil::ThreadMutex mWriterLock;

    static constexpr float DEFAULT_SHUFFLE_RANGE_RATIO = 0.7;
    
private:
    friend class OnlinePartitionWriterTest;
    friend class IndexPartitionMaker;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OnlinePartitionWriter);
IE_NAMESPACE_END(partition);

#endif //__ONLINE_PARTITION_WRITER_H
