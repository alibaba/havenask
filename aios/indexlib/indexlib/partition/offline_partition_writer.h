#ifndef __OFFLINE_PARTITION_WRITER_H
#define __OFFLINE_PARTITION_WRITER_H

#include <tr1/memory>
#include <autil/Lock.h>
#include "indexlib/misc/metric_provider.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/monitor.h"
#include "indexlib/partition/partition_writer.h"

DECLARE_REFERENCE_CLASS(index_base, InMemorySegment);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_STRUCT(index_base, SegmentInfo);
DECLARE_REFERENCE_CLASS(index_base, SegmentWriter);
DECLARE_REFERENCE_CLASS(partition, SortBuildChecker);
DECLARE_REFERENCE_CLASS(partition, OperationWriter);
DECLARE_REFERENCE_CLASS(partition, PartitionModifier);
DECLARE_REFERENCE_CLASS(partition, FlushedLocatorContainer);
DECLARE_REFERENCE_CLASS(partition, DumpSegmentContainer);
DECLARE_REFERENCE_CLASS(index, TTLDecoder);
DECLARE_REFERENCE_CLASS(util, AccumulativeCounter);
DECLARE_REFERENCE_CLASS(util, CounterMap);
DECLARE_REFERENCE_CLASS(util, MemoryReserver);
DECLARE_REFERENCE_CLASS(document, DocumentRewriterChain);

IE_NAMESPACE_BEGIN(partition);

class OfflinePartitionWriter : public PartitionWriter
{
public:
    OfflinePartitionWriter(
        const config::IndexPartitionOptions &options,
        const DumpSegmentContainerPtr &container,
        const FlushedLocatorContainerPtr &flushedLocatorContainer =
        FlushedLocatorContainerPtr(),
        misc::MetricProviderPtr metricProvider = misc::MetricProviderPtr(),
        const std::string &partitionName = "",
        const PartitionRange &range = PartitionRange());
    ~OfflinePartitionWriter();

public:
    void Open(const config::IndexPartitionSchemaPtr& schema,
                            const index_base::PartitionDataPtr& partitionData,
                            const PartitionModifierPtr& modifier) override;
    void ReOpenNewSegment(const PartitionModifierPtr& modifier) override;

    bool BuildDocument(const document::DocumentPtr& doc) override;
    // virtual for mock
    virtual bool AddDocument(const document::DocumentPtr& doc);
    virtual bool UpdateDocument(const document::DocumentPtr& doc);
    virtual bool RemoveDocument(const document::DocumentPtr& doc);
    
    bool NeedDump(size_t memoryQuota) const override;
    void DumpSegment() override;
    void Close() override;
    void RewriteDocument(const document::DocumentPtr& doc) override;
    uint64_t GetTotalMemoryUse() const override;
    void ReportMetrics() const override;

    void SetEnableReleaseOperationAfterDump(bool releaseOperations) override
    {
        mReleaseOperationAfterDump = releaseOperations;
    }
    
    uint64_t GetBuildingSegmentDumpExpandSize() const override
    {
        return EstimateDumpMemoryUse() + EstimateDumpFileSize();
    }
    bool NeedRewriteDocument(const document::DocumentPtr& doc) override final;
    
    misc::Status GetStatus() const override final;

    bool DumpSegmentWithMemLimit(); //TODO: delete
    void CommitVersion();

protected:
    bool IsDirty() const;
    void InitDocumentRewriter(const index_base::PartitionDataPtr& partitionData);
    void InitNewSegment();
    void Reset();
    //virtual for test
    virtual size_t GetFileSystemMemoryUse() const;

    virtual size_t GetResourceMemoryUse() const;

    virtual void CleanResource() const;

    virtual size_t EstimateMaxMemoryUseOfCurrentSegment() const;
    size_t EstimateDumpMemoryUse() const;
    size_t EstimateDumpFileSize() const;
    bool CanDumpSegment(const util::MemoryReserverPtr& fileSystemMemReserver,
                        const util::MemoryReserverPtr& dumpTmpMemReserver);

public:
    //for test
    const PartitionModifierPtr& GetModifier() const
    { return mModifier; }

private:
    struct MemoryStatusSnapShot
    {
        size_t fileSystemMemUse;
        size_t totalMemUse;
        size_t estimateDumpMemUse;
        size_t estimateDumpFileSize;
    };

private:
    void RegisterMetrics(TableType tableType);
    void InitCounters(const util::CounterMapPtr& counterMap);
    void UpdateBuildCounters(DocOperateType op); 
    void DedupBuiltSegments();
    bool AddOperation(const document::DocumentPtr& doc);
    SortBuildCheckerPtr CreateSortBuildChecker();

protected:
    config::IndexPartitionSchemaPtr mSchema;
    index_base::PartitionDataPtr mPartitionData;
    document::DocumentRewriterChainPtr mDocRewriteChain;
    index_base::InMemorySegmentPtr mInMemorySegment;
    index_base::SegmentWriterPtr mSegmentWriter;
    OperationWriterPtr mOperationWriter;
    index_base::SegmentInfoPtr mSegmentInfo;

    PartitionModifierPtr mModifier;
    mutable autil::RecursiveThreadMutex mWriterLock;
    volatile bool mReleaseOperationAfterDump;
    index::TTLDecoderPtr mTTLDecoder; 

private:
    DumpSegmentContainerPtr mDumpSegmentContainer;
    FlushedLocatorContainerPtr mFlushedLocatorContainer;
    misc::MetricProviderPtr mMetricProvider;
    double mMemoryUseRatio;
    bool mDelayDedupDocument;
    bool mInit;
    partition::SortBuildCheckerPtr mSortBuildChecker;
    PartitionRange mPartitionRange;
    
    IE_DECLARE_METRIC(buildMemoryUse);
    IE_DECLARE_METRIC(segmentWriterMemoryUse);
    IE_DECLARE_METRIC(currentSegmentMemoryQuotaUse);
    IE_DECLARE_METRIC(modifierMemoryUse);
    IE_DECLARE_METRIC(operationWriterTotalSerializeSize);
    IE_DECLARE_METRIC(operationWriterTotalDumpSize);
    IE_DECLARE_METRIC(operationWriterTotalOpCount);
    IE_DECLARE_METRIC(operationWriterMaxOpSerializeSize);
    IE_DECLARE_METRIC(operationWriterMemoryUse);
    IE_DECLARE_METRIC(estimateMaxMemoryUseOfCurrentSegment);
    IE_DECLARE_METRIC(dumpSegmentLatency);

    // counters
    util::AccumulativeCounterPtr mAddDocCountCounter;
    util::AccumulativeCounterPtr mUpdateDocCountCounter; 
    util::AccumulativeCounterPtr mDelDocCountCounter;


    static constexpr double MEMORY_USE_RATIO = 0.9;
private:
    friend class OfflinePartitionWriterTest;
    friend class OnlinePartitionWriterTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OfflinePartitionWriter);
IE_NAMESPACE_END(partition);

#endif //__OFFLINE_PARTITION_WRITER_H
