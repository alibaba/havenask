/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef __OFFLINE_PARTITION_WRITER_H
#define __OFFLINE_PARTITION_WRITER_H

#include <memory>

#include "autil/Lock.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/build_document_metrics.h"
#include "indexlib/partition/partition_writer.h"
#include "indexlib/util/metrics/MetricProvider.h"
#include "indexlib/util/metrics/Monitor.h"

DECLARE_REFERENCE_CLASS(index_base, InMemorySegment);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_STRUCT(index_base, SegmentInfo);
DECLARE_REFERENCE_CLASS(index_base, SegmentWriter);
DECLARE_REFERENCE_CLASS(partition, SortBuildChecker);
DECLARE_REFERENCE_CLASS(partition, OperationWriter);
DECLARE_REFERENCE_CLASS(partition, PartitionModifier);
DECLARE_REFERENCE_CLASS(partition, DocIdManager);
DECLARE_REFERENCE_CLASS(partition, FlushedLocatorContainer);
DECLARE_REFERENCE_CLASS(partition, DumpSegmentContainer);
DECLARE_REFERENCE_CLASS(index, TTLDecoder);
DECLARE_REFERENCE_CLASS(index, BuildProfilingMetrics);
DECLARE_REFERENCE_CLASS(util, AccumulativeCounter);
DECLARE_REFERENCE_CLASS(util, CounterMap);
DECLARE_REFERENCE_CLASS(util, GroupedThreadPool);
DECLARE_REFERENCE_CLASS(util, MemoryReserver);
DECLARE_REFERENCE_CLASS(util, WaitMemoryQuotaController);
DECLARE_REFERENCE_CLASS(document, DocumentRewriterChain);
DECLARE_REFERENCE_CLASS(document, DocumentCollector);

namespace indexlib { namespace partition {

class OfflinePartitionWriter : public PartitionWriter
{
public:
    OfflinePartitionWriter(const config::IndexPartitionOptions& options, const DumpSegmentContainerPtr& container,
                           const FlushedLocatorContainerPtr& flushedLocatorContainer = FlushedLocatorContainerPtr(),
                           util::MetricProviderPtr metricProvider = util::MetricProviderPtr(),
                           const std::string& partitionName = "", const PartitionRange& range = PartitionRange());
    ~OfflinePartitionWriter();

public:
    void Open(const config::IndexPartitionSchemaPtr& schema, const index_base::PartitionDataPtr& partitionData,
              const PartitionModifierPtr& modifier) override;

    void ReOpenNewSegment(const PartitionModifierPtr& modifier) override;

    bool BuildDocument(const document::DocumentPtr& doc) override;
    // 接管 docCollector 的生命周期，负责在线程池中而非主线程中析构它
    bool BatchBuild(std::unique_ptr<document::DocumentCollector> docCollector) override;
    PartitionWriter::BuildMode GetBuildMode() override { return _buildMode; }
    void SwitchBuildMode(PartitionWriter::BuildMode buildMode) override;

    bool NeedDump(size_t memoryQuota, const document::DocumentCollector* docCollector = nullptr) const override;
    void EndIndex() override;
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

    util::Status GetStatus() const override final;
    bool EnableAsyncDump() const override { return mEnableAsyncDump; }

    bool DumpSegmentWithMemLimit(); // TODO: delete
    void CommitVersion();

    int64_t GetFixedCostMemorySize() const { return mFixedCostMemorySize; }

protected:
    bool IsDirty() const;
    void InitDocumentRewriter(const index_base::PartitionDataPtr& partitionData);
    void InitNewSegment();
    void Reset();
    // virtual for test
    virtual size_t GetFileSystemMemoryUse() const;

    virtual size_t GetResourceMemoryUse() const;

    virtual void CleanResource() const;

    virtual size_t EstimateMaxMemoryUseOfCurrentSegment() const;
    size_t EstimateDumpMemoryUse() const;
    size_t EstimateDumpFileSize() const;
    bool CanDumpSegment(const util::MemoryReserverPtr& fileSystemMemReserver,
                        const util::MemoryReserverPtr& dumpTmpMemReserver);

public:
    // for test
    const PartitionModifierPtr& GetModifier() const { return mModifier; }

private:
    // virtual for mock
    virtual bool AddDocument(const document::DocumentPtr& doc);
    virtual bool UpdateDocument(const document::DocumentPtr& doc);
    virtual bool RemoveDocument(const document::DocumentPtr& doc);

private:
    struct MemoryStatusSnapShot {
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
    index::BuildProfilingMetricsPtr CreateBuildProfilingMetrics();
    std::unique_ptr<BuildDocumentMetrics> CreateBuildDocumentMetrics();
    void ReportBuildDocumentMetrics(const document::DocumentPtr& doc, uint32_t successMsgCount);
    bool CheckFixedCostMemorySize(int64_t fixedCostMemorySize);

    // virtual for test
    virtual bool PreprocessDocument(const document::DocumentPtr& doc);
    void PreprocessDocuments(document::DocumentCollector* docs);
    bool Validate(const document::DocumentPtr& doc);
    void ReinitDocIdManager();

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
    // _docIdManager should be initialized differently based on batch mode.
    std::unique_ptr<DocIdManager> _docIdManager;
    std::unique_ptr<util::GroupedThreadPool> _buildThreadPool;
    std::unique_ptr<util::WaitMemoryQuotaController> _docCollectorMemController;

    DumpSegmentContainerPtr mDumpSegmentContainer;
    FlushedLocatorContainerPtr mFlushedLocatorContainer;
    util::MetricProviderPtr mMetricProvider;
    double mMemoryUseRatio;
    bool mDelayDedupDocument;
    bool mInit;
    partition::SortBuildCheckerPtr mSortBuildChecker;
    index::BuildProfilingMetricsPtr mBuildProfilingMetrics;
    std::unique_ptr<BuildDocumentMetrics> _buildDocumentMetrics;
    PartitionRange mPartitionRange;
    mutable int64_t mFixedCostMemorySize;                 // pk memory use
    std::pair<segmentid_t, int64_t> mBuildingSegmentInfo; // first:segId, second:initTimestamp
    bool mEnableAsyncDump;

    IE_DECLARE_METRIC(buildMemoryUse);
    IE_DECLARE_METRIC(segmentWriterMemoryUse);
    IE_DECLARE_METRIC(currentSegmentMemoryQuotaUse);
    IE_DECLARE_METRIC(modifierMemoryUse);
    IE_DECLARE_METRIC(docCollectorMemoryUse);
    IE_DECLARE_METRIC(operationWriterTotalSerializeSize);
    IE_DECLARE_METRIC(operationWriterTotalDumpSize);
    IE_DECLARE_METRIC(operationWriterTotalOpCount);
    IE_DECLARE_METRIC(operationWriterMaxOpSerializeSize);
    IE_DECLARE_METRIC(operationWriterMemoryUse);
    IE_DECLARE_METRIC(estimateMaxMemoryUseOfCurrentSegment);
    IE_DECLARE_METRIC(dumpSegmentLatency);
    IE_DECLARE_METRIC(segmentBuildingTime);

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
}} // namespace indexlib::partition

#endif //__OFFLINE_PARTITION_WRITER_H
