#ifndef __INDEXLIB_ONLINE_PARTITION_H
#define __INDEXLIB_ONLINE_PARTITION_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/partition/reader_container.h"
#include "indexlib/partition/modifier/partition_modifier.h"
#include "indexlib/misc/monitor.h"
#include "indexlib/partition/online_partition_metrics.h"
#include "indexlib/partition/partition_resource_calculator.h"
#include "indexlib/partition/reopen_decider.h"
#include "indexlib/partition/open_executor/open_executor_chain.h"
#include "indexlib/partition/open_executor/executor_resource.h"
#include "indexlib/partition/online_partition_task_item.h"
#include "indexlib/partition/search_reader_updater.h"

DECLARE_REFERENCE_CLASS(partition, OnlinePartitionReader);
DECLARE_REFERENCE_CLASS(partition, TableReaderContainerUpdater);
DECLARE_REFERENCE_CLASS(partition, DumpSegmentContainer);
DECLARE_REFERENCE_CLASS(partition, JoinSegmentWriter);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(index_base, Version);
DECLARE_REFERENCE_CLASS(partition, OnlinePartitionWriter);
DECLARE_REFERENCE_CLASS(partition, AsyncSegmentDumper);
DECLARE_REFERENCE_CLASS(partition, VirtualAttributeContainer);
DECLARE_REFERENCE_CLASS(partition, LocalIndexCleaner);
DECLARE_REFERENCE_CLASS(common, ExecutorScheduler);
DECLARE_REFERENCE_CLASS(util, CounterMap);
DECLARE_REFERENCE_CLASS(util, MemoryQuotaSynchronizer);
DECLARE_REFERENCE_CLASS(util, SearchCachePartitionWrapper);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);

namespace future_lite
{
class Executor;
}

IE_NAMESPACE_BEGIN(partition);

class OnlinePartition : public IndexPartition
{
public:
    OnlinePartition(const std::string& partitionName,
                    const IndexPartitionResource& partitionResource);

    OnlinePartition(const std::string& partitionName,
                    const util::MemoryQuotaControllerPtr& memQuotaController,
                    misc::MetricProviderPtr metricProvider = misc::MetricProviderPtr());
    
    //for test
    OnlinePartition();
    ~OnlinePartition();

public:
    OpenStatus Open(const std::string& primaryDir, const std::string& secondaryDir,
                    const config::IndexPartitionSchemaPtr& schema,
                    const config::IndexPartitionOptions& options,
                    versionid_t targetVersionId = INVALID_VERSION) override;
    OpenStatus ReOpen(bool forceReopen,
                      versionid_t targetVersionId = INVALID_VERSION) override;
    void ReOpenNewSegment() override;
    void Close() override;

    PartitionWriterPtr GetWriter() const override;
    IndexPartitionReaderPtr GetReader() const override;
    IndexPartitionReaderPtr GetReader(versionid_t incVersionId) const
    {
        return mReaderContainer->GetLatestReader(incVersionId);
    }
    MemoryStatus CheckMemoryStatus() const override;
    int64_t GetRtSeekTimestampFromIncVersion() const override;

    config::IndexPartitionSchemaPtr GetSchema() const override
    { autil::ScopedLock lock(mSchemaLock); return mSchema; }
    void ExecuteTask(OnlinePartitionTaskItem::TaskType taskType);
    virtual util::MemoryReserverPtr ReserveVirtualAttributesResource(
                        const config::AttributeConfigVector& mainVirtualAttrConfigs,
                        const config::AttributeConfigVector& subVirtualAttrConfigs);
    virtual void AddVirtualAttributes(
        const config::AttributeConfigVector& mainVirtualAttrConfigs,
        const config::AttributeConfigVector& subVirtualAttrConfigs);
    bool CleanIndexFiles(const std::vector<versionid_t>& keepVersionIds) override;

public:
    //only for test
    index_base::PartitionDataPtr GetPartitionData() const
    { return mPartitionDataHolder.Get(); }

    //for test & index printer
    const file_system::IndexlibFileSystemPtr& GetFileSystem() const
    { return mFileSystem; }

    OnlinePartitionMetrics GetPartitionMetrics() const
    { return *mOnlinePartMetrics; }

    void TEST_SetDumpSegmentContainer(const DumpSegmentContainerPtr& dumpSegmentContainer)
    {
        mDumpSegmentContainer = dumpSegmentContainer;
    }

    const DumpSegmentContainerPtr& GetDumpSegmentContainer() const
    {
        return mDumpSegmentContainer;
    }
    void FlushDumpSegmentInContainer();

    void AddReaderUpdater(TableReaderContainerUpdater* readerUpdater) override
    {
        if (mReaderUpdater)
        {
            mReaderUpdater->AddReaderUpdater(readerUpdater);
        }
    }
    void RemoveReaderUpdater(TableReaderContainerUpdater* readerUpdater) override
    {
        if (mReaderUpdater)
        {
            return mReaderUpdater->RemoveReaderUpdater(readerUpdater);
        }
    }

    uint64_t GetPartitionIdentifier() const override
    {
        return mPartitionIdentifier;
    }
    void Lock() const override
    {
        mCleanerLock.lock();
        mDataLock.lock();
    }
    void UnLock() const override
    {
        mDataLock.unlock();
        mCleanerLock.unlock();
    }
protected:
    // virtual for test
    virtual void InitReader(const index_base::PartitionDataPtr& partData);
    virtual OpenExecutorChainPtr createOpenExecutorChain() 
    {
        OpenExecutorChainPtr chain(new OpenExecutorChain);
        return chain;
    }
    virtual PartitionResourceCalculatorPtr CreateResourceCalculator(
            const config::IndexPartitionOptions& options,
            const index_base::PartitionDataPtr& partitionData,
            const PartitionWriterPtr& writer,
            const plugin::PluginManagerPtr& pluginManager) const;
    virtual void ReportMetrics();
    virtual void ExecuteCleanResourceTask();

    void DumpSegmentOverInterval();
    
private:
    OpenStatus DoOpen(const std::string& primaryDirr,
                      const std::string& secondaryDir, 
                      const config::IndexPartitionSchemaPtr& schema, 
                      const config::IndexPartitionOptions& options,
                      versionid_t targetVersionId,
                      IndexPartition::IndexPhase indexPhase);

    void DoInitReader(const index_base::PartitionDataPtr& partData,
                      segmentid_t flushRtSegmentId);
    void InitWriter(const index_base::PartitionDataPtr& partData);
    void InitResourceCleaner();
    void InitResourceCalculator();

    void CheckParam(const config::IndexPartitionSchemaPtr& schema,
                    const config::IndexPartitionOptions& options) const;

    void CleanResource();
    void UpdateOnlinePartitionMetrics();

    void LoadReaderPatch(const index_base::PartitionDataPtr& partitionData,
                         partition::OnlinePartitionReaderPtr& reader);

    void SwitchReader(const partition::OnlinePartitionReaderPtr& reader);

    void AddOnDiskIndexCleaner();

    void AddVirtualAttributeDataCleaner(
            const config::IndexPartitionSchemaPtr &newSchema);
    void CollectVirtualAttributes(
            const config::AttributeSchemaPtr& newSchema,
            std::vector<std::pair<std::string, bool>>& attrNames,
            bool isSub);

    config::IndexPartitionSchemaPtr CreateRtSchema(const config::IndexPartitionOptions& options);

    config::IndexPartitionSchemaPtr CreateNewSchema(
            const config::IndexPartitionSchemaPtr& schema,
            const config::AttributeConfigVector& mainVirtualAttrConfigs,
            const config::AttributeConfigVector& subVirtualAttrConfigs);
    bool HasSameVirtualAttrSchema(
            const config::IndexPartitionSchemaPtr& newSchema,
            const config::IndexPartitionSchemaPtr& oldSchema);
    void ResetRtAndJoinDirPath(
            const file_system::IndexlibFileSystemPtr& fileSystem);
    
    PartitionModifierPtr CreateReaderModifier(
            const IndexPartitionReaderPtr& reader);


    JoinSegmentWriterPtr CreateJoinSegmentWriter(
        const index_base::Version& onDiskVersion, bool isForceReopen) const;

    bool InitAccessCounters(const util::CounterMapPtr& counterMap,
                            const config::IndexPartitionSchemaPtr& schema);
    void SetEnableReleaseOperationAfterDump(bool releaseOperations);
    ReopenDecider::ReopenType MakeReopenDecision(
            bool forceReopen,
            versionid_t targetVersionId,
            index_base::Version& onDiskVersion);
    
    void DoClose(); 
    void Reset();
    bool PrepareIntervalTask();
private:
    typedef std::tr1::shared_ptr<autil::ScopedLock> ScopedLockPtr;

    IndexPartition::OpenStatus DoReopen(bool forceReopen, versionid_t targetVersionId);
    IndexPartition::OpenStatus NormalReopen(ScopedLockPtr& scopedLock,
                                            index_base::Version& onDiskVersion);
    OpenStatus ForceReopen(index_base::Version& onDiskVersion);
    IndexPartition::OpenStatus ReclaimReaderMem();
    IndexPartition::OpenStatus SwitchFlushRtSegments();

    ExecutorResource CreateExecutorResource(const index_base::Version& onDiskVersion,
                                            bool forceReopen);

    OpenExecutorPtr CreateUnlockExecutor(ScopedLockPtr& scopedLock);
    OpenExecutorPtr CreatePreloadExecutor();
    OpenExecutorPtr CreatePreJoinExecutor(const JoinSegmentWriterPtr& joinSegWriter);
    OpenExecutorPtr CreateLockExecutor(ScopedLockPtr& scopedLock);
    OpenExecutorPtr CreateReclaimRtIndexExecutor();
    OpenExecutorPtr CreateReclaimRtSegmentsExecutor(bool reclaimBuildingSegment = true);
    OpenExecutorPtr CreateGenerateJoinSegmentExecutor(
        const JoinSegmentWriterPtr& joinSegWriter);
    virtual OpenExecutorPtr CreateReopenPartitionReaderExecutor(bool hasPreload);
    OpenExecutorPtr CreateReleaseReaderExecutor();
    OpenExecutorPtr CreateDumpSegmentExecutor(bool reInitReaderAndWriter = true);
    virtual OpenExecutorChainPtr CreateReopenExecutorChain(index_base::Version& onDiskVersion,
            ScopedLockPtr& scopedLock);
    virtual OpenExecutorChainPtr CreateForceReopenExecutorChain(index_base::Version& onDiskVersion);

    segmentid_t GetLatestLinkRtSegId(const index_base::PartitionDataPtr& partData,
                                     const util::MemoryReserverPtr& memReserver) const;

    void ReclaimRemainFlushRtIndex(const index_base::PartitionDataPtr& partData);
    void UpdateSwitchRtSegmentSize();

    int64_t GetLatestNeedSkipIncTs(const index_base::Version& incVersion) const;
    util::MemoryQuotaSynchronizerPtr CreateRealtimeMemoryQuotaSynchronizer(
        const util::MemoryQuotaControllerPtr& realtimeQuotaController);

    size_t EstimateAttributeExpandSize(
            const config::AttributeSchemaPtr& virtualAttrSchema,
            const config::AttributeConfigVector& attributeConfigs, size_t docCount);

    void CheckSecondIndex();
    bool NeedCheckSecondIndex() const
    {
        return mCheckSecondIndexIntervalInMin > 0 &&
            mOptions.GetOnlineConfig().NeedReadRemoteIndex() &&
            !mOpenIndexSecondaryDir.empty();
    }
    void SubscribeSecondIndex();
    bool NeedSubscribeSecondIndex() const
    {
        return mSubscribeSecondIndexIntervalInMin > 0 &&
            mOptions.GetOnlineConfig().NeedReadRemoteIndex() &&
            !mOpenIndexSecondaryDir.empty();
    }
    file_system::IndexlibFileSystemPtr CreateFileSystem(
            const std::string& primaryDir, const std::string& secondaryDir) override;
    void RewriteSchemaAndOptions(const config::IndexPartitionSchemaPtr& schema,
                                 const index_base::Version& onDiskVersion);

    void InitPathMetaCache(const file_system::DirectoryPtr& deployMetaDir,
                           const index_base::Version& version);

protected:
    config::IndexPartitionSchemaPtr mRtSchema;
    OnlinePartitionWriterPtr mWriter;

    mutable autil::ThreadMutex mReaderLock;
    mutable autil::RecursiveThreadMutex mSchemaLock;
    mutable autil::RecursiveThreadMutex mCleanerLock;
    
    partition::OnlinePartitionReaderPtr mReader;
    ReaderContainerPtr mReaderContainer;
    index_base::Version mLoadedIncVersion;
    common::ExecutorSchedulerPtr mResourceCleaner;
    LocalIndexCleanerPtr mLocalIndexCleaner;
    DumpSegmentContainerPtr mDumpSegmentContainer;

    util::SearchCachePartitionWrapperPtr mSearchCache;
    PartitionResourceCalculatorPtr mResourceCalculator;
    OnlinePartitionMetricsPtr mOnlinePartMetrics;
    util::MemoryQuotaSynchronizerPtr mRtMemQuotaSynchronizer;
    util::MemoryQuotaControllerPtr mRealtimeQuotaController;

    volatile bool mClosed;
    volatile bool mIsServiceNormal;
    volatile IndexPartition::IndexPhase mIndexPhase;
    int32_t mCleanResourceTaskId;
    int32_t mReportMetricsTaskId;
    int32_t mIntervalDumpTaskId;
    int32_t mAsyncDumpTaskId;
    int64_t mLatestNeedSkipIncTs;
    uint64_t mPartitionIdentifier;    
    AsyncSegmentDumperPtr mAsyncSegmentDumper;
    VirtualAttributeContainerPtr mVirtualAttrContainer;
    SearchReaderUpdaterPtr mReaderUpdater;

    int32_t mCheckIndexTaskId;
    int64_t mCheckSecondIndexIntervalInMin;
    int32_t mSubscribeIndexTaskId;
    int64_t mSubscribeSecondIndexIntervalInMin;
    std::atomic<int64_t> mMissingSegmentCount;
    future_lite::Executor* mFutureExecutor;

private:
    friend class OnlinePartitionTest;
    friend class ReopenDeciderTest;
    friend class OnlinePartitionInteTest;
    friend class ForceReopenInteTest;
    friend class MemoryControllerTest;
    friend class OnlinePartitionExceptionTest;
    friend class PartitionMemControlPerfTest;
    friend class DumpContainerFlushExecutorTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OnlinePartition);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_ONLINE_PARTITION_H
