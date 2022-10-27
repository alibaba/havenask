#ifndef __INDEXLIB_CUSTOM_ONLINE_PARTITION_H
#define __INDEXLIB_CUSTOM_ONLINE_PARTITION_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/partition/reopen_decider.h"
#include "indexlib/partition/search_reader_updater.h"
#include <future>

DECLARE_REFERENCE_CLASS(util, MemoryQuotaController);
DECLARE_REFERENCE_CLASS(util, MemoryQuotaSynchronizer);
DECLARE_REFERENCE_CLASS(util, BlockMemoryQuotaController);
DECLARE_REFERENCE_CLASS(util, ExecutorBase);
DECLARE_REFERENCE_CLASS(common, ExecutorScheduler);
DECLARE_REFERENCE_CLASS(partition, IndexPartitionReader);
DECLARE_REFERENCE_CLASS(partition, CustomOnlinePartitionReader);
DECLARE_REFERENCE_CLASS(partition, CustomOnlinePartitionWriter);
DECLARE_REFERENCE_CLASS(partition, ReaderContainer);
DECLARE_REFERENCE_CLASS(partition, PartitionResourceCalculator);
DECLARE_REFERENCE_CLASS(partition, IndexPartitionResource);
DECLARE_REFERENCE_CLASS(partition, OnlinePartitionMetrics);
DECLARE_REFERENCE_CLASS(partition, LocalIndexCleaner);
DECLARE_REFERENCE_CLASS(partition, DumpSegmentQueue);
DECLARE_REFERENCE_CLASS(table, TableFactoryWrapper);
DECLARE_REFERENCE_CLASS(config, IndexPartitionOptions);

IE_NAMESPACE_BEGIN(partition);

class CustomOnlinePartition : public IndexPartition
{
public:
    CustomOnlinePartition(const std::string& partitionName,
                          const partition::IndexPartitionResource& partitionResource);
    ~CustomOnlinePartition();
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
    void SetForceSeekInfo(const std::pair<int64_t, int64_t>& forceSeekInfo) override;
    MemoryStatus CheckMemoryStatus() const override;
    bool CleanIndexFiles(const std::vector<versionid_t>& keepVersionIds) override;
    int64_t GetRtSeekTimestampFromIncVersion() const override;
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
    std::string GetLastLocator() const override;

private:
    typedef std::shared_ptr<autil::ScopedLock> ScopedLockPtr;    
    OpenStatus DoOpen(const std::string& primaryDir,
                      const std::string& secondaryDir, 
                      const config::IndexPartitionSchemaPtr& schema, 
                      const config::IndexPartitionOptions& options,
                      versionid_t targetVersionId,
                      bool isForceOpen);

    IndexPartition::OpenStatus DoReopen(bool forceReopen, versionid_t targetVersionId);
    IndexPartition::OpenStatus NormalReopen(ScopedLockPtr& dataLock,
                                            index_base::Version& incVersion);

    IndexPartition::OpenStatus NormalReopenWithSegmentLevelPreload(
        ScopedLockPtr& dataLock, index_base::Version& incVersion);

    CustomOnlinePartitionReaderPtr CreateDiffVersionReader(
        const index_base::Version& newVersion, const index_base::Version& lastVersion) const;

    ReopenDecider::ReopenType MakeReopenDecision(
            bool forceReopen,
            versionid_t targetVersionId,
            index_base::Version& onDiskVersion);

    /* ExecutorChainPtr CreateReopenExecutorChain(const index_base::Version& onDiskVersion) const; */
    /* util::ExecutorBasePtr CreateDumpSegmentExecutor(); */
    /* util::ExecutorBasePtr CreateReclaimRtSegmentExecutor(); */
    /* util::ExecutorBasePtr CreateReleaseReaderExecutor(); */
    /* util::ExecutorBasePtr CreateReopenPartitionReaderExecutor(); */
    
    void DoClose();

    index_base::PartitionDataPtr CreateCustomPartitionData(
        const index_base::Version& incVersion,
        const index_base::Version& lastIncVersion,
        const DumpSegmentQueuePtr& dumpSegmentQueue,
        int64_t reclaimTimestamp,
        bool ignoreInMemVersion);
    
    util::MemoryQuotaSynchronizerPtr CreateRealtimeMemoryQuotaSynchronizer(
            const util::MemoryQuotaControllerPtr& realtimeQuotaController);

    void CheckParam(const config::IndexPartitionSchemaPtr& schema,
                    const config::IndexPartitionOptions& options) const;

    CustomOnlinePartitionReaderPtr InitReaderWithMemoryLimit(
        const index_base::PartitionDataPtr& partData,
        const CustomOnlinePartitionWriterPtr& writer,
        const index_base::Version& lastIncVersion,
        const index_base::Version& newIncVersion);

    CustomOnlinePartitionReaderPtr InitReaderWithHeritage(
        const CustomOnlinePartitionWriterPtr& writer,
        const index_base::PartitionDataPtr& partitionData,
        const CustomOnlinePartitionReaderPtr& lastReader,
        const CustomOnlinePartitionReaderPtr& preloadReader);

    CustomOnlinePartitionReaderPtr CreateDiffVersionReader(
        const index_base::Version& newVersion, const index_base::Version& lastVersion);

    void InitWriter(const index_base::PartitionDataPtr& partData,
                    CustomOnlinePartitionWriterPtr& writer);
    
    CustomOnlinePartitionReaderPtr InitReader(
        const CustomOnlinePartitionWriterPtr& writer,
        const index_base::PartitionDataPtr& partitionData);

    void SwitchReader(const CustomOnlinePartitionReaderPtr& reader);

    void InitResourceCalculator(const index_base::PartitionDataPtr& partData,
                                const CustomOnlinePartitionWriterPtr& writer);

    bool TryLock(autil::ThreadMutex& lock, size_t times, int64_t intervalInUs);
    void CheckSecondaryIndex();
    bool NeedCheckSecondaryIndex() const
    {
        // TODO: online config 添加配置：表级别 > 环境变量 > -1
        return mCheckSecondaryIndexIntervalInMin > 0 &&
            mOptions.GetOnlineConfig().NeedReadRemoteIndex() &&
            !mOpenIndexSecondaryDir.empty();
    }

    void DoInitPathMetaCache(const file_system::DirectoryPtr& deployMetaDir,
                           const index_base::Version& version);        
    void InitPathMetaCache(const file_system::DirectoryPtr& deployMetaDir,
                           const index_base::Version& version);    
    void Reset();
    void InitResourceCleaner();
    void AddOnDiskIndexCleaner();
    void ExecuteCleanResourceTask();
    void ExecuteCleanPartitionReaderTask();
    void CleanResource();
    void UpdateOnlinePartitionMetrics();    
    void ReportMetrics();
    void AsyncDumpSegment();
    void FlushDumpSegmentQueue(); 
    bool PrepareIntervalTask();
    void UpdateReader();

    void CleanPartitionReaders();

private:
    static size_t DEFAULT_IO_EXCEPTION_RETRY_LIMIT;
    static size_t DEFAULT_IO_EXCEPTION_RETRY_INTERVAL; 
    
private:
    volatile bool mClosed;
    volatile bool mIsServiceNormal;
    volatile IndexPartition::IndexPhase mIndexPhase; 
    util::MemoryQuotaControllerPtr mRealtimeQuotaController;
    uint64_t mPartitionIdentifier;
    util::MemoryQuotaSynchronizerPtr mRtMemQuotaSynchronizer;
    index_base::Version mLoadedIncVersion;
    table::TableFactoryWrapperPtr mTableFactory;
    CustomOnlinePartitionWriterPtr mWriter;
    CustomOnlinePartitionReaderPtr mReader;
    mutable autil::ThreadMutex mReaderLock;
    mutable autil::RecursiveThreadMutex mSchemaLock;
    mutable autil::RecursiveThreadMutex mCleanerLock;    
    ReaderContainerPtr mReaderContainer;
    SearchReaderUpdaterPtr mReaderUpdater;    
    PartitionResourceCalculatorPtr mResourceCalculator;
    OnlinePartitionMetricsPtr mOnlinePartMetrics; 
    LocalIndexCleanerPtr mLocalIndexCleaner;
    common::ExecutorSchedulerPtr mResourceCleaner;
    DumpSegmentQueuePtr mDumpSegmentQueue;
    int32_t mCleanPartitionReaderTaskId;
    int32_t mCleanResourceTaskId;
    int32_t mReportMetricsTaskId;
    int32_t mAsyncDumpTaskId;
    int32_t mCheckIndexTaskId;
    int64_t mCheckSecondaryIndexIntervalInMin;
    size_t mIOExceptionRetryLimit;
    size_t mIOExceptionRetryInterval;
    std::atomic<int64_t> mMissingSegmentCount;    
    std::future<void> mAsyncDumpFuture;
    util::BlockMemoryQuotaControllerPtr mTableWriterMemController;
    bool mSupportSegmentLevelReader;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CustomOnlinePartition);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_CUSTOM_ONLINE_PARTITION_H
