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
#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/document/index_locator.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/partition/modifier/partition_modifier.h"
#include "indexlib/partition/online_partition_metrics.h"
#include "indexlib/partition/online_partition_task_item.h"
#include "indexlib/partition/open_executor/executor_resource.h"
#include "indexlib/partition/open_executor/open_executor_chain_creator.h"
#include "indexlib/partition/operation_queue/dump_operation_redo_strategy.h"
#include "indexlib/partition/operation_queue/operation_cursor.h"
#include "indexlib/partition/partition_resource_calculator.h"
#include "indexlib/partition/reader_container.h"
#include "indexlib/partition/realtime_index_synchronizer.h"
#include "indexlib/partition/reopen_decider.h"
#include "indexlib/partition/search_reader_updater.h"
#include "indexlib/util/memory_control/SimpleMemoryQuotaController.h"
#include "indexlib/util/metrics/Monitor.h"

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
DECLARE_REFERENCE_CLASS(util, TaskItem);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);

namespace indexlib::file_system {
class LifecycleTable;
}

namespace future_lite {
class Executor;
}

namespace indexlib { namespace partition {

class OnlinePartition : public IndexPartition
{
public:
    OnlinePartition();
    OnlinePartition(const IndexPartitionResource& partitionResource);
    ~OnlinePartition();

public:
    OpenStatus Open(const std::string& primaryDir, const std::string& secondaryDir,
                    const config::IndexPartitionSchemaPtr& schema, const config::IndexPartitionOptions& options,
                    versionid_t targetVersionId = INVALID_VERSIONID) override;
    OpenStatus ReOpen(bool forceReopen, versionid_t targetVersionId = INVALID_VERSIONID) override;
    void ReOpenNewSegment() override;
    void Close() override;

    PartitionWriterPtr GetWriter() const override;
    IndexPartitionReaderPtr GetReader() const override;
    IndexPartitionReaderPtr GetReader(versionid_t incVersionId) const override
    {
        return mReaderContainer->GetLatestReader(incVersionId);
    }
    MemoryStatus CheckMemoryStatus() const override;
    int64_t GetRtSeekTimestampFromIncVersion() const override;

    config::IndexPartitionSchemaPtr GetSchema() const override
    {
        autil::ScopedLock lock(mSchemaLock);
        return mSchema;
    }
    bool ExecuteTask(OnlinePartitionTaskItem::TaskType taskType);
    virtual util::MemoryReserverPtr
    ReserveVirtualAttributesResource(const config::AttributeConfigVector& mainVirtualAttrConfigs,
                                     const config::AttributeConfigVector& subVirtualAttrConfigs) override;
    virtual void AddVirtualAttributes(const config::AttributeConfigVector& mainVirtualAttrConfigs,
                                      const config::AttributeConfigVector& subVirtualAttrConfigs) override;
    bool CleanIndexFiles(const std::vector<versionid_t>& keepVersionIds) override;
    bool CleanUnreferencedIndexFiles(const std::set<std::string>& toKeepFiles) override;

    document::SrcSignature GetSrcSignature() const override { return mSrcSignature; }

public:
    // only for test
    index_base::PartitionDataPtr GetPartitionData() const { return mPartitionDataHolder.Get(); }

    // for test & index printer
    const file_system::IFileSystemPtr& GetFileSystem() const { return mFileSystem; }

    OnlinePartitionMetrics* GetPartitionMetrics() const { return mOnlinePartMetrics.get(); }

    void TEST_SetDumpSegmentContainer(const DumpSegmentContainerPtr& dumpSegmentContainer)
    {
        mDumpSegmentContainer = dumpSegmentContainer;
    }

    const DumpSegmentContainerPtr& GetDumpSegmentContainer() const { return mDumpSegmentContainer; }
    void FlushDumpSegmentInContainer();

    void AddReaderUpdater(TableReaderContainerUpdater* readerUpdater) override
    {
        if (mReaderUpdater) {
            mReaderUpdater->AddReaderUpdater(readerUpdater);
        }
    }
    void RemoveReaderUpdater(TableReaderContainerUpdater* readerUpdater) override
    {
        if (mReaderUpdater) {
            return mReaderUpdater->RemoveReaderUpdater(readerUpdater);
        }
    }

    uint64_t GetPartitionIdentifier() const override { return mPartitionIdentifier; }
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
    void RedoOperations() override;

protected:
    // virtual for test
    virtual void InitReader();
    virtual OpenExecutorChainCreatorPtr CreateChainCreator()
    {
        return OpenExecutorChainCreatorPtr(new OpenExecutorChainCreator(mPartitionName, this));
    }
    virtual PartitionResourceCalculatorPtr
    CreateResourceCalculator(const config::IndexPartitionOptions& options,
                             const index_base::PartitionDataPtr& partitionData, const PartitionWriterPtr& writer,
                             const plugin::PluginManagerPtr& pluginManager) const;
    virtual void ReportMetrics();
    virtual void ExecuteCleanResourceTask(versionid_t loadedVersion);

    void DumpSegmentOverInterval();
    // todo
    bool NotAllowToModifyRootPath() const override { return !mOptions.GetOnlineConfig().IsPrimaryNode(); }
    void CheckParam(const config::IndexPartitionSchemaPtr& schema,
                    const config::IndexPartitionOptions& options) const override;

private:
    OpenStatus DoOpen(const std::string& primaryDirr, const std::string& secondaryDir,
                      const config::IndexPartitionSchemaPtr& schema, const config::IndexPartitionOptions& options,
                      versionid_t targetVersionId, IndexPartition::IndexPhase indexPhase);

    void DoInitReader(segmentid_t flushRtSegmentId);
    void InitWriter(const index_base::PartitionDataPtr& partData);
    void InitResourceCleaner();
    void InitResourceCalculator();

    void CleanResource();
    void UpdateOnlinePartitionMetrics();

    void LoadReaderPatch(const index_base::PartitionDataPtr& partitionData,
                         partition::OnlinePartitionReaderPtr& reader);

    void SwitchReader(const partition::OnlinePartitionReaderPtr& reader);

    void AddOnDiskIndexCleaner();

    void AddVirtualAttributeDataCleaner(const config::IndexPartitionSchemaPtr& newSchema);
    void CollectVirtualAttributes(const config::AttributeSchemaPtr& newSchema,
                                  std::vector<std::pair<std::string, bool>>& attrNames, bool isSub);

    config::IndexPartitionSchemaPtr CreateRtSchema(const config::IndexPartitionOptions& options);

    config::IndexPartitionSchemaPtr CreateNewSchema(const config::IndexPartitionSchemaPtr& schema,
                                                    const config::AttributeConfigVector& mainVirtualAttrConfigs,
                                                    const config::AttributeConfigVector& subVirtualAttrConfigs);
    bool HasSameVirtualAttrSchema(const config::IndexPartitionSchemaPtr& newSchema,
                                  const config::IndexPartitionSchemaPtr& oldSchema);
    void ResetRtAndJoinDirPath(const file_system::DirectoryPtr& dir);

    PartitionModifierPtr CreateReaderModifier(const IndexPartitionReaderPtr& reader);
    bool InitAccessCounters(const util::CounterMapPtr& counterMap, const config::IndexPartitionSchemaPtr& schema);
    void SetEnableReleaseOperationAfterDump(bool releaseOperations);
    ReopenDecider::ReopenType MakeReopenDecision(bool forceReopen, versionid_t targetVersionId,
                                                 index_base::Version& onDiskVersion);

    void DoClose();
    void Reset();
    bool PrepareIntervalTask();
    bool HasFlushRtIndex();
    void ResetCounter();

    std::shared_ptr<file_system::LifecycleTable> GetLifecycleTableFromVersion(const index_base::Version& version);
    void RefreshLifecycleProperty(const std::shared_ptr<file_system::LifecycleTable>& lifecycleTable);

private:
    typedef std::shared_ptr<autil::ScopedLock> ScopedLockPtr;

    IndexPartition::OpenStatus DoReopen(bool forceReopen, versionid_t targetVersionId);
    IndexPartition::OpenStatus NormalReopen(index_base::Version& onDiskVersion);
    OpenStatus LoadIncWithRt(index_base::Version& onDiskVersion);
    IndexPartition::OpenStatus ReclaimReaderMem();
    IndexPartition::OpenStatus SwitchFlushRtSegments();

    ExecutorResource CreateExecutorResource(const index_base::Version& onDiskVersion, bool forceReopen,
                                            bool needSnapshot = false);
    segmentid_t GetLatestLinkRtSegId(const index_base::PartitionDataPtr& partData,
                                     const util::MemoryReserverPtr& memReserver) const;

    void UpdateSwitchRtSegmentSize();

    int64_t GetLatestNeedSkipIncTs(const index_base::Version& incVersion) const;
    util::MemoryQuotaSynchronizerPtr
    CreateRealtimeMemoryQuotaSynchronizer(const util::MemoryQuotaControllerPtr& realtimeQuotaController);

    size_t EstimateAttributeExpandSize(const config::AttributeSchemaPtr& virtualAttrSchema,
                                       const config::AttributeConfigVector& attributeConfigs, size_t docCount);

    void CheckSecondIndex();
    bool NeedCheckSecondIndex() const
    {
        return mCheckSecondIndexIntervalInMin > 0 && mOptions.GetOnlineConfig().NeedReadRemoteIndex() &&
               !mOpenIndexSecondaryDir.empty();
    }
    void SubscribeSecondIndex();
    bool NeedSubscribeSecondIndex() const;
    file_system::IFileSystemPtr CreateFileSystem(const std::string& primaryDir,
                                                 const std::string& secondaryDir) override;
    void RewriteSchemaAndOptions(const config::IndexPartitionSchemaPtr& schema,
                                 const index_base::Version& onDiskVersion);

    bool RecoverFlushRtIndex();

    index_base::Version LoadOnDiskVersion(const std::string& workRoot, const std::string& sourceRoot,
                                          versionid_t targetVersionId);

    void GetRedoParam(index_base::PartitionDataPtr& partData, config::IndexPartitionSchemaPtr& schema,
                      util::SimpleMemoryQuotaControllerPtr& memController, PartitionModifierPtr& modifier,
                      OnlinePartitionReaderPtr& hintReader, bool isAboutToSwtchMaster);
    // return left cnt of operation from lastCursor
    uint64_t BatchRedoOpertions(const OperationRedoStrategyPtr& dumpRedoStrategy, OperationCursor& lastCursor,
                                OnlinePartitionReaderPtr& reader, bool isAboutToSwtchMaster);
    bool AddOneIntervalTask(const std::string& taskName, int32_t interval, OnlinePartitionTaskItem::TaskType taskType);
    void CalculateMetrics() const;

protected:
    config::IndexPartitionSchemaPtr mRtSchema;
    OnlinePartitionWriterPtr mWriter;

    mutable autil::ThreadMutex mReaderLock;
    mutable autil::RecursiveThreadMutex mSchemaLock;
    mutable autil::RecursiveThreadMutex mCleanerLock;
    mutable autil::RecursiveThreadMutex mDumpLock;
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
    RealtimeIndexSynchronizerPtr mRealtimeIndexSynchronizer;
    autil::ThreadPoolPtr mRealtimeIndexSyncThreadPool;
    volatile bool mClosed;
    volatile bool mIsServiceNormal;
    volatile IndexPartition::IndexPhase mIndexPhase;
    bool mEnableAsyncDump;

    std::vector<std::pair<std::string, int32_t>> mTaskIds;
    int64_t mLatestNeedSkipIncTs;
    uint64_t mPartitionIdentifier;
    uint64_t mOperationLogCatchUpThreshold;
    AsyncSegmentDumperPtr mAsyncSegmentDumper;
    VirtualAttributeContainerPtr mVirtualAttrContainer;
    SearchReaderUpdaterPtr mReaderUpdater;

    int64_t mCheckSecondIndexIntervalInMin;
    int64_t mSubscribeSecondIndexIntervalInMin;
    std::atomic<int64_t> mMissingSegmentCount;
    future_lite::Executor* mFutureExecutor;
    int64_t mRecoverMaxTs;
    bool mNeedReportTemperature;
    document::SrcSignature mSrcSignature;
    int64_t mMaxRedoTime;

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
}} // namespace indexlib::partition
