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

#include <future>
#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/partition/reopen_decider.h"
#include "indexlib/partition/search_reader_updater.h"

namespace indexlibv2 {
class MemoryQuotaController;
namespace framework {
struct VersionDeployDescription;
typedef std::shared_ptr<VersionDeployDescription> VersionDeployDescriptionPtr;
} // namespace framework
} // namespace indexlibv2

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

namespace indexlib { namespace partition {

class CustomOnlinePartition : public IndexPartition
{
public:
    CustomOnlinePartition(const partition::IndexPartitionResource& partitionResource);
    ~CustomOnlinePartition();

public:
    OpenStatus Open(const std::string& primaryDir, const std::string& secondaryDir,
                    const config::IndexPartitionSchemaPtr& schema, const config::IndexPartitionOptions& options,
                    versionid_t targetVersionId = INVALID_VERSIONID) override;

    OpenStatus ReOpen(bool forceReopen, versionid_t targetVersionId = INVALID_VERSIONID) override;

    void ReOpenNewSegment() override;
    void Close() override;
    PartitionWriterPtr GetWriter() const override;
    IndexPartitionReaderPtr GetReader() const override;
    void SetForceSeekInfo(const std::pair<int64_t, int64_t>& forceSeekInfo) override;
    MemoryStatus CheckMemoryStatus() const override;
    MemoryStatus CheckMemoryWatermark() const;
    bool CleanIndexFiles(const std::vector<versionid_t>& keepVersionIds) override;
    bool CleanUnreferencedIndexFiles(const std::set<std::string>& toKeepFiles) override;
    int64_t GetRtSeekTimestampFromIncVersion() const override;
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
    OpenStatus DoOpen(const std::string& primaryDir, const std::string& secondaryDir,
                      const config::IndexPartitionSchemaPtr& schema, const config::IndexPartitionOptions& options,
                      versionid_t targetVersionId, bool isForceOpen);

    IndexPartition::OpenStatus DoReopen(bool forceReopen, versionid_t targetVersionId);
    IndexPartition::OpenStatus NormalReopen(ScopedLockPtr& dataLock, index_base::Version& incVersion,
                                            const indexlibv2::framework::VersionDeployDescriptionPtr& versionDpDesc);

    IndexPartition::OpenStatus
    NormalReopenWithSegmentLevelPreload(ScopedLockPtr& dataLock, index_base::Version& incVersion,
                                        const indexlibv2::framework::VersionDeployDescriptionPtr& versionDpDesc);

    CustomOnlinePartitionReaderPtr
    CreateDiffVersionReader(const index_base::Version& newVersion,
                            const indexlibv2::framework::VersionDeployDescriptionPtr& newVersionDpDesc,
                            const index_base::Version& lastVersion,
                            const indexlibv2::framework::VersionDeployDescriptionPtr& lastVersionDpDesc);

    ReopenDecider::ReopenType MakeReopenDecision(bool forceReopen, versionid_t targetVersionId,
                                                 index_base::Version& onDiskVersion);

    indexlibv2::framework::VersionDeployDescriptionPtr
    CreateVersionDeployDescription(const std::string& indexRoot, const indexlib::config::OnlineConfig& onlineConfig,
                                   versionid_t versionId);
    /* ExecutorChainPtr CreateReopenExecutorChain(const index_base::Version& onDiskVersion) const; */
    /* util::ExecutorBasePtr CreateDumpSegmentExecutor(); */
    /* util::ExecutorBasePtr CreateReclaimRtSegmentExecutor(); */
    /* util::ExecutorBasePtr CreateReleaseReaderExecutor(); */
    /* util::ExecutorBasePtr CreateReopenPartitionReaderExecutor(); */

    void DoClose();

    index_base::PartitionDataPtr
    CreateCustomPartitionData(const index_base::Version& incVersion, const index_base::Version& lastIncVersion,
                              const indexlibv2::framework::VersionDeployDescriptionPtr& versionDpDesc,
                              const DumpSegmentQueuePtr& dumpSegmentQueue, int64_t reclaimTimestamp,
                              bool ignoreInMemVersion) const;

    util::MemoryQuotaSynchronizerPtr CreateRealtimeMemoryQuotaSynchronizer(
        const std::shared_ptr<indexlibv2::MemoryQuotaController>& realtimeQuotaController);

    void CheckParam(const config::IndexPartitionSchemaPtr& schema,
                    const config::IndexPartitionOptions& options) const override;
    bool NotAllowToModifyRootPath() const override { return !mOptions.GetOnlineConfig().IsPrimaryNode(); }
    CustomOnlinePartitionReaderPtr InitReaderWithMemoryLimit(const index_base::PartitionDataPtr& partData,
                                                             const CustomOnlinePartitionWriterPtr& writer,
                                                             const index_base::Version& lastIncVersion,
                                                             const index_base::Version& newIncVersion);

    CustomOnlinePartitionReaderPtr InitReaderWithHeritage(const CustomOnlinePartitionWriterPtr& writer,
                                                          const index_base::PartitionDataPtr& partitionData,
                                                          const CustomOnlinePartitionReaderPtr& lastReader,
                                                          const CustomOnlinePartitionReaderPtr& preloadReader);

    CustomOnlinePartitionReaderPtr InitReaderWithFlushedSegments(segmentid_t latestLinkRtSegId);

    CustomOnlinePartitionReaderPtr CreateReaderWithUpdatedLifecycle(const CustomOnlinePartitionReaderPtr& lastReader);

    void InitWriter(const index_base::PartitionDataPtr& partData, CustomOnlinePartitionWriterPtr& writer);

    CustomOnlinePartitionReaderPtr InitReader(const CustomOnlinePartitionWriterPtr& writer,
                                              const index_base::PartitionDataPtr& partitionData);

    void SwitchReader(const CustomOnlinePartitionReaderPtr& reader);

    void InitResourceCalculator(const index_base::PartitionDataPtr& partData,
                                const CustomOnlinePartitionWriterPtr& writer);

    bool TryLock(autil::ThreadMutex& lock, size_t times, int64_t intervalInUs);
    void CheckSecondaryIndex();
    void CheckRealtimeBuild();
    bool NeedCheckSecondaryIndex() const
    {
        // TODO: online config 添加配置：表级别 > 环境变量 > -1
        return mCheckSecondaryIndexIntervalInMin > 0 && mOptions.GetOnlineConfig().NeedReadRemoteIndex() &&
               !mOpenIndexSecondaryDir.empty();
    }
    file_system::IFileSystemPtr CreateFileSystem(const std::string& primaryDir,
                                                 const std::string& secondaryDir) override;
    void ResetRtAndJoinDirPath(const file_system::IFileSystemPtr& fileSystem);
    void Reset();
    void InitResourceCleaner();
    void AddOnDiskIndexCleaner();
    void ExecuteCleanResourceTask(std::function<void()>);
    void ExecuteCleanPartitionReaderTask();
    void CleanResource();
    void UpdateOnlinePartitionMetrics();
    void ReportMetrics();
    void RefreshReader();
    void DoRefreshReader(bool needSync);
    bool PrepareIntervalTask();
    void UpdateReader(const CustomOnlinePartitionReaderPtr& preloadReader);
    std::set<segmentid_t>
    CalculateLifecycleUpdatedSegments(const index_base::PartitionDataPtr& newPartitionData,
                                      const CustomOnlinePartitionReaderPtr& oldPartitionReader) const;

    void CleanPartitionReaders();

    // return true means latest link rt segId is updated
    bool UpdateLatestLinkRtSegId(const index_base::PartitionDataPtr& partData, segmentid_t& latestLinkRtSegId) const;
    void UpdateSwitchRtSegmentSize();
    int64_t GetReclaimTimestamp(const index_base::Version& version);
    // only be called when reach rt mem limit
    void DumpSegmentWhenReachMemLimit();

public:
    void TEST_DeleteBackGroundTask();

private:
    static size_t DEFAULT_IO_EXCEPTION_RETRY_LIMIT;
    static size_t DEFAULT_IO_EXCEPTION_RETRY_INTERVAL;
    static size_t DEFAULT_CHECK_RT_BUILD_DUMP_LIMIT;
    static size_t DEFAULT_LOW_MEM_USED_WATERMARK;
    static size_t DEFAULT_HIGH_MEM_USED_WATERMARK;

private:
    volatile bool mClosed;
    volatile bool mIsServiceNormal;
    volatile IndexPartition::IndexPhase mIndexPhase;
    std::shared_ptr<indexlibv2::MemoryQuotaController> mRealtimeQuotaController;
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
    int32_t mRefreshReaderTaskId;
    int32_t mCheckIndexTaskId;
    int32_t mCheckRealtimeBuildTaskId;
    int64_t mCheckSecondaryIndexIntervalInMin;
    size_t mHighMemUsedWatermark;
    size_t mLowMemUsedWatermark;
    size_t mIOExceptionRetryLimit;
    size_t mIOExceptionRetryInterval;
    std::atomic<int64_t> mMissingSegmentCount;
    size_t mCheckRtBuildDumpLimit;
    std::future<void> mRefreshReaderFuture;
    util::BlockMemoryQuotaControllerPtr mTableWriterMemController;
    bool mSupportSegmentLevelReader;
    bool mNeedCheckRealTimeBuild;
    bool mEnableAsyncDump;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CustomOnlinePartition);
}} // namespace indexlib::partition
