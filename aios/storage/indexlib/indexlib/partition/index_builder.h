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
#ifndef __INDEXLIB_PARTITION_INDEX_BUILDER_H
#define __INDEXLIB_PARTITION_INDEX_BUILDER_H

#include <atomic>
#include <memory>

#include "autil/Thread.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/index_base/branch_fs.h"
#include "indexlib/index_base/index_meta/parallel_build_info.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/builder_branch_hinter.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/partition/partition_writer.h"
#include "indexlib/util/metrics/MetricProvider.h"

DECLARE_REFERENCE_CLASS(partition, IndexPartitionResource);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(document, Document);
DECLARE_REFERENCE_CLASS(document, DocumentCollector);
DECLARE_REFERENCE_CLASS(util, QuotaControl);
DECLARE_REFERENCE_CLASS(util, CounterMap);
DECLARE_REFERENCE_CLASS(util, Clock);

namespace indexlib { namespace partition {

class IndexBuilder
{
public:
    IndexBuilder(const std::string& rootDir, const config::IndexPartitionOptions& options,
                 const config::IndexPartitionSchemaPtr& schema, const util::QuotaControlPtr& memoryQuotaControl,
                 const BuilderBranchHinter::Option& branchOption,
                 util::MetricProviderPtr metricProvider = util::MetricProviderPtr(),
                 const std::string& indexPluginPath = "", const PartitionRange& range = PartitionRange());

    IndexBuilder(const std::string& rootDir, const index_base::ParallelBuildInfo& parallelInfo,
                 const config::IndexPartitionOptions& options, const config::IndexPartitionSchemaPtr& schema,
                 const util::QuotaControlPtr& memoryQuotaControl, const BuilderBranchHinter::Option& branchOption,
                 util::MetricProviderPtr metricProvider = util::MetricProviderPtr(),
                 const std::string& indexPluginPath = "", const PartitionRange& range = PartitionRange());

    IndexBuilder(const IndexPartitionPtr& indexPartition, const util::QuotaControlPtr& memoryQuotaControl,
                 util::MetricProviderPtr metricProvider = util::MetricProviderPtr(),
                 const PartitionRange& range = PartitionRange());

    virtual ~IndexBuilder();

public:
    // virtual these methods for test
    virtual bool Init();

    virtual bool Build(const document::DocumentPtr& doc);

    // 使此前所有调用过 Build() 且校验成功的 doc 可查。
    // 只有之前调用 Build() 时有返回过 BUILD_RESULT_PENDING，调用 Flush() 才有意义
    virtual void Flush();

    // 当前 builder 启动时默认为 InconsistentBatch 模式。模式含义查看 enum PartitionWriter::BuildMode 的注释
    virtual void SwitchToConsistentMode();

    virtual void EndIndex(int64_t versionTimestamp = INVALID_TIMESTAMP);
    virtual versionid_t Merge(const config::IndexPartitionOptions& options, bool optimize = false,
                              int64_t currentTs = 0);
    virtual std::string GetLastLocator() const;
    // get last segment locator in latest on disk version
    virtual std::string GetLocatorInLatestVersion() const;
    virtual std::string GetLastFlushedLocator();
    virtual void DumpSegment();
    virtual bool IsDumping();
    size_t GetLastConsumedMessageCount() const { return mLastConsumedMessageCount; }

    int64_t GetIncVersionTimestamp() const;
    const IndexPartitionPtr& GetIndexPartition() const;

    versionid_t GetVersion();
    virtual const util::CounterMapPtr& GetCounterMap();
    void ReclaimBuildMemory();

    // TODO: remove below, will not return false
    bool GetLastLocator(std::string& locator) const;
    bool GetIncVersionTimestamp(int64_t& ts) const;

    void StoreBranchCkp();

public:
    // for test
    autil::ThreadMutex* GetLock() const { return mDataLock; }
    bool GetIsLegacyIndexVersion() const { return mIsLegacyIndexVersion; }
    void SetIsLegacyIndexVersion(bool isLegacyIndexVersion) { mIsLegacyIndexVersion = isLegacyIndexVersion; }
    config::IndexPartitionOptions TEST_GET_INDEX_PARTITION_OPTIONS() { return mOptions; }
    const index_base::ParallelBuildInfo& TEST_GET_PARALLEL_BUILD_INFO() const { return mParallelInfo; }
    file_system::DirectoryPtr TEST_GetRootDirectory() const;
    void TEST_SetClock(std::unique_ptr<util::Clock> clock);
    index_base::BranchFS* GetBranchFs();
    void TEST_BranchFSMoveToMainRoad();

private:
    void InitWriter();
    bool IsEnableBatchBuild(bool isConsistentMode);
    bool BatchBuild(const document::DocumentPtr& doc);
    void MaybeDoBatchBuild();

    IndexPartitionPtr CreateIndexPartition(const config::IndexPartitionSchemaPtr& schema,
                                           const config::IndexPartitionOptions& options,
                                           const IndexPartitionResource& indexPartitionResource);
    bool MaybePrepareBuilder();
    void MaybePrepareDoc(const document::DocumentPtr& doc);
    bool BuildSingleDocument(const document::DocumentPtr& doc);
    bool DoBuildSingleDocument(const document::DocumentPtr& doc);
    // virtual for test
    virtual bool DoBatchBuild();
    void DoDumpSegment();

    versionid_t DoMerge(const std::string& rootPath, const config::IndexPartitionSchemaPtr& schema,
                        const config::IndexPartitionOptions& options, bool optimize, int64_t currentTs);
    void ClosePartition();
    index_base::Version GetLatestOnDiskVersion();
    void CreateEmptyVersion(const file_system::DirectoryPtr& dir, int64_t versionTimestamp);

    void AlignVersionTimestamp(const file_system::DirectoryPtr& rootDir, const index_base::Version& latestVersion,
                               int64_t alignedTimestamp);

    // Attention: trie index not support realtime build
    bool SupportOnlineBuild(const config::IndexPartitionSchemaPtr& schema);
    bool CreateInstanceDirectoryForParallelInc();

    bool CreateMemoryControlThread();
    void CleanMemoryControlThread();
    void MemoryControlThread();
    void RewriteOfflineBuildOptions();
    bool IsEndIndex();

private:
    static const int32_t MEMORY_CTRL_THREAD_INTERVAL = 5 * 1000 * 1000; // 5s

    enum Status {
        INITIALIZING = 0,
        BUILDING = 1,
        DUMPING = 2,
        ENDINDEXING = 3,
        MERGING = 4,
    };

private:
    autil::ThreadMutex* mDataLock;
    config::IndexPartitionOptions mOptions;
    config::IndexPartitionSchemaPtr mSchema; // hold schema for partition openForWrite
    util::QuotaControlPtr mMemoryQuotaControl;
    IndexPartitionPtr mIndexPartition;
    partition::PartitionWriterPtr mIndexPartitionWriter;
    util::MetricProviderPtr mMetricProvider;
    std::string mRootDir;
    index_base::PartitionMeta mPartitionMeta;
    bool mIsLegacyIndexVersion; // for locator compatibility
    // Attention: trie index not support realtime build
    bool mSupportBuild;
    std::atomic<Status> mStatus;
    std::string mIndexPluginPath;
    index_base::ParallelBuildInfo mParallelInfo;
    PartitionRange mPartitionRange;
    size_t mLastConsumedMessageCount;
    volatile bool mIsMemoryCtrlRunning;
    autil::ThreadPtr mMemoryCtrlThread;
    BuilderBranchHinterPtr mBranchHinter;

    uint64_t _batchStartTimestampUS;
    // _clock is a member in order to inject time in unit test.
    std::unique_ptr<util::Clock> _clock;
    std::unique_ptr<document::DocumentCollector> _docCollector;

private:
    friend class IndexBuilderTest;
    friend class TruncateIndexTest;
    friend class IndexlibExceptionPerfTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexBuilder);
}} // namespace indexlib::partition

#endif //__INDEXLIB_PARTITION_INDEX_BUILDER_H
