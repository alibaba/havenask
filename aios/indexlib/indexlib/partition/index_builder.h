#ifndef __INDEXLIB_PARTITION_INDEX_BUILDER_H
#define __INDEXLIB_PARTITION_INDEX_BUILDER_H

#include <tr1/memory>
#include "indexlib/misc/metric_provider.h"
#include <atomic>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/partition/index_builder_metrics.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/index_base/index_meta/parallel_build_info.h"
#include "indexlib/index_base/index_meta/version.h"

DECLARE_REFERENCE_CLASS(partition, IndexPartition);
DECLARE_REFERENCE_CLASS(partition, PartitionWriter);
DECLARE_REFERENCE_CLASS(partition, IndexPartitionResource);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(document, Document);
DECLARE_REFERENCE_CLASS(util, QuotaControl);
DECLARE_REFERENCE_CLASS(util, CounterMap);

IE_NAMESPACE_BEGIN(partition);

class IndexBuilder
{
public:
    IndexBuilder(
        const std::string& rootDir,
        const config::IndexPartitionOptions& options,
        const config::IndexPartitionSchemaPtr& schema,
        const util::QuotaControlPtr& memoryQuotaControl,
        misc::MetricProviderPtr metricProvider = misc::MetricProviderPtr(),
        const std::string& indexPluginPath = "",
        const PartitionRange& range = PartitionRange());

    IndexBuilder(
        const std::string& rootDir,
        const index_base::ParallelBuildInfo& parallelInfo,
        const config::IndexPartitionOptions& options,
        const config::IndexPartitionSchemaPtr& schema,
        const util::QuotaControlPtr& memoryQuotaControl,
        misc::MetricProviderPtr metricProvider = misc::MetricProviderPtr(),
        const std::string& indexPluginPath = "",
        const PartitionRange& range = PartitionRange());

    IndexBuilder(
        const IndexPartitionPtr& indexPartition,
        const util::QuotaControlPtr& memoryQuotaControl,
        misc::MetricProviderPtr metricProvider = misc::MetricProviderPtr(),
        const PartitionRange& range = PartitionRange());

    virtual ~IndexBuilder();

public:
    // virtual these methods for test    
    virtual bool Init();
    virtual bool Build(const document::DocumentPtr& doc);
    virtual void EndIndex(int64_t versionTimestamp = INVALID_TIMESTAMP);
    virtual versionid_t Merge(
            const config::IndexPartitionOptions& options,
            bool optimize = false,
            int64_t currentTs = 0);
    virtual std::string GetLastLocator() const;
    // get last segment locator in latest on disk version
    virtual std::string GetLocatorInLatestVersion() const;
    virtual std::string GetLastFlushedLocator();
    virtual void DumpSegment();
    virtual bool IsDumping();

    size_t GetLastConsumedMessageCount() const { return mLastConsumedMessageCount; }

    int64_t GetIncVersionTimestamp() const;
    const IndexPartitionPtr &GetIndexPartition() const;

    const IndexBuilderMetrics& GetBuilderMetrics()
    { return mBuilderMetrics; }

    versionid_t GetVersion();
    virtual const util::CounterMapPtr& GetCounterMap();
    
    // TODO: remove below, will not return false
    bool GetLastLocator(std::string& locator) const;
    bool GetIncVersionTimestamp(int64_t& ts) const;
    
public:
    // for test
    autil::ThreadMutex *GetLock() const
    {
        return mDataLock;
    }
    bool GetIsLegacyIndexVersion() const
    {
        return mIsLegacyIndexVersion;
    }
    void SetIsLegacyIndexVersion(bool isLegacyIndexVersion)
    {
        mIsLegacyIndexVersion = isLegacyIndexVersion;
    }
    config::IndexPartitionOptions TEST_GET_INDEX_PARTITION_OPTIONS()
    {
        return mOptions;
    }
    const index_base::ParallelBuildInfo& TEST_GET_PARALLEL_BUILD_INFO() const
    {
        return mParallelInfo;
    }
    
private:
    IndexPartitionPtr CreateIndexPartition(const config::IndexPartitionSchemaPtr& schema,
                                           const std::string& partitionName,
                                           const config::IndexPartitionOptions& options,
                                           const IndexPartitionResource& indexPartitionResource);
    bool BuildDocument(const document::DocumentPtr& doc);
    versionid_t DoMerge(
            const config::IndexPartitionOptions& options,
            bool optimize,
            int64_t currentTs);
    void ClosePartition();
    index_base::Version GetLatestOnDiskVersion() const;
    void CreateEmptyVersion(int64_t versionTimestamp);


    void AlignVersionTimestamp(const file_system::DirectoryPtr& rootDir,
                               const index_base::Version& latestVersion,
                               int64_t alignedTimestamp);
    
    //Attention: trie index not support realtime build
    bool SupportOnlineBuild(const config::IndexPartitionSchemaPtr& schema);
    bool CreateInstanceDirectoryForParallelInc();
    
private:
    enum Status
    {
        INITIALIZING = 0,
        BUILDING = 1,
        DUMPING = 2,
        ENDINDEXING = 3,
        MERGING = 4,
    };

private:
    autil::ThreadMutex *mDataLock;
    config::IndexPartitionOptions mOptions;
    config::IndexPartitionSchemaPtr mSchema; // hold schema for partition openForWrite
    util::QuotaControlPtr mMemoryQuotaControl;
    IndexPartitionPtr mIndexPartition;
    partition::PartitionWriterPtr mIndexPartitionWriter;
    IndexBuilderMetrics mBuilderMetrics;
    misc::MetricProviderPtr mMetricProvider;
    std::string mRootDir;
    index_base::PartitionMeta mPartitionMeta;
    bool mIsLegacyIndexVersion; // for locator compatibility
    //Attention: trie index not support realtime build
    bool mSupportBuild;
    std::atomic<Status> mStatus;
    std::string mIndexPluginPath;
    index_base::ParallelBuildInfo mParallelInfo;
    PartitionRange mPartitionRange;
    size_t mLastConsumedMessageCount;

private:
    friend class IndexBuilderTest;
    friend class TruncateIndexTest;
    friend class IndexlibExceptionPerfTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexBuilder);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_PARTITION_INDEX_BUILDER_H
