#ifndef __INDEXLIB_OFFLINE_PARTITION_H
#define __INDEXLIB_OFFLINE_PARTITION_H

#include <tr1/memory>
#include <autil/Thread.h>
#include <autil/ThreadPool.h>
#include "indexlib/indexlib.h"
#include "indexlib/misc/monitor.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/index_base/index_meta/parallel_build_info.h"
#include "indexlib/partition/segment/in_memory_segment_cleaner.h"

DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(index_base, InMemorySegment);
DECLARE_REFERENCE_CLASS(partition, OfflinePartitionWriter);
DECLARE_REFERENCE_CLASS(partition, OfflinePartitionReader);
DECLARE_REFERENCE_CLASS(partition, InMemorySegmentContainer);
DECLARE_REFERENCE_CLASS(partition, DumpSegmentContainer);
DECLARE_REFERENCE_CLASS(partition, PartitionModifier);
DECLARE_REFERENCE_CLASS(plugin, PluginManager);
DECLARE_REFERENCE_CLASS(util, SearchCachePartitionWrapper);

IE_NAMESPACE_BEGIN(partition);

class OfflinePartition : public IndexPartition
{
public:
    OfflinePartition(const std::string& partitionName,
                     const util::MemoryQuotaControllerPtr& memQuotaController,
                     misc::MetricProviderPtr metricProvider = misc::MetricProviderPtr());

    OfflinePartition(const std::string& partitionName,
                     const partition::IndexPartitionResource& partitionResource);

    OfflinePartition(misc::MetricProviderPtr metricProvider = misc::MetricProviderPtr());
    ~OfflinePartition();

public:
    OpenStatus Open(const std::string& primaryDir,
                    const std::string& secondaryDir,
                    const config::IndexPartitionSchemaPtr& schema,
                    const config::IndexPartitionOptions& options,
                    versionid_t versionId = INVALID_VERSION) override;

    OpenStatus ReOpen(bool forceReopen,
                      versionid_t reopenVersionId = INVALID_VERSION) override;

    void ReOpenNewSegment() override;
    void Close() override;
    PartitionWriterPtr GetWriter() const override;
    IndexPartitionReaderPtr GetReader() const override;

private:
    file_system::IndexlibFileSystemPtr CreateFileSystem(
            const std::string& primaryDir, const std::string& secondaryDir) override;

private:
    OpenStatus DoOpen(const std::string& dir,
                      const config::IndexPartitionSchemaPtr& schema,
                      const config::IndexPartitionOptions& options,
                      versionid_t versionId);
    void StoreSchemaAndFormatVersionFile(const std::string& dir);
    void InitPartitionData(const std::string& dir,
                           const std::string& secondaryDir,
                           index_base::Version& version,
                           const util::CounterMapPtr& counterMap,
                           const plugin::PluginManagerPtr& pluginManager);
    void RewriteLoadConfigs(config::IndexPartitionOptions& options) const;
    void InitWriter(const index_base::PartitionDataPtr& partData);

    bool MakeRootDir(const std::string& rootDir);
    bool IsEmptyDir(const std::string& dir,
                    const config::IndexPartitionSchemaPtr& schema) const;

    void CheckParam(const config::IndexPartitionSchemaPtr& schema,
                    const config::IndexPartitionOptions& options);
    PartitionModifierPtr CreateModifier(const index_base::PartitionDataPtr& partData);
    // report metrics and release memory
    void BackGroundThread();
    void PushToReleasePool(index_base::InMemorySegmentPtr& inMemSegment);

    //virtual for test
    virtual OfflinePartitionWriterPtr CreatePartitionWriter();

    util::SearchCachePartitionWrapperPtr CreateSearchCacheWrapper() const;

    bool IsFirstIncParallelInstance() const
    {
        return mParallelBuildInfo.IsParallelBuild() && (mParallelBuildInfo.instanceId == 0);
    }

private:
    std::string mRootDir;
    OfflinePartitionWriterPtr mWriter;
    mutable partition::OfflinePartitionReaderPtr mReader;
    volatile bool mClosed;
    autil::ThreadPtr mBackGroundThreadPtr;
    DumpSegmentContainerPtr mDumpSegmentContainer;
    InMemorySegmentCleaner mInMemSegCleaner;
    index_base::ParallelBuildInfo mParallelBuildInfo;
    mutable autil::ThreadMutex mReaderLock;
    PartitionRange mPartitionRange;

    IE_DECLARE_METRIC(oldInMemorySegmentMemoryUse);
private:
    friend class OfflinePartitionTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OfflinePartition);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_OFFLINE_PARTITION_H
