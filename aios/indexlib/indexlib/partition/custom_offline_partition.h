#ifndef __INDEXLIB_CUSTOM_OFFLINE_PARTITION_H
#define __INDEXLIB_CUSTOM_OFFLINE_PARTITION_H

#include <tr1/memory>
#include <autil/Thread.h>
#include <autil/ThreadPool.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/index_base/index_meta/parallel_build_info.h"

DECLARE_REFERENCE_CLASS(partition, CustomOfflinePartitionWriter);
DECLARE_REFERENCE_CLASS(table, TableFactoryWrapper);
DECLARE_REFERENCE_CLASS(index_base, PartitionMeta);
DECLARE_REFERENCE_CLASS(partition, DumpSegmentQueue);
DECLARE_REFERENCE_CLASS(util, BlockMemoryQuotaController);

IE_NAMESPACE_BEGIN(partition);

class CustomOfflinePartition : public IndexPartition
{
public:
    CustomOfflinePartition(const std::string& partitionName,
                           const partition::IndexPartitionResource& partitionResource);
    ~CustomOfflinePartition();
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
    std::string GetLastLocator() const override;


private:
    OpenStatus DoOpen(const std::string& primaryDir,
                      const config::IndexPartitionSchemaPtr& schema, 
                      const config::IndexPartitionOptions& options,
                      versionid_t versionId);

    bool InitPlugin(const std::string& pluginPath,
                    const config::IndexPartitionSchemaPtr& schema,
                    const config::IndexPartitionOptions& options);

    bool MakeRootDir(const std::string& rootDir);    
    void CleanIndexDir(const std::string& dir,
                       const config::IndexPartitionSchemaPtr& schema,
                       bool& isEmptyDir) const;
    void ValidateConfigAndParam(const config::IndexPartitionSchemaPtr& schemaInConfig,
                                const config::IndexPartitionSchemaPtr& schemaOnDisk,
                                const config::IndexPartitionOptions& options);
    void InitEmptyIndexDir(const std::string& dir);
    
    bool ValidateSchema(const config::IndexPartitionSchemaPtr& schema);
    bool InitPartitionData(const file_system::IndexlibFileSystemPtr& fileSystem,
                           index_base::Version& version,
                           const util::CounterMapPtr& counterMap,
                           const plugin::PluginManagerPtr& pluginManager);

    bool ValidatePartitionMeta(const index_base::PartitionMeta& partitionMeta) const;

    void InitWriter(const index_base::PartitionDataPtr& partData);
    void BackGroundThread();
    bool IsFirstIncParallelInstance() const
    {
        return mParallelBuildInfo.IsParallelBuild() && (mParallelBuildInfo.instanceId == 0);
    }
private:
    std::string mRootDir;
    index_base::ParallelBuildInfo mParallelBuildInfo;
    volatile bool mClosed;
    bool mDisableBackgroundThread;
    table::TableFactoryWrapperPtr mTableFactory;
    CustomOfflinePartitionWriterPtr mWriter;
    DumpSegmentQueuePtr mDumpSegmentQueue;
    autil::ThreadPtr mBackGroundThreadPtr;
    PartitionRange mPartitionRange;
    util::BlockMemoryQuotaControllerPtr mTableWriterMemController;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CustomOfflinePartition);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_CUSTOM_OFFLINE_PARTITION_H
