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
#ifndef __INDEXLIB_CUSTOM_OFFLINE_PARTITION_H
#define __INDEXLIB_CUSTOM_OFFLINE_PARTITION_H

#include <memory>

#include "autil/Thread.h"
#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/parallel_build_info.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/index_partition.h"

DECLARE_REFERENCE_CLASS(partition, CustomOfflinePartitionWriter);
DECLARE_REFERENCE_CLASS(table, TableFactoryWrapper);
DECLARE_REFERENCE_CLASS(partition, DumpSegmentQueue);
DECLARE_REFERENCE_CLASS(util, BlockMemoryQuotaController);

namespace indexlib { namespace partition {

class CustomOfflinePartition : public IndexPartition
{
public:
    CustomOfflinePartition(const partition::IndexPartitionResource& partitionResource);
    ~CustomOfflinePartition();

public:
    OpenStatus Open(const std::string& primaryDir, const std::string& secondaryDir,
                    const config::IndexPartitionSchemaPtr& schema, const config::IndexPartitionOptions& options,
                    versionid_t versionId = INVALID_VERSION) override;

    OpenStatus ReOpen(bool forceReopen, versionid_t reopenVersionId = INVALID_VERSION) override;

    void ReOpenNewSegment() override;
    void Close() override;
    PartitionWriterPtr GetWriter() const override;
    IndexPartitionReaderPtr GetReader() const override;
    std::string GetLastLocator() const override;
    void ExecuteBuildMemoryControl() override;

private:
    OpenStatus DoOpen(const std::string& primaryDir, const config::IndexPartitionSchemaPtr& schema,
                      const config::IndexPartitionOptions& options, versionid_t versionId);

    bool InitPlugin(const std::string& pluginPath, const config::IndexPartitionSchemaPtr& schema,
                    const config::IndexPartitionOptions& options);

    // void CleanIndexDir(const std::string& root, const config::IndexPartitionSchemaPtr& schema, bool& isEmptyDir)
    // const;
    void ValidateConfigAndParam(const config::IndexPartitionSchemaPtr& schemaInConfig,
                                const config::IndexPartitionSchemaPtr& schemaOnDisk,
                                const config::IndexPartitionOptions& options);

    bool ValidateSchema(const config::IndexPartitionSchemaPtr& schema);
    bool InitPartitionData(const file_system::IFileSystemPtr& fileSystem, index_base::Version& version,
                           const util::CounterMapPtr& counterMap, const plugin::PluginManagerPtr& pluginManager);

    bool ValidatePartitionMeta(const index_base::PartitionMeta& partitionMeta) const;

    void InitWriter(const index_base::PartitionDataPtr& partData);
    void BackGroundThread();
    bool IsFirstIncParallelInstance() const
    {
        return mParallelBuildInfo.IsParallelBuild() && (mParallelBuildInfo.instanceId == 0);
    }
    bool PrepareIntervalTask();
    void CleanIntervalTask();
    void AsyncDumpThread();
    void FlushDumpSegmentQueue();
    int64_t GetAllowedMemoryInBytes() const;
    bool NeedReclaimMemory() const;
    bool NotAllowToModifyRootPath() const override
    {
        return mParallelBuildInfo.IsParallelBuild() && (!IsFirstIncParallelInstance() || mHinterOption.branchId);
    }
    void CheckParam(const config::IndexPartitionSchemaPtr& schema,
                    const config::IndexPartitionOptions& options) const override
    {
    }

private:
    constexpr static uint32_t DEFAULT_OFFLINE_MEMORY_FACTOR = 2;
    constexpr static double DEFAULT_MACHINE_MEMORY_RATIO = 0.9;

private:
    index_base::ParallelBuildInfo mParallelBuildInfo;
    volatile bool mClosed;
    bool mDisableBackgroundThread;
    PartitionRange mPartitionRange;
    int64_t mMemLimitInBytes;
    bool mEnableAsyncDump;
    mutable autil::RecursiveThreadMutex mAsyncDumpLock;

    table::TableFactoryWrapperPtr mTableFactory;
    CustomOfflinePartitionWriterPtr mWriter;
    DumpSegmentQueuePtr mDumpSegmentQueue;
    autil::ThreadPtr mAsyncDumpThreadPtr;
    autil::ThreadPtr mBackGroundThreadPtr;
    util::BlockMemoryQuotaControllerPtr mTableWriterMemController;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CustomOfflinePartition);
}} // namespace indexlib::partition

#endif //__INDEXLIB_CUSTOM_OFFLINE_PARTITION_H
