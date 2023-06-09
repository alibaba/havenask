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
#ifndef __INDEXLIB_CUSTOM_OFFLINE_PARTITION_WRITER_H
#define __INDEXLIB_CUSTOM_OFFLINE_PARTITION_WRITER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/build_document_metrics.h"
#include "indexlib/partition/partition_writer.h"
#include "indexlib/partition/segment/custom_segment_dump_item.h"
#include "indexlib/util/metrics/MetricProvider.h"
#include "indexlib/util/metrics/Monitor.h"

DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(partition, CustomPartitionData);
DECLARE_REFERENCE_CLASS(partition, PartitionModifier);
DECLARE_REFERENCE_CLASS(partition, FlushedLocatorContainer);
DECLARE_REFERENCE_CLASS(partition, DumpSegmentQueue);
DECLARE_REFERENCE_CLASS(index_base, BuildingSegmentData);
DECLARE_REFERENCE_STRUCT(partition, NewSegmentMeta);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(config, IndexPartitionOptions);
DECLARE_REFERENCE_STRUCT(index_base, SegmentInfo);
DECLARE_REFERENCE_CLASS(file_system, Directory);
// TODO: @qingran package
// DECLARE_REFERENCE_CLASS(file_system, PackDirectory);
DECLARE_REFERENCE_CLASS(file_system, IFileSystem);
DECLARE_REFERENCE_CLASS(table, TableResource);
DECLARE_REFERENCE_CLASS(table, TableFactoryWrapper);
DECLARE_REFERENCE_CLASS(table, TableWriter);
DECLARE_REFERENCE_STRUCT(table, BuildSegmentDescription);
DECLARE_REFERENCE_CLASS(index_base, SegmentDataBase);
DECLARE_REFERENCE_CLASS(util, PartitionMemoryQuotaController);
DECLARE_REFERENCE_CLASS(util, BlockMemoryQuotaController);
DECLARE_REFERENCE_CLASS(plugin, PluginManager);
DECLARE_REFERENCE_CLASS(partition, CustomSegmentDumpItem);

namespace indexlib { namespace partition {

class CustomOfflinePartitionWriter : public PartitionWriter
{
public:
    CustomOfflinePartitionWriter(const config::IndexPartitionOptions& options,
                                 const table::TableFactoryWrapperPtr& tableFactory,
                                 const FlushedLocatorContainerPtr& flushedLocatorContainer,
                                 util::MetricProviderPtr metricProvider, const std::string& partitionName,
                                 const PartitionRange& range = PartitionRange());
    ~CustomOfflinePartitionWriter();

public:
    void Open(const config::IndexPartitionSchemaPtr& schema, const index_base::PartitionDataPtr& partitionData,
              const PartitionModifierPtr& modifier) override;
    void ReOpenNewSegment(const PartitionModifierPtr& modifier) override;
    bool BuildDocument(const document::DocumentPtr& doc) override;
    bool NeedDump(size_t memoryQuota, const document::DocumentCollector* docCollector = nullptr) const override;
    void DumpSegment() override;
    void Close() override;

    void DoClose();
    void Release();
    uint64_t GetTotalMemoryUse() const override;
    uint64_t GetFileSystemMemoryUse() const;
    uint64_t GetBuildingSegmentDumpExpandSize() const override;

    void ReportMetrics() const override;
    bool NeedRewriteDocument(const document::DocumentPtr& doc) override { return false; }
    util::Status GetStatus() const override { return mStatus; }
    bool EnableAsyncDump() const override { return mEnableAsyncDump; }

    const table::TableResourcePtr& GetTableResource() const { return mTableResource; }
    table::TableWriterPtr GetTableWriter()
    {
        autil::ScopedLock lock(mWriterLock);
        return mTableWriter;
    }
    std::string GetLastLocator() const;

    void ResetPartitionData(const index_base::PartitionDataPtr& partitionData,
                            const table::TableResourcePtr& tableResource);

    bool PrepareTableResource(const CustomPartitionDataPtr& partitionData, table::TableResourcePtr& tableResource);

    NewSegmentMetaPtr GetBuildingSegmentMeta() const;

private:
    // unsupported interface
    void RewriteDocument(const document::DocumentPtr& doc) override { assert(false); }
    void SetEnableReleaseOperationAfterDump(bool releaseOperations) override { assert(false); }

    // supported interface
    void RegisterMetrics(util::MetricProviderPtr mMetricProvider);
    void CleanResourceAfterDump();

    file_system::DirectoryPtr PrepareNewSegmentDirectory(const index_base::SegmentDataBasePtr& newSegmentData,
                                                         const config::BuildConfig& buildConfig);

    file_system::DirectoryPtr PrepareBuildingDirForTableWriter(const file_system::DirectoryPtr& segmentDir);

    table::TableWriterPtr InitTableWriter(const index_base::SegmentDataBasePtr& newSegmentData,
                                          const file_system::DirectoryPtr& dataDirectory,
                                          const table::TableResourcePtr& tableResource,
                                          const plugin::PluginManagerPtr& pluginManager);

    docid_t UpdateBuildingSegmentMeta(const document::DocumentPtr& doc);
    void StoreSegmentMetas(const index_base::SegmentInfoPtr& segmentInfo,
                           table::BuildSegmentDescription& segDescription);
    void CommitVersion(segmentid_t segmentId, const index_base::SegmentInfo& segmentInfo,
                       const table::BuildSegmentDescription& segDescription);

    bool IsDirty() const;
    void CleanFileSystemCache() const;

    size_t EstimateBuildMemoryUse() const;
    bool CheckMemoryLimit(size_t resourceMemUse, size_t writerMemUse) const;

    void ReportPartitionMetrics(const index_base::PartitionDataPtr& partitionData,
                                const NewSegmentMetaPtr& buildingSegMeta);

    bool HasResourceMemoryLimit() const { return mOptions.GetBuildConfig().buildResourceMemoryLimit >= 0; }

    void ResetNewSegmentData(const CustomPartitionDataPtr& partitionData);

protected:
    table::TableFactoryWrapperPtr mTableFactory;
    FlushedLocatorContainerPtr mFlushedLocatorContainer;
    util::MetricProviderPtr mMetricProvider;
    std::unique_ptr<BuildDocumentMetrics> mBuildDocumentMetrics;
    util::Status mStatus;

    config::IndexPartitionSchemaPtr mSchema;
    CustomPartitionDataPtr mPartitionData;
    file_system::DirectoryPtr mRootDir;
    file_system::IFileSystemPtr mFileSystem;
    table::TableResourcePtr mTableResource;
    table::TableWriterPtr mTableWriter;
    NewSegmentMetaPtr mBuildingSegMeta;
    file_system::DirectoryPtr mBuildingSegDir;
    PartitionModifierPtr mModifier;
    DumpSegmentQueuePtr mDumpSegmentQueue;

    util::BlockMemoryQuotaControllerPtr mTableWriterMemController;
    util::PartitionMemoryQuotaControllerPtr mPartitionMemController;
    mutable autil::RecursiveThreadMutex mPartDataLock;
    mutable autil::RecursiveThreadMutex mWriterLock;
    mutable autil::RecursiveThreadMutex mBuildingMetaLock;
    PartitionRange mPartitionRange;
    bool mEnableAsyncDump;

    IE_DECLARE_METRIC(partitionDocCount);
    IE_DECLARE_METRIC(segmentCount);
    IE_DECLARE_METRIC(TableWriterMemoryUse);
    IE_DECLARE_METRIC(TableResourceMemoryUse);
    IE_DECLARE_METRIC(dumpSegmentLatency);
    IE_DECLARE_METRIC(AliveTableWriterMemoryUse);

private:
    static constexpr double MEMORY_USE_RATIO = 0.9;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CustomOfflinePartitionWriter);
}} // namespace indexlib::partition

#endif //__INDEXLIB_CUSTOM_OFFLINE_PARTITION_WRITER_H
