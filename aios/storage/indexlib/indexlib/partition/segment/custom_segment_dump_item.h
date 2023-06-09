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
#ifndef __INDEXLIB_CUSTOM_SEGMENT_DUMP_ITEM_H
#define __INDEXLIB_CUSTOM_SEGMENT_DUMP_ITEM_H

#include <memory>

#include "indexlib/indexlib.h"
#include "indexlib/partition/segment/segment_dump_item.h"
#include "indexlib/table/table_common.h"
#include "indexlib/util/metrics/Metric.h"

DECLARE_REFERENCE_CLASS(partition, PartitionModifier);
DECLARE_REFERENCE_CLASS(partition, FlushedLocatorContainer);
DECLARE_REFERENCE_CLASS(partition, OperationWriter);
DECLARE_REFERENCE_CLASS(index_base, SegmentWriter);
DECLARE_REFERENCE_CLASS(index_base, InMemorySegment);
DECLARE_REFERENCE_CLASS(file_system, IFileSystem);
DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(util, MemoryReserver);
DECLARE_REFERENCE_CLASS(table, TableWriter);
DECLARE_REFERENCE_STRUCT(partition, NewSegmentMeta);
DECLARE_REFERENCE_STRUCT(index_base, SegmentInfo);

namespace indexlib { namespace partition {

class CustomSegmentDumpItem : public SegmentDumpItem
{
public:
    CustomSegmentDumpItem(const config::IndexPartitionOptions& options, const config::IndexPartitionSchemaPtr& schema,
                          const std::string& partitionName);
    ~CustomSegmentDumpItem();

public:
    void Init(util::MetricPtr dumpSegmentLatencyMetric, const file_system::DirectoryPtr& rootDir,
              const PartitionModifierPtr& modifier, const FlushedLocatorContainerPtr& flushedLocatorContainer,
              bool releaseOperationAfterDump, const NewSegmentMetaPtr& buildingSegMeta,
              const file_system::DirectoryPtr& buildingSegDir,
              // TODO: @qingran package
              // const file_system::PackDirectoryPtr& buildingSegPackDir,
              const table::TableWriterPtr& tableWriter);

public:
    void Dump() override;
    bool DumpWithMemLimit() override
    {
        assert(false);
        return false;
    }
    bool IsDumped() const override { return mIsDumped.load(); }
    uint64_t GetInMemoryMemUse() const override;
    uint64_t GetEstimateDumpSize() const override { return mEstimateDumpSize; }
    size_t GetTotalMemoryUse() const override;
    segmentid_t GetSegmentId() const override;
    index_base::SegmentInfoPtr GetSegmentInfo() const override;

public:
    table::TableWriterPtr GetTableWriter() const { return mTableWriter; }
    partition::NewSegmentMetaPtr GetSegmentMeta() const { return mBuildingSegMeta; };
    table::BuildSegmentDescription GetSegmentDescription() const { return mSegDescription; }

private:
    void StoreSegmentMetas(const index_base::SegmentInfoPtr& segmentInfo,
                           table::BuildSegmentDescription& segDescription);

    size_t EstimateDumpMemoryUse() const;

    size_t EstimateDumpFileSize() const;

private:
    util::MetricPtr mDumpSegmentLatencyMetric;
    PartitionModifierPtr mModifier;
    FlushedLocatorContainerPtr mFlushedLocatorContainer;
    file_system::DirectoryPtr mDirectory;
    file_system::IFileSystemPtr mFileSystem;
    volatile bool mReleaseOperationAfterDump;
    uint64_t mEstimateDumpSize;
    table::BuildSegmentDescription mSegDescription;
    NewSegmentMetaPtr mBuildingSegMeta;
    file_system::DirectoryPtr mBuildingSegDir;
    table::TableWriterPtr mTableWriter;
    std::atomic<bool> mIsDumped;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CustomSegmentDumpItem);
}} // namespace indexlib::partition

#endif //__INDEXLIB_CUSTOM_SEGMENT_DUMP_ITEM_H
