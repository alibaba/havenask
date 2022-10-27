#ifndef __INDEXLIB_CUSTOM_SEGMENT_DUMP_ITEM_H
#define __INDEXLIB_CUSTOM_SEGMENT_DUMP_ITEM_H

#include <tr1/memory>
#include "indexlib/misc/metric.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/segment/segment_dump_item.h"
#include "indexlib/table/table_common.h"

DECLARE_REFERENCE_CLASS(partition, PartitionModifier);
DECLARE_REFERENCE_CLASS(partition, FlushedLocatorContainer);
DECLARE_REFERENCE_CLASS(partition, OperationWriter);
DECLARE_REFERENCE_CLASS(partition, CustomPartitionData);
DECLARE_REFERENCE_CLASS(index_base, SegmentWriter);
DECLARE_REFERENCE_CLASS(index_base, InMemorySegment);
DECLARE_REFERENCE_CLASS(file_system, IndexlibFileSystem);
DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(util, MemoryReserver);
DECLARE_REFERENCE_CLASS(table, TableWriter);
DECLARE_REFERENCE_CLASS(partition, NewSegmentMeta);
DECLARE_REFERENCE_CLASS(index_base, SegmentInfo);

IE_NAMESPACE_BEGIN(partition);

class CustomSegmentDumpItem : public SegmentDumpItem
{
public:
    CustomSegmentDumpItem(
        const config::IndexPartitionOptions& options,
        const config::IndexPartitionSchemaPtr& schema,
        const std::string& partitionName);
    ~CustomSegmentDumpItem();
public:

    void Init(misc::MetricPtr dumpSegmentLatencyMetric,
              const file_system::DirectoryPtr& rootDir,
              const PartitionModifierPtr& modifier,
              const FlushedLocatorContainerPtr& flushedLocatorContainer,
              bool releaseOperationAfterDump,
              const NewSegmentMetaPtr& buildingSegMeta,
              const file_system::DirectoryPtr& buildingSegDir,
              const file_system::PackDirectoryPtr& buildingSegPackDir,
              const table::TableWriterPtr& tableWriter);
    

public:
    void Dump() override;
    bool DumpWithMemLimit() override { assert(false); return false; }
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
    misc::MetricPtr mDumpSegmentLatencyMetric;
    CustomPartitionDataPtr mPartitionData;
    PartitionModifierPtr mModifier;
    FlushedLocatorContainerPtr mFlushedLocatorContainer;
    file_system::DirectoryPtr mDirectory;
    file_system::IndexlibFileSystemPtr mFileSystem;    
    volatile bool mReleaseOperationAfterDump;    
    uint64_t mEstimateDumpSize;
    table::BuildSegmentDescription mSegDescription;
    NewSegmentMetaPtr mBuildingSegMeta;
    file_system::DirectoryPtr mBuildingSegDir; 
    file_system::PackDirectoryPtr mBuildingSegPackDir;
    table::TableWriterPtr mTableWriter;
    std::atomic<bool> mIsDumped;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CustomSegmentDumpItem);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_CUSTOM_SEGMENT_DUMP_ITEM_H
