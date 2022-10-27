#ifndef __INDEXLIB_NORMAL_SEGMENT_DUMP_ITEM_H
#define __INDEXLIB_NORMAL_SEGMENT_DUMP_ITEM_H

#include <tr1/memory>
#include "indexlib/misc/metric.h"
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/segment/segment_dump_item.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/segment_writer.h"

DECLARE_REFERENCE_CLASS(partition, PartitionModifier);
DECLARE_REFERENCE_CLASS(partition, FlushedLocatorContainer);
DECLARE_REFERENCE_CLASS(partition, OperationWriter);
DECLARE_REFERENCE_CLASS(index_base, SegmentWriter);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(index_base, InMemorySegment);
DECLARE_REFERENCE_CLASS(file_system, IndexlibFileSystem);
DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(util, MemoryReserver);

IE_NAMESPACE_BEGIN(partition);

class NormalSegmentDumpItem : public SegmentDumpItem
{
public:
    NormalSegmentDumpItem(
        const config::IndexPartitionOptions& options,
        const config::IndexPartitionSchemaPtr& schema,
        const std::string& partitionName);
    virtual ~NormalSegmentDumpItem();    
public:
    void Init(misc::MetricPtr mdumpSegmentLatencyMetric,
              const index_base::PartitionDataPtr& partitionData,
              const PartitionModifierPtr& modifier,
              const FlushedLocatorContainerPtr& flushedLocatorContainer,
              bool releaseOperationAfterDump);

    void Dump() override;
    bool DumpWithMemLimit() override;
    bool IsDumped() const override;    
    uint64_t GetEstimateDumpSize() const override;
    size_t GetTotalMemoryUse() const override;
    segmentid_t GetSegmentId() const override { return mInMemorySegment->GetSegmentId();}
    index_base::SegmentInfoPtr GetSegmentInfo() const override { return mSegmentWriter->GetSegmentInfo();}

public:
    index_base::InMemorySegmentPtr GetInMemorySegment() { return mInMemorySegment; }
    void SetReleaseOperationAfterDump(bool flag)
    { mReleaseOperationAfterDump = flag; }

private:
    struct MemoryStatusSnapShot
    {
        size_t fileSystemMemUse;
        size_t totalMemUse;
        size_t estimateDumpMemUse;
        size_t estimateDumpFileSize;
    };

private:
    MemoryStatusSnapShot TakeMemoryStatusSnapShot();
    bool CanDumpSegment(
        const util::MemoryReserverPtr& fileSystemMemReserver,
        const util::MemoryReserverPtr& dumpTmpMemReserver);
    size_t EstimateDumpMemoryUse() const;
    void PrintBuildResourceMetrics();
    void EndSegment();
    void CheckMemoryEstimation(MemoryStatusSnapShot& snapShot);
    size_t EstimateDumpFileSize() const;
    uint64_t GetInMemoryMemUse() const override { return mInMemorySegment->GetTotalMemoryUse(); }

private:
    void CleanResource();
    
private:
    misc::MetricPtr mdumpSegmentLatencyMetric;
    file_system::DirectoryPtr mDirectory;
    OperationWriterPtr mOperationWriter;
    PartitionModifierPtr mModifier;
    FlushedLocatorContainerPtr mFlushedLocatorContainer;
    index_base::SegmentWriterPtr mSegmentWriter;
    file_system::IndexlibFileSystemPtr mFileSystem;
    volatile bool mReleaseOperationAfterDump;
    uint64_t mEstimateDumpSize;
protected:
    // protected for test
    index_base::InMemorySegmentPtr mInMemorySegment;    
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(NormalSegmentDumpItem);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_NORMAL_SEGMENT_DUMP_ITEM_H
