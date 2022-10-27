#ifndef __INDEXLIB_COMMON_SEGMENT_IN_MEMORY_SEGMENT_H
#define __INDEXLIB_COMMON_SEGMENT_IN_MEMORY_SEGMENT_H

#include <tr1/memory>
#include <autil/WorkItem.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/build_config.h"
#include "indexlib/index_base/segment/building_segment_data.h"

DECLARE_REFERENCE_CLASS(index_base, SegmentInfo);
DECLARE_REFERENCE_CLASS(index_base, SegmentContainer);
DECLARE_REFERENCE_CLASS(index, InMemorySegmentReader);
DECLARE_REFERENCE_CLASS(common, DumpItem);
DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(util, CounterMap);
DECLARE_REFERENCE_CLASS(util, BlockMemoryQuotaController);
DECLARE_REFERENCE_CLASS(util, MemoryReserver);
DECLARE_REFERENCE_CLASS(document, Document);

IE_NAMESPACE_BEGIN(util);

template<typename T>
class SimpleMemoryQuotaControllerType;
typedef SimpleMemoryQuotaControllerType<int64_t> UnsafeSimpleMemoryQuotaController;
DEFINE_SHARED_PTR(UnsafeSimpleMemoryQuotaController);

IE_NAMESPACE_END(util);

IE_NAMESPACE_BEGIN(index_base);

class InMemorySegment;
DEFINE_SHARED_PTR(InMemorySegment);
class SegmentWriter;
DEFINE_SHARED_PTR(SegmentWriter);

class InMemorySegment
{
public:
    InMemorySegment(const config::BuildConfig& buildConfig,
                    const util::BlockMemoryQuotaControllerPtr& memController,
                    const util::CounterMapPtr& counterMap);
    
    InMemorySegment(const InMemorySegment& other, BuildingSegmentData& segmentData);
    virtual ~InMemorySegment();
    
public:
    enum Status
    {
        BUILDING,
        WAITING_TO_DUMP,
        DUMPING,
        DUMPED
    };

public:
    void Init(const BuildingSegmentData& segmentData,
              const SegmentWriterPtr& segmentWriter,
              bool isSubSegment);    

    void Init(const BuildingSegmentData& segmentData, bool isSubSegment);
    Status GetStatus() const { return mStatus; } 
    void SetStatus(Status status)
    {
        mStatus = status;
        if (mSubInMemSegment)
        {
            mSubInMemSegment->SetStatus(status);
        }
    }

    //TODO: in sub doc, it's multi segment writer
    const SegmentWriterPtr& GetSegmentWriter() const
    { return mSegmentWriter; }

    //support operate sub 
    bool IsDirectoryDumped() const;
    void BeginDump();
    void EndDump();
    void UpdateSegmentInfo(const document::DocumentPtr& doc);

    const SegmentData& GetSegmentData() const { return mSegmentData; }

    const index::InMemorySegmentReaderPtr& GetSegmentReader() const
    { return mSegmentReader; }

    virtual const index_base::SegmentInfoPtr& GetSegmentInfo() const
    { return mSegmentInfo; }

    const file_system::DirectoryPtr& GetDirectory();

    exdocid_t GetBaseDocId() const;
    
    virtual segmentid_t GetSegmentId() const;
    
    timestamp_t GetTimestamp() const;

    void SetOperationWriter(const index_base::SegmentContainerPtr& operationWriter)
    { mOperationWriter = operationWriter; }
    
    index_base::SegmentContainerPtr GetOperationWriter() const
    { return mOperationWriter; }

    void SetSubInMemorySegment(InMemorySegmentPtr inMemorySegment);

    InMemorySegmentPtr GetSubInMemorySegment() const 
    { return mSubInMemSegment; }

    virtual size_t GetTotalMemoryUse() const;

    size_t GetDumpMemoryUse() const
    { return mDumpMemoryUse; }
    void UpdateMemUse();

    util::MemoryReserverPtr CreateMemoryReserver(const std::string& name);
    
    int64_t GetUsedQuota() const;

public:
    // for test
    void SetBaseDocId(exdocid_t docid);
    
private:
    //file_system::DirectoryPtr CreateDirectory();

    void EndSegment();

    void MultiThreadDump();

    void CreateDumpItems(std::vector<common::DumpItem*>& dumpItems);

private:
    SegmentWriterPtr mSegmentWriter;
    BuildingSegmentData mSegmentData;
    index_base::SegmentInfoPtr mSegmentInfo;
    index::InMemorySegmentReaderPtr mSegmentReader;
    index_base::SegmentContainerPtr mOperationWriter;
    InMemorySegmentPtr mSubInMemSegment;
    config::BuildConfig mBuildConfig;
    util::UnsafeSimpleMemoryQuotaControllerPtr mMemoryController;
    size_t mDumpMemoryUse;
    bool mIsSubSegment;
    util::CounterMapPtr mCounterMap;
    volatile Status mStatus;
private:
    friend class InMemoryPartitionDataTest;
    IE_LOG_DECLARE();
};

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_COMMON_SEGMENT_IN_MEMORY_SEGMENT_H
