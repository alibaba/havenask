#ifndef __INDEXLIB_DEMO_TABLE_WRITER_H
#define __INDEXLIB_DEMO_TABLE_WRITER_H

#include <tr1/memory>
#include <autil/Lock.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/table/table_writer.h"

IE_NAMESPACE_BEGIN(table);

class DemoTableWriter : public TableWriter
{
public:
    DemoTableWriter(const util::KeyValueMap& parameters);
    ~DemoTableWriter();
public:
    bool DoInit() override;
    BuildResult Build(docid_t docId, const document::DocumentPtr& doc) override;
    bool IsDirty() const override;
    bool DumpSegment(BuildSegmentDescription& segmentDescription) override;
    BuildingSegmentReaderPtr CreateBuildingSegmentReader(const util::UnsafeSimpleMemoryQuotaControllerPtr&) override;
    size_t GetLastConsumedMessageCount() const override { return 1u; }    

public:
    size_t EstimateInitMemoryUse(const TableWriterInitParamPtr& initParam) const override;
    size_t GetCurrentMemoryUse() const override;
    size_t EstimateDumpMemoryUse() const override; 
    size_t EstimateDumpFileSize() const override;
public:
    static std::string DATA_FILE_NAME;
    static std::string MILESTONE_FILE_NAME;
    
private:
    std::shared_ptr<autil::ThreadMutex> mDataLock;    
    std::shared_ptr<std::map<std::string, std::string>> mData;
    bool mIsDirty;
    bool mIsOnline;
    int64_t mRtFilterTimestamp;
    size_t mThreshold;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DemoTableWriter);

IE_NAMESPACE_END(table);

#endif //__INDEXLIB_DEMO_TABLE_WRITER_H
