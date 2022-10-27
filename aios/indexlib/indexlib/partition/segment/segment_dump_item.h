#ifndef __INDEXLIB_SEGMENT_DUMP_ITEM_H
#define __INDEXLIB_SEGMENT_DUMP_ITEM_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/index_base/index_meta/segment_info.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(index_base, SegmentWriter);
DECLARE_REFERENCE_CLASS(index_base, SegmentWriter);
                        
IE_NAMESPACE_BEGIN(partition);

class SegmentDumpItem
{
public:
    SegmentDumpItem(
        const config::IndexPartitionOptions& options,
        const config::IndexPartitionSchemaPtr& schema,
        const std::string& partitionName);
    virtual ~SegmentDumpItem();
public:
    virtual void Dump() = 0;
    // virtual for test
    virtual bool DumpWithMemLimit() = 0;
    virtual bool IsDumped() const = 0;
    virtual uint64_t GetInMemoryMemUse() const = 0;
    virtual uint64_t GetEstimateDumpSize() const = 0;
    virtual size_t GetTotalMemoryUse() const = 0;
    virtual segmentid_t GetSegmentId() const = 0;
    virtual index_base::SegmentInfoPtr GetSegmentInfo() const = 0;

protected:
    config::IndexPartitionOptions mOptions;
    config::IndexPartitionSchemaPtr mSchema;
    std::string mPartitionName;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SegmentDumpItem);
IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_SEGMENT_DUMP_ITEM_H
