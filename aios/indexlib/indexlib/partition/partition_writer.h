#ifndef __INDEXLIB_PARTITION_WRITER_H
#define __INDEXLIB_PARTITION_WRITER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/misc/status.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/partition/modifier/partition_modifier.h"

DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(document, Document);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);

IE_NAMESPACE_BEGIN(partition);

class PartitionWriter
{
public:
    PartitionWriter(const config::IndexPartitionOptions& options,
                    const std::string& partitionName = "")
        : mOptions(options)
        , mPartitionName(partitionName)
        , mLastConsumedMessageCount(0)
    {}

    virtual ~PartitionWriter() {}

public:
    virtual void Open(const config::IndexPartitionSchemaPtr& schema,
                      const index_base::PartitionDataPtr& partitionData,
                      const PartitionModifierPtr& modifier) = 0;
    virtual void ReOpenNewSegment(const PartitionModifierPtr& modifier) = 0;
    virtual bool BuildDocument(const document::DocumentPtr& doc) = 0; 
    virtual bool NeedDump(size_t memoryQuota) const = 0;
    virtual void DumpSegment() = 0;
    virtual void Close() = 0;

    virtual void RewriteDocument(const document::DocumentPtr& doc) = 0;

    virtual uint64_t GetTotalMemoryUse() const = 0;
    virtual uint64_t GetBuildingSegmentDumpExpandSize() const
    { assert(false); return 0; }

    virtual void ReportMetrics() const = 0;
    virtual void SetEnableReleaseOperationAfterDump(bool releaseOperations) = 0;
    virtual bool NeedRewriteDocument(const document::DocumentPtr& doc);

    virtual misc::Status GetStatus() const { return misc::UNKNOWN; }

public:
    size_t GetLastConsumedMessageCount() const { return mLastConsumedMessageCount; }

protected:
    config::IndexPartitionOptions mOptions;
    std::string mPartitionName;
    size_t mLastConsumedMessageCount;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PartitionWriter);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_PARTITION_WRITER_H
