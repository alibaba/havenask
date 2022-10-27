#ifndef __INDEXLIB_CUSTOM_ONLINE_PARTITION_WRITER_H
#define __INDEXLIB_CUSTOM_ONLINE_PARTITION_WRITER_H

#include <tr1/memory>
#include "indexlib/misc/metric_provider.h"
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/custom_offline_partition_writer.h"

DECLARE_REFERENCE_CLASS(table, TableFactoryWrapper);
DECLARE_REFERENCE_CLASS(partition, PartitonData);
DECLARE_REFERENCE_CLASS(partition, PartitionModifier);
DECLARE_REFERENCE_CLASS(partition, FlushedLocatorContainer);
DECLARE_REFERENCE_CLASS(partition, OnlinePartitionMetrics);
DECLARE_REFERENCE_CLASS(document, Document);

IE_NAMESPACE_BEGIN(partition);

class CustomOnlinePartitionWriter : public CustomOfflinePartitionWriter
{
public:
    CustomOnlinePartitionWriter(
        const config::IndexPartitionOptions& options,
        const table::TableFactoryWrapperPtr& tableFactory,
        const FlushedLocatorContainerPtr& flushedLocatorContainer, 
        OnlinePartitionMetrics* onlinePartMetrics,
        const std::string& partitionName,
        const PartitionRange& range = PartitionRange());
    ~CustomOnlinePartitionWriter();
public:
    void Open(const config::IndexPartitionSchemaPtr& schema,
              const index_base::PartitionDataPtr& partitionData,
              const PartitionModifierPtr& modifier) override;
    bool BuildDocument(const document::DocumentPtr& doc) override;
    void Close() override;
    void ReportMetrics() const override;
    bool NeedRewriteDocument(const document::DocumentPtr& doc) override
    {
        return false;
    }
private:
    bool CheckTimestamp(const document::DocumentPtr& doc) const;
private:
    OnlinePartitionMetrics* mOnlinePartMetrics; 
    volatile bool mIsClosed;
    int64_t mRtFilterTimestamp;
    mutable autil::ThreadMutex mOnlineWriterLock;;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CustomOnlinePartitionWriter);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_CUSTOM_ONLINE_PARTITION_WRITER_H
