#ifndef __INDEXLIB_INDEX_PARTITION_ITERATOR_H
#define __INDEXLIB_INDEX_PARTITION_ITERATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/attribute_data_iterator.h"

DECLARE_REFERENCE_CLASS(partition, OfflinePartition);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);

IE_NAMESPACE_BEGIN(partition);

class PartitionIterator
{
public:
    PartitionIterator() {}
    ~PartitionIterator() {}
    
public:
    bool Init(const OfflinePartitionPtr& offlinePartition);
    
    index::AttributeDataIteratorPtr CreateSingleAttributeIterator(
            const std::string& attrName, segmentid_t segmentId);

    bool GetSegmentDocCount(segmentid_t segmentId, uint32_t &docCount) const;

    config::IndexPartitionSchemaPtr GetSchema() const {
        return mSchema;
    }
private:
    config::AttributeConfigPtr GetAttributeConfig(const std::string& attrName);
    
private:
    index_base::PartitionDataPtr mPartitionData;
    config::IndexPartitionSchemaPtr mSchema;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PartitionIterator);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_INDEX_PARTITION_ITERATOR_H
