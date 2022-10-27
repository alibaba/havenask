#ifndef __INDEXLIB_ATTRIBUTE_READER_CONTAINER_H
#define __INDEXLIB_ATTRIBUTE_READER_CONTAINER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(index, MultiFieldAttributeReader);
DECLARE_REFERENCE_CLASS(index, MultiPackAttributeReader);
DECLARE_REFERENCE_CLASS(index, AttributeMetrics);
DECLARE_REFERENCE_CLASS(index, AttributeReader);
DECLARE_REFERENCE_CLASS(index, PackAttributeReader);

IE_NAMESPACE_BEGIN(index);

class AttributeReaderContainer;
DEFINE_SHARED_PTR(AttributeReaderContainer);

class AttributeReaderContainer
{
public:
    AttributeReaderContainer(const IE_NAMESPACE(config)::IndexPartitionSchemaPtr& schema);
    ~AttributeReaderContainer();
public:
    void Init(const index_base::PartitionDataPtr& partitionData,
              AttributeMetrics* attrMetrics,
              bool lazyLoad, bool needPackAttributeReaders, int32_t initReaderThreadCount);
    void InitAttributeReader(const index_base::PartitionDataPtr& partitionData,
                             bool lazyLoadAttribute,
                             const std::string& field);

    const AttributeReaderPtr& GetAttributeReader(
        const std::string& field) const;
    const PackAttributeReaderPtr& GetPackAttributeReader(
        const std::string& packAttrName) const;
    const PackAttributeReaderPtr& GetPackAttributeReader(
        packattrid_t packAttrId) const;

public:
    const MultiFieldAttributeReaderPtr& GetAttrReaders() const
    {
        return mAttrReaders;
    }
    const MultiFieldAttributeReaderPtr& GetVirtualAttrReaders() const
    {
        return mVirtualAttrReaders;
    }
    const MultiPackAttributeReaderPtr& GetPackAttrReaders() const
    {
        return mPackAttrReaders;
    }

private:
    IE_NAMESPACE(config)::IndexPartitionSchemaPtr mSchema;
    MultiFieldAttributeReaderPtr mAttrReaders;
    MultiFieldAttributeReaderPtr mVirtualAttrReaders;
    MultiPackAttributeReaderPtr mPackAttrReaders;

public:
    static PackAttributeReaderPtr RET_EMPTY_PACK_ATTR_READER;
    static AttributeReaderPtr RET_EMPTY_ATTR_READER;
    static AttributeReaderContainerPtr RET_EMPTY_ATTR_READER_CONTAINER;
    
private:
    IE_LOG_DECLARE();
};

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ATTRIBUTE_READER_CONTAINER_H
