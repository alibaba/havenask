#include "indexlib/index/normal/attribute/accessor/attribute_reader_container.h"
#include "indexlib/index/normal/attribute/accessor/multi_field_attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/multi_pack_attribute_reader.h"
#include "indexlib/index/normal/attribute/attribute_metrics.h"
#include "indexlib/config/index_partition_schema.h"

using namespace std;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, AttributeReaderContainer);

PackAttributeReaderPtr AttributeReaderContainer::RET_EMPTY_PACK_ATTR_READER = PackAttributeReaderPtr();
AttributeReaderPtr AttributeReaderContainer::RET_EMPTY_ATTR_READER = AttributeReaderPtr();
AttributeReaderContainerPtr AttributeReaderContainer::RET_EMPTY_ATTR_READER_CONTAINER = AttributeReaderContainerPtr();

AttributeReaderContainer::AttributeReaderContainer(const IndexPartitionSchemaPtr& schema)
    : mSchema(schema)
{
}

AttributeReaderContainer::~AttributeReaderContainer() 
{
    mAttrReaders.reset();
    mVirtualAttrReaders.reset();
    mPackAttrReaders.reset();
}

void AttributeReaderContainer::Init(const PartitionDataPtr& partitionData,
                                    AttributeMetrics* attrMetrics,
                                    bool lazyLoad, bool needPackAttributeReaders,
                                    int32_t initReaderThreadCount)
{
    IE_LOG(INFO, "InitAttributeReaders begin");
        
    const AttributeSchemaPtr& attrSchema = mSchema->GetAttributeSchema();
    if (attrSchema)
    {
        mAttrReaders.reset(new MultiFieldAttributeReader(
                        attrSchema, attrMetrics, lazyLoad, initReaderThreadCount));
        mAttrReaders->Open(partitionData);
    }

    const AttributeSchemaPtr& virtualAttrSchema = mSchema->GetVirtualAttributeSchema();
    if (virtualAttrSchema)
    {
        mVirtualAttrReaders.reset(new MultiFieldAttributeReader(
                        virtualAttrSchema, attrMetrics));
        mVirtualAttrReaders->Open(partitionData);
    }
    IE_LOG(INFO, "InitAttributeReaders end");

    if (!needPackAttributeReaders) {
        return;
    }

    IE_LOG(INFO, "InitPackAttributeReaders begin");
    if (!attrSchema)
    {
        return;
    }
    mPackAttrReaders.reset(
        new MultiPackAttributeReader(attrSchema, attrMetrics));
    mPackAttrReaders->Open(partitionData);

    IE_LOG(INFO, "InitPackAttributeReaders end");
}

const AttributeReaderPtr& AttributeReaderContainer::GetAttributeReader(
    const string& field) const 
{
    if (mAttrReaders) {
        const AttributeReaderPtr& attrReader = mAttrReaders->GetAttributeReader(field);
        if (attrReader) {
            return attrReader;
        }
    }

    if (mVirtualAttrReaders) {
        const AttributeReaderPtr& attrReader = mVirtualAttrReaders->GetAttributeReader(field);
        if (attrReader) {
            return attrReader;
        }
    }
    return RET_EMPTY_ATTR_READER;
}

const PackAttributeReaderPtr& AttributeReaderContainer::GetPackAttributeReader(
    const string& packAttrName) const
{
    if (mPackAttrReaders)
    {
        return mPackAttrReaders->GetPackAttributeReader(packAttrName);        
    }
    return RET_EMPTY_PACK_ATTR_READER;
}

const PackAttributeReaderPtr& AttributeReaderContainer::GetPackAttributeReader(
    packattrid_t packAttrId) const 
{
    if (mPackAttrReaders)
    {
        return mPackAttrReaders->GetPackAttributeReader(packAttrId);        
    }
    return RET_EMPTY_PACK_ATTR_READER;
}

void AttributeReaderContainer::InitAttributeReader(const PartitionDataPtr& partitionData,
                                                   bool lazyLoadAttribute,
                                                   const string& field)
{
    const AttributeSchemaPtr& attrSchema = mSchema->GetAttributeSchema();
    if (!mAttrReaders)
    {
        mAttrReaders.reset(new MultiFieldAttributeReader(attrSchema,
                                                         NULL,
                                                         lazyLoadAttribute));
    }
    const AttributeConfigPtr& attrConfig = attrSchema->GetAttributeConfig(field);
    mAttrReaders->InitAttributeReader(attrConfig, partitionData);
}

IE_NAMESPACE_END(index);
