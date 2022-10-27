#include <sstream>
#include "indexlib/index/normal/attribute/accessor/attribute_reader_creator.h"
#include "indexlib/index/normal/attribute/attribute_metrics.h"
#include "indexlib/index/normal/attribute/accessor/join_docid_attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/string_attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader_factory.h"
#include "indexlib/index/normal/attribute/accessor/location_attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/line_attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/polygon_attribute_reader.h"
#include "indexlib/misc/exception.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index_base/partition_data.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;

IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(index);

AttributeReaderFactory::AttributeReaderFactory()
{
    Init();
}

AttributeReaderFactory::~AttributeReaderFactory() 
{
}

void AttributeReaderFactory::Init()
{
    RegisterCreator(AttributeReaderCreatorPtr(
                    new Int16AttributeReader::Creator()));
    RegisterCreator(AttributeReaderCreatorPtr(
                    new UInt16AttributeReader::Creator()));
    RegisterCreator(AttributeReaderCreatorPtr(
                    new Int8AttributeReader::Creator()));
    RegisterCreator(AttributeReaderCreatorPtr(
                    new UInt8AttributeReader::Creator()));
    RegisterCreator(AttributeReaderCreatorPtr(
                    new Int32AttributeReader::Creator()));
    RegisterCreator(AttributeReaderCreatorPtr(
                    new UInt32AttributeReader::Creator()));
    RegisterCreator(AttributeReaderCreatorPtr(
                    new Int64AttributeReader::Creator()));
    RegisterCreator(AttributeReaderCreatorPtr(
                    new UInt64AttributeReader::Creator()));
    RegisterCreator(AttributeReaderCreatorPtr(
                    new Hash64AttributeReader::Creator()));
    RegisterCreator(AttributeReaderCreatorPtr(
                    new Hash128AttributeReader::Creator()));
    RegisterCreator(AttributeReaderCreatorPtr(
                    new FloatAttributeReader::Creator()));
    RegisterCreator(AttributeReaderCreatorPtr(
                    new DoubleAttributeReader::Creator()));
    // RegisterCreator(AttributeReaderCreatorPtr(
    //                 new Hash64AttributeReader::Creator()));
    // RegisterCreator(AttributeReaderCreatorPtr(
    //                 new Hash128AttributeReader::Creator()));
    RegisterCreator(AttributeReaderCreatorPtr(
                    new StringAttributeReader::Creator()));

    RegisterMultiValueCreator(AttributeReaderCreatorPtr(
                    new Int8MultiValueAttributeReader::Creator()));
    RegisterMultiValueCreator(AttributeReaderCreatorPtr(
                    new UInt8MultiValueAttributeReader::Creator()));
    RegisterMultiValueCreator(AttributeReaderCreatorPtr(
                    new Int16MultiValueAttributeReader::Creator()));
    RegisterMultiValueCreator(AttributeReaderCreatorPtr(
                    new UInt16MultiValueAttributeReader::Creator()));
    RegisterMultiValueCreator(AttributeReaderCreatorPtr(
                    new Int32MultiValueAttributeReader::Creator()));
    RegisterMultiValueCreator(AttributeReaderCreatorPtr(
                    new UInt32MultiValueAttributeReader::Creator()));
    RegisterMultiValueCreator(AttributeReaderCreatorPtr(
                    new Int64MultiValueAttributeReader::Creator()));
    RegisterMultiValueCreator(AttributeReaderCreatorPtr(
                    new UInt64MultiValueAttributeReader::Creator()));
    RegisterMultiValueCreator(AttributeReaderCreatorPtr(
                    new FloatMultiValueAttributeReader::Creator()));
    RegisterMultiValueCreator(AttributeReaderCreatorPtr(
                    new DoubleMultiValueAttributeReader::Creator()));
    RegisterMultiValueCreator(AttributeReaderCreatorPtr(
                    new MultiStringAttributeReader::Creator()));
    RegisterMultiValueCreator(AttributeReaderCreatorPtr(
                    new LocationAttributeReader::Creator()));
    RegisterMultiValueCreator(AttributeReaderCreatorPtr(
                    new LineAttributeReader::Creator()));
    RegisterMultiValueCreator(AttributeReaderCreatorPtr(
                    new PolygonAttributeReader::Creator()));
}

void AttributeReaderFactory::RegisterCreator(AttributeReaderCreatorPtr creator)
{
    assert (creator != NULL);

    ScopedLock l(mLock);    
    FieldType type = creator->GetAttributeType();
    CreatorMap::iterator it = mReaderCreators.find(type);
    if(it != mReaderCreators.end())
    {
        mReaderCreators.erase(it);
    }
    mReaderCreators.insert(make_pair(type, creator));
}

void AttributeReaderFactory::RegisterMultiValueCreator(AttributeReaderCreatorPtr creator)
{
    assert (creator != NULL);

    ScopedLock l(mLock);    
    FieldType type = creator->GetAttributeType();
    CreatorMap::iterator it = mMultiValueReaderCreators.find(type);
    if(it != mMultiValueReaderCreators.end())
    {
        mMultiValueReaderCreators.erase(it);
    }
    mMultiValueReaderCreators.insert(make_pair(type, creator));
}

AttributeReader* AttributeReaderFactory::CreateJoinDocidAttributeReader()
{
    return new JoinDocidAttributeReader;
}

AttributeReader* AttributeReaderFactory::CreateAttributeReader(
        const FieldConfigPtr& fieldConfig, AttributeMetrics* metrics) 
{
    autil::ScopedLock l(mLock);
    bool isMultiValue = fieldConfig->IsMultiValue();
    FieldType fieldType = fieldConfig->GetFieldType();
    const string& fieldName = fieldConfig->GetFieldName();
    if (!isMultiValue)
    {
        if (fieldName == MAIN_DOCID_TO_SUB_DOCID_ATTR_NAME
            || fieldName == SUB_DOCID_TO_MAIN_DOCID_ATTR_NAME)
        {
            return CreateJoinDocidAttributeReader();
        }

        CreatorMap::const_iterator it = mReaderCreators.find(fieldType);
        if (it != mReaderCreators.end())
        {
            return it->second->Create(metrics);
        }
        else
        {
            stringstream s;
            s << "Attribute Reader type: " << fieldType <<" not implemented yet!";
            INDEXLIB_THROW(misc::UnImplementException, "%s", s.str().c_str());
        }
    }
    else
    {
        CreatorMap::const_iterator it = mMultiValueReaderCreators.find(fieldType);
        if (it != mMultiValueReaderCreators.end())
        {
            return it->second->Create(metrics);
        }
        else
        {
            stringstream s;
            s << "MultiValueAttribute Reader type: " << fieldType <<" not implemented yet!";
            INDEXLIB_THROW(misc::UnImplementException, "%s", s.str().c_str());
        }
    }
    return NULL;
}

AttributeReaderPtr AttributeReaderFactory::CreateAttributeReader(
            const AttributeConfigPtr& attrConfig,
            const PartitionDataPtr& partitionData,
            AttributeMetrics* metrics)
{
    AttributeReaderPtr attrReader(
            AttributeReaderFactory::GetInstance()->CreateAttributeReader(
                    attrConfig->GetFieldConfig(), metrics));
    if (!attrReader)
    {
        return AttributeReaderPtr();
    }

    if (!attrReader->Open(attrConfig, partitionData))
    {
        return AttributeReaderPtr();
    }
    return attrReader;
}

JoinDocidAttributeReaderPtr AttributeReaderFactory::CreateJoinAttributeReader(
        const IndexPartitionSchemaPtr& schema,
        const string& attrName,
        const PartitionDataPtr& partitionData)
{
    PartitionDataPtr subPartitionData = partitionData->GetSubPartitionData();
    assert(subPartitionData);
    if (attrName == MAIN_DOCID_TO_SUB_DOCID_ATTR_NAME)
    {
        return CreateJoinAttributeReader(schema, attrName, 
                partitionData, subPartitionData);
    }
    else if (attrName == SUB_DOCID_TO_MAIN_DOCID_ATTR_NAME)
    {
        IndexPartitionSchemaPtr subSchema = schema->GetSubIndexPartitionSchema();
        return CreateJoinAttributeReader(subSchema, attrName,
                subPartitionData, partitionData);
    }
    assert(false);
    return JoinDocidAttributeReaderPtr();
}

JoinDocidAttributeReaderPtr AttributeReaderFactory::CreateJoinAttributeReader(
        const IndexPartitionSchemaPtr& schema,
        const string& attrName,
        const PartitionDataPtr& partitionData,
        const PartitionDataPtr& joinPartitionData)
{
    const AttributeSchemaPtr& attrSchema = schema->GetAttributeSchema();
    assert(attrSchema);

    AttributeConfigPtr attrConfig;
    attrConfig = attrSchema->GetAttributeConfig(attrName);
    assert(attrConfig);

    JoinDocidAttributeReaderPtr attrReader(new JoinDocidAttributeReader);
    attrReader->Open(attrConfig, partitionData);
    attrReader->InitJoinBaseDocId(joinPartitionData);
    return attrReader;
}

IE_NAMESPACE_END(index);

