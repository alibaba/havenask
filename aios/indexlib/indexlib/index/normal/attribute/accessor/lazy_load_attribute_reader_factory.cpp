#include <sstream>
#include "indexlib/index/normal/attribute/accessor/lazy_load_string_attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/lazy_load_single_value_attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/lazy_load_var_num_attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/lazy_load_attribute_reader_factory.h"
#include "indexlib/index/normal/attribute/accessor/lazy_load_location_attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/lazy_load_line_attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/lazy_load_polygon_attribute_reader.h"
#include "indexlib/misc/exception.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;

IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(index);

LazyLoadAttributeReaderFactory::LazyLoadAttributeReaderFactory()
{
    Init();
}

LazyLoadAttributeReaderFactory::~LazyLoadAttributeReaderFactory() 
{
}

void LazyLoadAttributeReaderFactory::Init()
{
    RegisterCreator(AttributeReaderCreatorPtr(
                    new LazyLoadInt16AttributeReader::Creator()));
    RegisterCreator(AttributeReaderCreatorPtr(
                    new LazyLoadUInt16AttributeReader::Creator()));
    RegisterCreator(AttributeReaderCreatorPtr(
                    new LazyLoadInt8AttributeReader::Creator()));
    RegisterCreator(AttributeReaderCreatorPtr(
                    new LazyLoadUInt8AttributeReader::Creator()));
    RegisterCreator(AttributeReaderCreatorPtr(
                    new LazyLoadInt32AttributeReader::Creator()));
    RegisterCreator(AttributeReaderCreatorPtr(
                    new LazyLoadUInt32AttributeReader::Creator()));
    RegisterCreator(AttributeReaderCreatorPtr(
                    new LazyLoadInt64AttributeReader::Creator()));
    RegisterCreator(AttributeReaderCreatorPtr(
                    new LazyLoadUInt64AttributeReader::Creator()));
    RegisterCreator(AttributeReaderCreatorPtr(
                    new LazyLoadHash64AttributeReader::Creator()));
    RegisterCreator(AttributeReaderCreatorPtr(
                    new LazyLoadHash128AttributeReader::Creator()));
    RegisterCreator(AttributeReaderCreatorPtr(
                    new LazyLoadFloatAttributeReader::Creator()));
    RegisterCreator(AttributeReaderCreatorPtr(
                    new LazyLoadDoubleAttributeReader::Creator()));
    // RegisterCreator(AttributeReaderCreatorPtr(
    //                 new Hash64AttributeReader::Creator()));
    // RegisterCreator(AttributeReaderCreatorPtr(
    //                 new Hash128AttributeReader::Creator()));
    RegisterCreator(AttributeReaderCreatorPtr(
                    new LazyLoadStringAttributeReader::Creator()));
    RegisterMultiValueCreator(AttributeReaderCreatorPtr(
                    new LazyLoadInt8MultiValueAttributeReader::Creator()));
    RegisterMultiValueCreator(AttributeReaderCreatorPtr(
                    new LazyLoadUInt8MultiValueAttributeReader::Creator()));
    RegisterMultiValueCreator(AttributeReaderCreatorPtr(
                    new LazyLoadInt16MultiValueAttributeReader::Creator()));
    RegisterMultiValueCreator(AttributeReaderCreatorPtr(
                    new LazyLoadUInt16MultiValueAttributeReader::Creator()));
    RegisterMultiValueCreator(AttributeReaderCreatorPtr(
                    new LazyLoadInt32MultiValueAttributeReader::Creator()));
    RegisterMultiValueCreator(AttributeReaderCreatorPtr(
                    new LazyLoadUInt32MultiValueAttributeReader::Creator()));
    RegisterMultiValueCreator(AttributeReaderCreatorPtr(
                    new LazyLoadInt64MultiValueAttributeReader::Creator()));
    RegisterMultiValueCreator(AttributeReaderCreatorPtr(
                    new LazyLoadUInt64MultiValueAttributeReader::Creator()));
    RegisterMultiValueCreator(AttributeReaderCreatorPtr(
                    new LazyLoadFloatMultiValueAttributeReader::Creator()));
    RegisterMultiValueCreator(AttributeReaderCreatorPtr(
                    new LazyLoadDoubleMultiValueAttributeReader::Creator()));
    RegisterMultiValueCreator(AttributeReaderCreatorPtr(
                    new LazyLoadMultiStringAttributeReader::Creator()));
    RegisterMultiValueCreator(AttributeReaderCreatorPtr(
                    new LazyLoadLocationAttributeReader::Creator()));
    RegisterMultiValueCreator(AttributeReaderCreatorPtr(
                    new LazyLoadLineAttributeReader::Creator()));
    RegisterMultiValueCreator(AttributeReaderCreatorPtr(
                    new LazyLoadPolygonAttributeReader::Creator()));
}

void LazyLoadAttributeReaderFactory::RegisterCreator(AttributeReaderCreatorPtr creator)
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

void LazyLoadAttributeReaderFactory::RegisterMultiValueCreator(AttributeReaderCreatorPtr creator)
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

AttributeReader* LazyLoadAttributeReaderFactory::CreateAttributeReader(
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
            INDEXLIB_THROW(misc::UnSupportedException,
                           "LazyLoadAttributeReaderFactory does not "
                           "support creating JoinDocIdAttribute reader");            
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

AttributeReaderPtr LazyLoadAttributeReaderFactory::CreateAttributeReader(
            const AttributeConfigPtr& attrConfig,
            const PartitionDataPtr& partitionData,
            AttributeMetrics* metrics)
{
    AttributeReaderPtr attrReader(
            LazyLoadAttributeReaderFactory::GetInstance()->CreateAttributeReader(
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

IE_NAMESPACE_END(index);

