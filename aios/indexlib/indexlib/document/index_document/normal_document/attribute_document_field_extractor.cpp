#include "indexlib/document/index_document/normal_document/attribute_document_field_extractor.h"
#include "indexlib/document/index_document/normal_document/attribute_document.h"
#include "indexlib/config/attribute_schema.h"
#include "indexlib/common/field_format/pack_attribute/pack_attribute_formatter.h"
#include "indexlib/common/field_format/attribute/attribute_convertor.h"
#include <autil/ConstString.h>

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(document);
IE_LOG_SETUP(document, AttributeDocumentFieldExtractor);

AttributeDocumentFieldExtractor::AttributeDocumentFieldExtractor() 
{
}

AttributeDocumentFieldExtractor::~AttributeDocumentFieldExtractor() 
{
}

bool AttributeDocumentFieldExtractor::Init(const AttributeSchemaPtr& attributeSchema)
{
    if (!attributeSchema) {
        return false;
    }
    mAttrSchema = attributeSchema;
    size_t packAttrCount = attributeSchema->GetPackAttributeCount();
    for (size_t i = 0; i < packAttrCount; i++)
    {
        const PackAttributeConfigPtr packAttrConfig =
            attributeSchema->GetPackAttributeConfig(packattrid_t(i));
        assert(packAttrConfig);

        PackAttributeFormatterPtr formatter(new PackAttributeFormatter());
        if (!formatter->Init(packAttrConfig))
        {
            return false;
        }
        mPackFormatters.push_back(formatter);
    }
    return true;
}

ConstString AttributeDocumentFieldExtractor::GetField(
        const AttributeDocumentPtr& attrDoc, fieldid_t fid, Pool *pool)
{
    if (!attrDoc)
    {
        return ConstString::EMPTY_STRING;
    }

    if (attrDoc->HasField(fid))
    {
        return attrDoc->GetField(fid);
    }

    // find field in pack attribute
    AttributeConfigPtr attrConfig = mAttrSchema->GetAttributeConfigByFieldId(fid);
    if (!attrConfig)
    {
        IE_LOG(ERROR, "get field[%d] failed, it does not belong to any attribute.", fid);
        ERROR_COLLECTOR_LOG(ERROR, "get field[%d] failed, it does not belong to any attribute.", fid);
        return ConstString::EMPTY_STRING;
    }

    PackAttributeConfig* packAttrConfig = attrConfig->GetPackAttributeConfig();
    if (!packAttrConfig)
    {
        // empty single value field will trigger this log
        IE_LOG(DEBUG, "get field[%d] failed, "
               "it is not in attributeDocument.", fid);
        ERROR_COLLECTOR_LOG(ERROR, "get field[%d] failed, "
               "it is not in attributeDocument.", fid);
        return ConstString::EMPTY_STRING;
    }

    packattrid_t packId = packAttrConfig->GetPackAttrId();
    const PackAttributeFormatterPtr& formatter = mPackFormatters[packId];
    assert(formatter);
    const ConstString& packValue = attrDoc->GetPackField(packId);
    if (packValue.empty())
    {
        IE_LOG(ERROR, "get field[%d] failed, "
               "due to pack attribute[%ud] is empty.", fid, packId); 
        ERROR_COLLECTOR_LOG(ERROR, "get field[%d] failed, "
               "due to pack attribute[%ud] is empty.", fid, packId); 
        return ConstString::EMPTY_STRING;
    }
    
    // rawValue is un-encoded fieldValue
    ConstString rawField = formatter->GetFieldValueFromPackedValue(packValue, attrConfig->GetAttrId());
    AttributeConvertorPtr convertor =
        formatter->GetAttributeConvertorByAttrId(attrConfig->GetAttrId());
    assert(pool);
    uint64_t dummyKey = uint64_t(-1);
    common::AttrValueMeta attrValueMeta(dummyKey, rawField);
    return convertor->EncodeFromAttrValueMeta(attrValueMeta, pool);
}

IE_NAMESPACE_END(document);

