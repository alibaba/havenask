#include "indexlib/index/normal/ttl_decoder.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"

using namespace std;
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, TTLDecoder);

TTLDecoder::TTLDecoder(const IndexPartitionSchemaPtr& schema)
{
    mTTLFieldId = INVALID_FIELDID;
    if (!schema)
    {
        return;
    }
    const AttributeSchemaPtr &attrSchema = schema->GetAttributeSchema();
    if (!attrSchema)
    {
        return;
    }

    AttributeConfigPtr attrConfig = attrSchema->GetAttributeConfig(schema->GetTTLFieldName());
    if (!attrConfig)
    {
        return;
    }
    mTTLFieldId = attrConfig->GetFieldId();
    mConverter.reset(
            AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(
                    attrConfig->GetFieldConfig()));
}

TTLDecoder::~TTLDecoder()
{
}

void TTLDecoder::SetDocumentTTL(const DocumentPtr& document) const {
    if (INVALID_FIELDID == mTTLFieldId)
    {
        return ;
    }
    // only invert_index support doc ttl
    NormalDocumentPtr doc = DYNAMIC_POINTER_CAST(NormalDocument, document);
    const AttributeDocumentPtr& attributeDocument = doc->GetAttributeDocument();
    if (!attributeDocument)
    {
        return;
    }
    const autil::ConstString& field = attributeDocument->GetField(mTTLFieldId);
    uint32_t ttl = 0;
    if (!field.empty()) {
        common::AttrValueMeta meta = mConverter->Decode(field);
        assert(sizeof(uint32_t) == meta.data.size());
        ttl = *(uint32_t*)meta.data.data();
    }
    doc->SetTTL(ttl);
}

IE_NAMESPACE_END(index);
