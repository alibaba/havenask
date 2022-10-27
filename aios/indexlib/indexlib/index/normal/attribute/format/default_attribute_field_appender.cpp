#include "indexlib/index/normal/attribute/format/default_attribute_field_appender.h"
#include "indexlib/index/in_memory_segment_reader.h"
#include "indexlib/common/field_format/attribute/attribute_value_initializer.h"
#include "indexlib/common/field_format/attribute/attribute_value_initializer_creator.h"
#include "indexlib/common/field_format/attribute/default_attribute_value_initializer_creator.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/config/index_partition_schema.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(document);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, DefaultAttributeFieldAppender);

DefaultAttributeFieldAppender::DefaultAttributeFieldAppender() 
{
}

DefaultAttributeFieldAppender::~DefaultAttributeFieldAppender() 
{
}

void DefaultAttributeFieldAppender::Init(
        const IndexPartitionSchemaPtr& schema,
        const InMemorySegmentReaderPtr& inMemSegmentReader)
{
    assert(schema);
    mSchema = schema;
    InitAttributeValueInitializers(
            mSchema->GetAttributeSchema(), inMemSegmentReader);
    InitAttributeValueInitializers(
            mSchema->GetVirtualAttributeSchema(), inMemSegmentReader);
}

void DefaultAttributeFieldAppender::AppendDefaultFieldValues(
        const NormalDocumentPtr& document)
{
    assert(document->GetDocOperateType() == ADD_DOC);
    InitEmptyFields(document, mSchema->GetAttributeSchema(), false);
    InitEmptyFields(document, mSchema->GetVirtualAttributeSchema(), true);
}


void DefaultAttributeFieldAppender::InitAttributeValueInitializers(
        const AttributeSchemaPtr& attrSchema,
        const InMemorySegmentReaderPtr& inMemSegmentReader)
{
    if (!attrSchema)
    {
        return;
    }

    AttributeSchema::Iterator iter = attrSchema->Begin();
    for (; iter != attrSchema->End(); iter++)
    {
        const AttributeConfigPtr& attrConf = *iter;
        AttributeValueInitializerCreatorPtr creator = 
            attrConf->GetAttrValueInitializerCreator();
        if (!creator)
        {
            creator.reset(new DefaultAttributeValueInitializerCreator(attrConf->GetFieldConfig()));
        }
        fieldid_t fieldId = attrConf->GetFieldId();
        assert(fieldId != INVALID_FIELDID);

        if ((size_t)fieldId >= mAttrInitializers.size())
        {
            mAttrInitializers.resize(fieldId + 1);
        }
        mAttrInitializers[fieldId] = creator->Create(inMemSegmentReader);
    }
}

void DefaultAttributeFieldAppender::InitEmptyFields(
        const NormalDocumentPtr& document, 
        const AttributeSchemaPtr& attrSchema, bool isVirtual)
{
    if (!attrSchema)
    {
        return;
    }

    if (!document->GetAttributeDocument())
    {
        AttributeDocumentPtr newAttrDoc(new AttributeDocument);
        newAttrDoc->Reserve(attrSchema->GetAttributeCount());
        newAttrDoc->SetDocId(document->GetDocId());
        document->SetAttributeDocument(newAttrDoc);
    }

    const AttributeDocumentPtr& attrDoc = document->GetAttributeDocument();
    assert(attrDoc);
    
    uint32_t attrCount = attrSchema->GetAttributeCount();
    uint32_t notEmptyAttrFieldCount = attrDoc->GetNotEmptyFieldCount();
    if (!isVirtual && (notEmptyAttrFieldCount >= attrCount))
    {
        IE_LOG(DEBUG, "all attribute field not empty!");
        return;
    }

    docid_t docId = attrDoc->GetDocId();
    AttributeSchema::Iterator iter = attrSchema->Begin();
    for (; iter != attrSchema->End(); iter++)
    {
        const AttributeConfigPtr& attrConfig = *iter;
        if (attrConfig->IsDeleted())
        {
            continue;
        }
        
        fieldid_t fieldId = attrConfig->GetFieldId();
        assert(fieldId != INVALID_FIELDID);
        const ConstString& fieldValue = attrDoc->GetField(fieldId);

        if (fieldValue.empty() && (attrConfig->GetPackAttributeConfig() == NULL)) 
        {
            ConstString initValue;
            if (!mAttrInitializers[fieldId])
            {
                IE_LOG(ERROR, "attributeValueInitializer [fieldId:%d] is NULL",
                       fieldId);
                ERROR_COLLECTOR_LOG(ERROR, "attributeValueInitializer [fieldId:%d] is NULL",
                        fieldId);
                continue;
            }

            if (!mAttrInitializers[fieldId]->GetInitValue(
                            docId, initValue, document->GetPool()))
            {
                IE_LOG(ERROR, "GetInitValue for field [%s] in document [%d] fail!",
                       attrConfig->GetAttrName().c_str(), docId);
                ERROR_COLLECTOR_LOG(ERROR, "GetInitValue for field [%s] in document [%d] fail!",
                       attrConfig->GetAttrName().c_str(), docId);
                continue;
            }
            attrDoc->SetField(fieldId, initValue);
        }
    }
}

IE_NAMESPACE_END(index);

