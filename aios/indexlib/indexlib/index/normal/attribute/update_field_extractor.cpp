#include "indexlib/index/normal/attribute/update_field_extractor.h"
#include "indexlib/document/index_document/normal_document/attribute_document.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(document);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, UpdateFieldExtractor);

bool UpdateFieldExtractor::Init(const document::AttributeDocumentPtr& attrDoc)
{
    assert(mFieldVector.empty());
    if (!attrDoc)
    {
        IE_LOG(WARN, "attribute document is NULL!");
        ERROR_COLLECTOR_LOG(WARN, "attribute document is NULL!");
        return false;
    }

    const AttributeSchemaPtr &attrSchema = mSchema->GetAttributeSchema();
    if (!attrSchema)
    {
        IE_LOG(WARN, "no attribute schema found!");
        ERROR_COLLECTOR_LOG(WARN, "no attribute schema found!");
        return false;
    }
    const FieldSchemaPtr& fieldSchema = mSchema->GetFieldSchema();

    fieldid_t fieldId = INVALID_FIELDID;
    AttributeDocument::Iterator it = attrDoc->CreateIterator();
    while (it.HasNext())
    {
        const ConstString& fieldValue = it.Next(fieldId);
        const FieldConfigPtr &fieldConfig = fieldSchema->GetFieldConfig(fieldId);
        if (!fieldConfig)
        {
            if (mSchema->HasModifyOperations() && fieldId < (fieldid_t)fieldSchema->GetFieldCount())
            {
                IE_LOG(INFO, "field id [%d] is deleted, will ignore!", fieldId);
                continue;
            }
            IE_LOG(ERROR, "fieldId [%d] in update document not in schema, "
                   "drop the doc", fieldId);
            ERROR_COLLECTOR_LOG(ERROR, "fieldId [%d] in update document not in schema, "
                   "drop the doc", fieldId);
            mFieldVector.clear();
            return false;
        }

        if (IsFieldIgnore(fieldConfig, fieldValue))
        {
            continue;
        }
        mFieldVector.push_back(make_pair(fieldId, fieldValue));
    }
    return true;
}

IE_NAMESPACE_END(index);

