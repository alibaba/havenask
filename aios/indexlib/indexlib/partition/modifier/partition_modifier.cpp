#include "indexlib/partition/modifier/partition_modifier.h"
#include "indexlib/document/index_document/normal_document/attribute_document.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/document/index_document/normal_document/attribute_document_field_extractor.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index/normal/primarykey/primary_key_index_reader.h"
#include "indexlib/index/normal/attribute/format/default_attribute_field_appender.h"
#include "indexlib/index/normal/attribute/accessor/attr_field_value.h"
#include "indexlib/util/memory_control/build_resource_metrics.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, PartitionModifier);

PartitionModifier::PartitionModifier(const IndexPartitionSchemaPtr& schema)
    : mSchema(schema)
    , mSupportAutoUpdate(false)
    , mDumpThreadNum(1)
{
    if (mSchema)
    {
        mSupportAutoUpdate = mSchema->SupportAutoUpdate();
    }

    if (mSupportAutoUpdate)
    {
        mDefaultValueAppender.reset(new index::DefaultAttributeFieldAppender);
        mDefaultValueAppender->Init(mSchema, index::InMemorySegmentReaderPtr());
        mAttrFieldExtractor.reset(new document::AttributeDocumentFieldExtractor());
        mAttrFieldExtractor->Init(mSchema->GetAttributeSchema());
    }
}


bool PartitionModifier::TryRewriteAdd2Update(const DocumentPtr& document)
{
    assert(mSupportAutoUpdate);
    assert(mDefaultValueAppender);
    assert(mAttrFieldExtractor);    

    NormalDocumentPtr doc = DYNAMIC_POINTER_CAST(NormalDocument, document);
    const PrimaryKeyIndexReaderPtr& pkReader =
        GetPrimaryKeyIndexReader();
    if (pkReader)
    {
        const string& pkString = doc->GetIndexDocument()->GetPrimaryKey();
        docid_t docId = pkReader->Lookup(pkString);
        if (docId != INVALID_DOCID)
        {
            RewriteAttributeDocument(doc->GetAttributeDocument(), doc->GetPool());
            mDefaultValueAppender->AppendDefaultFieldValues(doc);
            doc->SetDocOperateType(UPDATE_FIELD);
            doc->SetDocId(docId);
            return true;
        }
    }
    return false;
}

void PartitionModifier::RewriteAttributeDocument(const AttributeDocumentPtr& attrDoc, Pool* pool)
{
    const FieldSchemaPtr& fieldSchema = mSchema->GetFieldSchema();
    const AttributeSchemaPtr& attrSchema = mSchema->GetAttributeSchema();
    for (FieldSchema::Iterator it = fieldSchema->Begin();
         it != fieldSchema->End(); ++it)
    {
        if ((*it)->IsDeleted())
        {
            continue;
        }
        
        fieldid_t fieldId = (*it)->GetFieldId();
        if (!attrSchema->IsInAttribute(fieldId))
        {
            continue;
        }
        if (attrDoc->HasField(fieldId))
        {
            continue;
        }
        ConstString fieldValue = mAttrFieldExtractor->GetField(attrDoc, fieldId, pool);
        if (fieldValue.empty())
        {
            continue;
        }
        attrDoc->SetField(fieldId, fieldValue);
    }
}

int64_t PartitionModifier::GetTotalMemoryUse() const
{
    assert(mBuildResourceMetrics);
    return mBuildResourceMetrics->GetValue(util::BMT_CURRENT_MEMORY_USE);
}

bool PartitionModifier::UpdateField(const index::AttrFieldValue& value) 
{
    if (value.IsPackAttr())
    {
        return UpdatePackField(value.GetDocId(), value.GetPackAttrId(),
                               *value.GetConstStringData());
    }
    return UpdateField(value.GetDocId(), value.GetFieldId(), 
                       *value.GetConstStringData()); 
}

const util::BuildResourceMetricsPtr& PartitionModifier::GetBuildResourceMetrics() const
{
    return mBuildResourceMetrics;
}

IE_NAMESPACE_END(partition);

