#include "indexlib/partition/operation_queue/update_field_operation_creator.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/index/normal/attribute/update_field_extractor.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/common/field_format/attribute/attribute_convertor.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, UpdateFieldOperationCreator);

UpdateFieldOperationCreator::UpdateFieldOperationCreator(
        const IndexPartitionSchemaPtr& schema)
    : OperationCreator(schema)
{
    InitAttributeConvertorMap(schema);
}

UpdateFieldOperationCreator::~UpdateFieldOperationCreator() 
{
}

OperationBase* UpdateFieldOperationCreator::Create(
        const NormalDocumentPtr& doc, Pool *pool)
{
    assert(doc->HasPrimaryKey());
    uint128_t pkHash;
    GetPkHash(doc->GetIndexDocument(), pkHash);
    segmentid_t segmentId = doc->GetSegmentIdBeforeModified();

    uint32_t itemCount = 0;
    OperationItem* items = CreateOperationItems(
            pool, doc->GetAttributeDocument(), itemCount);
    if (items == NULL)
    {
        return NULL;
    }

    OperationBase *operation = NULL;
    if (GetPkIndexType() == it_primarykey64)
    {
        UpdateFieldOperation<uint64_t> *updateOperation =
            IE_POOL_COMPATIBLE_NEW_CLASS(pool, UpdateFieldOperation<uint64_t>, 
                                      doc->GetTimestamp());
        updateOperation->Init(pkHash.value[1], items, itemCount, segmentId);
        operation = updateOperation;
    }
    else
    {
        UpdateFieldOperation<uint128_t> *updateOperation =
            IE_POOL_COMPATIBLE_NEW_CLASS(pool, UpdateFieldOperation<uint128_t>, 
                                      doc->GetTimestamp());
        updateOperation->Init(pkHash, items, itemCount, segmentId);
        operation = updateOperation;
    }

    if (!operation)
    {
        IE_LOG(ERROR, "allocate memory fail!");
        return NULL;
    }
    return operation;
}

OperationItem* UpdateFieldOperationCreator::CreateOperationItems(
        Pool *pool, const AttributeDocumentPtr& attrDoc,
        uint32_t& itemCount)
{
    UpdateFieldExtractor fieldExtractor(mSchema);
    fieldExtractor.Init(attrDoc);

    itemCount = (uint32_t)fieldExtractor.GetFieldCount();
    if (itemCount == 0)
    {
        return NULL;
    }

    OperationItem *items = IE_POOL_COMPATIBLE_NEW_VECTOR(
            pool, OperationItem, itemCount);
    if (!items)
    {
        IE_LOG(ERROR, "allocate memory fail!");
        return NULL;
    }

    uint32_t idx = 0;
    common::AttrValueMeta attrMeta;
    UpdateFieldExtractor::Iterator it = fieldExtractor.CreateIterator();
    while (it.HasNext())
    {
        fieldid_t fieldId = INVALID_FIELDID;
        const ConstString& fieldValue = it.Next(fieldId);
        ConstString decodedData = DeserializeField(fieldId, fieldValue);
        ConstString itemValue(decodedData.data(), decodedData.size(), pool);
        items[idx].first = fieldId;
        items[idx].second = itemValue;
        ++idx;
    }
    assert(idx == itemCount);
    return items;
}

ConstString UpdateFieldOperationCreator::DeserializeField(
        fieldid_t fieldId, const ConstString& fieldValue)
{
    const AttributeConvertorPtr& convertor = mFieldId2ConvertorMap[fieldId];
    assert(convertor);
    common::AttrValueMeta meta = convertor->Decode(fieldValue);
    return meta.data;
}

void UpdateFieldOperationCreator::InitAttributeConvertorMap(
        const IndexPartitionSchemaPtr& schema)
{
    AttributeSchemaPtr attrSchema = schema->GetAttributeSchema();
    if (!attrSchema)
    {
        return;
    }

    size_t fieldCount = schema->GetFieldSchema()->GetFieldCount();
    mFieldId2ConvertorMap.resize(fieldCount);
    AttributeSchema::Iterator iter = attrSchema->Begin();
    for (; iter != attrSchema->End(); iter++)
    {
        const FieldConfigPtr& fieldConfig = (*iter)->GetFieldConfig();
        AttributeConvertorPtr attrConvertor(
                AttributeConvertorFactory::GetInstance()
                ->CreateAttrConvertor(fieldConfig));
        mFieldId2ConvertorMap[fieldConfig->GetFieldId()] = attrConvertor;
    }
}

IE_NAMESPACE_END(partition);

