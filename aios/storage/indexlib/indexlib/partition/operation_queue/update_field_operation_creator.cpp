/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "indexlib/partition/operation_queue/update_field_operation_creator.h"

#include "indexlib/common/field_format/attribute/attribute_convertor.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/index/normal/attribute/update_field_extractor.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::document;
using namespace indexlib::common;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, UpdateFieldOperationCreator);

UpdateFieldOperationCreator::UpdateFieldOperationCreator(const IndexPartitionSchemaPtr& schema)
    : OperationCreator(schema)
{
    InitAttributeConvertorMap(schema);
}

UpdateFieldOperationCreator::~UpdateFieldOperationCreator() {}

bool UpdateFieldOperationCreator::Create(const document::NormalDocumentPtr& doc, autil::mem_pool::Pool* pool,
                                         OperationBase** operation)
{
    assert(doc->HasPrimaryKey());
    uint32_t itemCount = 0;
    OperationItem* items = nullptr;
    if (!CreateOperationItems(pool, doc, &items, &itemCount)) {
        return false;
    }
    if (itemCount == 0) {
        *operation = nullptr;
        return true;
    }
    return CreateUpdateFieldOperation(doc, pool, items, itemCount, operation);
}

bool UpdateFieldOperationCreator::CreateUpdateFieldOperation(const NormalDocumentPtr& doc, Pool* pool,
                                                             OperationItem* items, uint32_t itemCount,
                                                             OperationBase** operation)
{
    assert(items);
    uint128_t pkHash;
    GetPkHash(doc->GetIndexDocument(), pkHash);
    segmentid_t segmentId = doc->GetSegmentIdBeforeModified();
    if (GetPkIndexType() == it_primarykey64) {
        UpdateFieldOperation<uint64_t>* updateOperation =
            IE_POOL_COMPATIBLE_NEW_CLASS(pool, UpdateFieldOperation<uint64_t>, doc->GetTimestamp());
        updateOperation->Init(pkHash.value[1], items, itemCount, segmentId);
        *operation = updateOperation;
    } else {
        UpdateFieldOperation<uint128_t>* updateOperation =
            IE_POOL_COMPATIBLE_NEW_CLASS(pool, UpdateFieldOperation<uint128_t>, doc->GetTimestamp());
        updateOperation->Init(pkHash, items, itemCount, segmentId);
        *operation = updateOperation;
    }

    if (!*operation) {
        IE_LOG(ERROR, "allocate memory fail!");
        return false;
    }
    return true;
}

bool UpdateFieldOperationCreator::CreateOperationItems(Pool* pool, const NormalDocumentPtr& doc, OperationItem** items,
                                                       uint32_t* itemCount)
{
    const auto& attrDoc = doc->GetAttributeDocument();
    const auto& indexDoc = doc->GetIndexDocument();
    UpdateFieldExtractor fieldExtractor(mSchema);
    if (!fieldExtractor.Init(attrDoc)) {
        return false;
    }
    *itemCount = (uint32_t)fieldExtractor.GetFieldCount();

    std::vector<fieldid_t> validFields;
    const auto& modifiedTokens = indexDoc->GetModifiedTokens();
    for (const auto& tokens : modifiedTokens) {
        if (!tokens.Valid() or tokens.Empty()) {
            continue;
        }
        fieldid_t fieldId = tokens.FieldId();
        const auto& fieldConfig = mSchema->GetFieldConfig(fieldId);
        if (!fieldConfig) {
            if (mSchema->HasModifyOperations() && fieldId < (fieldid_t)mSchema->GetFieldCount()) {
                IE_LOG(INFO, "field id [%d] is deleted, will ignore!", fieldId);
                continue;
            }
            IE_LOG(ERROR,
                   "fieldId [%d] in update document not in schema, "
                   "drop the doc",
                   fieldId);
            ERROR_COLLECTOR_LOG(ERROR,
                                "fieldId [%d] in update document not in schema, "
                                "drop the doc",
                                fieldId);
            return false;
        }
        validFields.push_back(fieldId);
    }
    *itemCount += validFields.size();
    if (!validFields.empty()) {
        (*itemCount)++;
    }
    if (*itemCount == 0) {
        return true;
    }
    *items = IE_POOL_COMPATIBLE_NEW_VECTOR(pool, OperationItem, *itemCount);
    if (!*items) {
        IE_LOG(ERROR, "allocate memory fail!");
        return false;
    }

    CreateAttrOperationItems(pool, attrDoc, fieldExtractor, *items);

    OperationItem* curItem = (*items) + (uint32_t)fieldExtractor.GetFieldCount();
    if (!validFields.empty()) {
        curItem->first = INVALID_FIELDID; // separator of attr/index items
        curItem->second = autil::StringView::empty_instance();
        ++curItem;
        CreateIndexOperationItems(pool, indexDoc, validFields, curItem);
    }
    return true;
}

void UpdateFieldOperationCreator::CreateAttrOperationItems(Pool* pool, const AttributeDocumentPtr& doc,
                                                           UpdateFieldExtractor& fieldExtractor,
                                                           OperationItem* itemBase)
{
    common::AttrValueMeta attrMeta;
    UpdateFieldExtractor::Iterator it = fieldExtractor.CreateIterator();
    OperationItem* curItem = itemBase;
    while (it.HasNext()) {
        fieldid_t fieldId = INVALID_FIELDID;
        bool isNull = false;
        const StringView& fieldValue = it.Next(fieldId, isNull);
        StringView itemValue;
        if (isNull) {
            itemValue = StringView::empty_instance();
        } else {
            StringView decodedData = DeserializeField(fieldId, fieldValue);
            itemValue = autil::MakeCString(decodedData.data(), decodedData.size(), pool);
        }
        curItem->first = fieldId;
        curItem->second = itemValue;
        ++curItem;
    }
}

void UpdateFieldOperationCreator::CreateIndexOperationItems(Pool* pool, const IndexDocumentPtr& doc,
                                                            const std::vector<fieldid_t>& validFields,
                                                            OperationItem* itemBase)
{
    assert(doc);
    OperationItem* curItem = itemBase;
    for (fieldid_t fieldId : validFields) {
        const ModifiedTokens* tokens = doc->GetFieldModifiedTokens(fieldId);
        if (!tokens) {
            continue;
        }
        autil::DataBuffer buffer(autil::DataBuffer::DEFAUTL_DATA_BUFFER_SIZE, pool);
        StringView data = tokens->Serialize(buffer);
        curItem->first = fieldId;
        curItem->second = data;
        ++curItem;
    }
}

StringView UpdateFieldOperationCreator::DeserializeField(fieldid_t fieldId, const StringView& fieldValue)
{
    const AttributeConvertorPtr& convertor = mFieldId2ConvertorMap[fieldId];
    assert(convertor);
    common::AttrValueMeta meta = convertor->Decode(fieldValue);
    return meta.data;
}

void UpdateFieldOperationCreator::InitAttributeConvertorMap(const IndexPartitionSchemaPtr& schema)
{
    AttributeSchemaPtr attrSchema = schema->GetAttributeSchema();
    if (!attrSchema) {
        return;
    }

    size_t fieldCount = schema->GetFieldCount();
    mFieldId2ConvertorMap.resize(fieldCount);
    AttributeSchema::Iterator iter = attrSchema->Begin();
    for (; iter != attrSchema->End(); iter++) {
        const auto& attrConfig = *iter;
        AttributeConvertorPtr attrConvertor(AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(attrConfig));
        mFieldId2ConvertorMap[attrConfig->GetFieldId()] = attrConvertor;
    }
}
}} // namespace indexlib::partition
