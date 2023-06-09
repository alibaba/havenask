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
#include "indexlib/index/operation_log/UpdateFieldOperationCreator.h"

#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/attribute/UpdateFieldExtractor.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertor.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertorFactory.h"
#include "indexlib/index/operation_log/UpdateFieldOperation.h"

namespace indexlib::index {
namespace {
using indexlibv2::index::UpdateFieldExtractor;
} // namespace

AUTIL_LOG_SETUP(indexlib.index, UpdateFieldOperationCreator);

UpdateFieldOperationCreator::UpdateFieldOperationCreator(const std::shared_ptr<OperationLogConfig>& opConfig)
    : OperationCreator(opConfig)
{
    InitAttributeConvertorMap(opConfig);
}

bool UpdateFieldOperationCreator::Create(const indexlibv2::document::NormalDocument* doc, autil::mem_pool::Pool* pool,
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

bool UpdateFieldOperationCreator::CreateUpdateFieldOperation(const indexlibv2::document::NormalDocument* doc,
                                                             autil::mem_pool::Pool* pool, OperationItem* items,
                                                             uint32_t itemCount, OperationBase** operation)
{
    assert(items);
    autil::uint128_t pkHash;
    GetPkHash(doc->GetIndexDocument(), pkHash);
    segmentid_t segmentId = doc->GetSegmentIdBeforeModified();
    auto docInfo = doc->GetDocInfo();
    if (GetPkIndexType() == it_primarykey64) {
        UpdateFieldOperation<uint64_t>* updateOperation =
            IE_POOL_COMPATIBLE_NEW_CLASS(pool, UpdateFieldOperation<uint64_t>, docInfo.timestamp, docInfo.hashId);
        updateOperation->Init(pkHash.value[1], items, itemCount, segmentId);
        *operation = updateOperation;
    } else {
        UpdateFieldOperation<autil::uint128_t>* updateOperation = IE_POOL_COMPATIBLE_NEW_CLASS(
            pool, UpdateFieldOperation<autil::uint128_t>, docInfo.timestamp, docInfo.hashId);
        updateOperation->Init(pkHash, items, itemCount, segmentId);
        *operation = updateOperation;
    }

    if (!*operation) {
        AUTIL_LOG(ERROR, "allocate memory fail!");
        return false;
    }
    return true;
}

bool UpdateFieldOperationCreator::CreateOperationItems(autil::mem_pool::Pool* pool,
                                                       const indexlibv2::document::NormalDocument* doc,
                                                       OperationItem** items, uint32_t* itemCount)
{
    const auto& attrDoc = doc->GetAttributeDocument();
    const auto& allFieldConfigs = _config->GetFieldConfigs();
    const auto& attrConfigs = _config->GetIndexConfigs(indexlibv2::index::ATTRIBUTE_INDEX_TYPE_STR);
    const auto& primaryKeyConfig = _config->GetPrimaryKeyIndexConfig();
    UpdateFieldExtractor attributeUpdateFieldExtractor;
    attributeUpdateFieldExtractor.Init(primaryKeyConfig, attrConfigs, allFieldConfigs);
    if (!attributeUpdateFieldExtractor.LoadFieldsFromDoc(attrDoc.get())) {
        return false;
    }

    std::vector<fieldid_t> invertedIndexUpdateFieldIds;
    const auto& modifiedTokens = doc->GetIndexDocument()->GetModifiedTokens();
    for (const auto& tokens : modifiedTokens) {
        if (!tokens.Valid() or tokens.Empty()) {
            continue;
        }
        fieldid_t fieldId = tokens.FieldId();
        // TODO: 参考：UpdateFieldExtractor.cpp，add check for delete field
        // const auto& fieldConfig = mSchema->GetFieldConfig(fieldId);
        // if (!fieldConfig) {
        //     if (mSchema->HasModifyOperations() && fieldId < (fieldid_t)mSchema->GetFieldCount()) {
        //         AUTIL_LOG(INFO, "field id [%d] is deleted, will ignore!", fieldId);
        //         continue;
        //     }
        //     AUTIL_LOG(ERROR, "fieldId [%d] in update document not in schema, drop the doc", fieldId);
        //     ERROR_COLLECTOR_LOG(ERROR, "fieldId [%d] in update document not in schema, drop the doc", fieldId);
        //     return false;
        // }
        invertedIndexUpdateFieldIds.push_back(fieldId);
    }

    *itemCount = (uint32_t)attributeUpdateFieldExtractor.GetFieldCount() +
                 (invertedIndexUpdateFieldIds.empty() ? 0 : 1 + invertedIndexUpdateFieldIds.size());
    if (*itemCount == 0) {
        return true;
    }
    *items = IE_POOL_COMPATIBLE_NEW_VECTOR(pool, OperationItem, *itemCount);
    if (!*items) {
        AUTIL_LOG(ERROR, "allocate memory fail!");
        return false;
    }

    CreateAttrOperationItems(pool, attrDoc, attributeUpdateFieldExtractor, *items);
    OperationItem* curItem = (*items) + (uint32_t)attributeUpdateFieldExtractor.GetFieldCount();
    if (not invertedIndexUpdateFieldIds.empty()) {
        curItem->first = INVALID_FIELDID; // separator of attr/index items
        curItem->second = autil::StringView::empty_instance();
        ++curItem;
    }
    CreateInvertedIndexOperationItems(pool, doc->GetIndexDocument(), invertedIndexUpdateFieldIds, curItem);
    return true;
}

void UpdateFieldOperationCreator::CreateAttrOperationItems(autil::mem_pool::Pool* pool,
                                                           const std::shared_ptr<document::AttributeDocument>& doc,
                                                           UpdateFieldExtractor& fieldExtractor,
                                                           OperationItem* itemBase)
{
    UpdateFieldExtractor::Iterator it = fieldExtractor.CreateIterator();
    OperationItem* curItem = itemBase;
    while (it.HasNext()) {
        fieldid_t fieldId = INVALID_FIELDID;
        bool isNull = false;
        const autil::StringView& fieldValue = it.Next(fieldId, isNull);
        autil::StringView itemValue;
        if (isNull) {
            itemValue = autil::StringView::empty_instance();
        } else {
            autil::StringView decodedData = DeserializeField(fieldId, fieldValue);
            itemValue = autil::MakeCString(decodedData.data(), decodedData.size(), pool);
        }
        curItem->first = fieldId;
        curItem->second = itemValue;
        ++curItem;
    }
}

void UpdateFieldOperationCreator::CreateInvertedIndexOperationItems(autil::mem_pool::Pool* pool,
                                                                    const std::shared_ptr<document::IndexDocument>& doc,
                                                                    const std::vector<fieldid_t>& updateFieldIds,
                                                                    OperationItem* itemBase)
{
    assert(doc);
    OperationItem* curItem = itemBase;
    for (fieldid_t fieldId : updateFieldIds) {
        const document::ModifiedTokens* tokens = doc->GetFieldModifiedTokens(fieldId);
        if (!tokens) {
            continue;
        }
        autil::DataBuffer buffer(autil::DataBuffer::DEFAUTL_DATA_BUFFER_SIZE, pool);
        autil::StringView data = tokens->Serialize(buffer);
        curItem->first = fieldId;
        curItem->second = data;
        ++curItem;
    }
}

autil::StringView UpdateFieldOperationCreator::DeserializeField(fieldid_t fieldId, const autil::StringView& fieldValue)
{
    const auto& convertor = _fieldId2ConvertorMap[fieldId];
    assert(convertor);
    auto meta = convertor->Decode(fieldValue);
    return meta.data;
}

void UpdateFieldOperationCreator::InitAttributeConvertorMap(const std::shared_ptr<OperationLogConfig>& opConfig)
{
    auto allFieldConfigs = opConfig->GetFieldConfigs();

    size_t fieldCount = allFieldConfigs.size();
    _fieldId2ConvertorMap.resize(fieldCount);
    auto indexConfigs = opConfig->GetIndexConfigs(indexlibv2::index::ATTRIBUTE_INDEX_TYPE_STR);

    for (const auto& indexConfig : indexConfigs) {
        const auto& attrConfig = std::dynamic_pointer_cast<indexlibv2::config::AttributeConfig>(indexConfig);
        assert(attrConfig != nullptr);
        const auto& fieldConfig = attrConfig->GetFieldConfig();
        std::shared_ptr<indexlibv2::index::AttributeConvertor> attrConvertor(
            indexlibv2::index::AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(attrConfig));
        _fieldId2ConvertorMap[fieldConfig->GetFieldId()] = attrConvertor;
    }
}

} // namespace indexlib::index
