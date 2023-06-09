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
#include "indexlib/document/document_rewriter/pack_attribute_appender.h"

#include "indexlib/config/attribute_schema.h"
#include "indexlib/config/kkv_index_config.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace indexlib::common;
using namespace indexlib::config;

namespace indexlib { namespace document {
IE_LOG_SETUP(document, PackAttributeAppender);

bool PackAttributeAppender::Init(const IndexPartitionSchemaPtr& schema, regionid_t regionId)
{
    AttributeSchemaPtr attrSchema = schema->GetAttributeSchema(regionId);
    if (!attrSchema) {
        IE_LOG(INFO, "no attribute schema defined in schema: %s", schema->GetSchemaName().c_str());
        return false;
    }

    TableType tableType = schema->GetTableType();
    if (tableType == tt_kkv || tableType == tt_kv) {
        const IndexSchemaPtr& indexSchema = schema->GetIndexSchema(regionId);
        KVIndexConfigPtr kvConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
        if (!kvConfig) {
            return false;
        }
        const ValueConfigPtr& valueConfig = kvConfig->GetValueConfig();
        if (tableType == tt_kv && schema->GetRegionCount() == 1u && valueConfig->IsSimpleValue()) {
            IE_LOG(INFO, "No need PackAttributeAppender for region [%d]!", kvConfig->GetRegionId());
            return false;
        }

        if (tableType == tt_kkv) {
            KKVIndexConfigPtr kkvConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
            if (kkvConfig->OptimizedStoreSKey()) {
                string suffixKeyFieldName = kkvConfig->GetSuffixFieldName();
                fieldid_t fieldId = schema->GetFieldSchema(regionId)->GetFieldId(suffixKeyFieldName);
                mClearFields.push_back(fieldId);
            }
        }
        return InitOnePackAttr(valueConfig->CreatePackAttributeConfig());
    }

    size_t packConfigCount = attrSchema->GetPackAttributeCount();
    if (packConfigCount == 0) {
        return false;
    }

    mPackFormatters.reserve(packConfigCount);

    for (packattrid_t packId = 0; (size_t)packId < packConfigCount; ++packId) {
        if (!InitOnePackAttr(attrSchema->GetPackAttributeConfig(packId))) {
            return false;
        }
    }
    return true;
}

bool PackAttributeAppender::InitOnePackAttr(const PackAttributeConfigPtr& packAttrConfig)
{
    assert(packAttrConfig);
    const vector<AttributeConfigPtr>& subAttrConfs = packAttrConfig->GetAttributeConfigVec();

    for (size_t i = 0; i < subAttrConfs.size(); ++i) {
        mInPackFields.push_back(subAttrConfs[i]->GetFieldId());
        mClearFields.push_back(subAttrConfs[i]->GetFieldId());
    }

    PackAttributeFormatterPtr packFormatter(new PackAttributeFormatter());
    if (!packFormatter->Init(packAttrConfig)) {
        return false;
    }
    mPackFormatters.push_back(packFormatter);
    return true;
}

bool PackAttributeAppender::AppendPackAttribute(const AttributeDocumentPtr& attrDocument, Pool* pool)
{
    if (attrDocument->GetPackFieldCount() > 0) {
        IE_LOG(DEBUG, "attributes have already been packed");
        return CheckPackAttrFields(attrDocument);
    }

    for (size_t i = 0; i < mPackFormatters.size(); ++i) {
        const PackAttributeFormatter::FieldIdVec& fieldIdVec = mPackFormatters[i]->GetFieldIds();
        vector<StringView> fieldVec;
        fieldVec.reserve(fieldIdVec.size());
        for (auto fid : fieldIdVec) {
            fieldVec.push_back(attrDocument->GetField(fid));
        }

        StringView packedAttrField = mPackFormatters[i]->Format(fieldVec, pool);
        if (packedAttrField.empty()) {
            IE_LOG(WARN, "format pack attr field [%lu] fail!", i);
            ERROR_COLLECTOR_LOG(WARN, "format pack attr field [%lu] fail!", i);
            return false;
        }
        attrDocument->SetPackField(packattrid_t(i), packedAttrField);
    }
    attrDocument->ClearFields(mClearFields);
    return true;
}

bool PackAttributeAppender::EncodePatchValues(const AttributeDocumentPtr& attrDocument, Pool* pool)
{
    if (attrDocument->GetPackFieldCount() > 0) {
        IE_LOG(DEBUG, "attributes have already been encoded");
        return true;
    }

    for (size_t i = 0; i < mPackFormatters.size(); ++i) {
        const auto& packConfigPtr = mPackFormatters[i]->GetPackAttributeConfig();
        common::PackAttributeFormatter::PackAttributeFields patchFields;
        patchFields.reserve(packConfigPtr->GetAttributeConfigVec().size());
        for (const auto& attrConfig : packConfigPtr->GetAttributeConfigVec()) {
            if (attrDocument->HasField(attrConfig->GetFieldId())) {
                patchFields.push_back(
                    make_pair(attrConfig->GetAttrId(), attrDocument->GetField(attrConfig->GetFieldId())));
            }
        }
        auto bufferLen = mPackFormatters[i]->GetEncodePatchValueLen(patchFields);
        auto buffer = pool->allocate(bufferLen);
        size_t encodeLen = mPackFormatters[i]->EncodePatchValues(patchFields, (uint8_t*)buffer, bufferLen);
        StringView patchAttrField((char*)buffer, encodeLen);
        if (patchAttrField.empty()) {
            IE_LOG(WARN, "encode patch attr field [%lu] failed!", i);
            ERROR_COLLECTOR_LOG(WARN, "encode patch attr field [%lu] failed!", i);
            return false;
        }
        attrDocument->SetPackField(packattrid_t(i), patchAttrField);
    }
    attrDocument->ClearFields(mClearFields);
    return true;
}

bool PackAttributeAppender::DecodePatchValues(uint8_t* buffer, size_t bufferLen, packattrid_t packId,
                                              PackAttributeFormatter::PackAttributeFields& patchFields)
{
    if (packId >= mPackFormatters.size()) {
        return false;
    }
    return mPackFormatters[packId]->DecodePatchValues(buffer, bufferLen, patchFields);
}

autil::StringView PackAttributeAppender::MergeAndFormatUpdateFields(
    const char* baseAddr, const PackAttributeFormatter::PackAttributeFields& packAttrFields,
    bool hasHashKeyInAttrFields, util::MemBuffer& buffer, packattrid_t packId)
{
    if (packId >= mPackFormatters.size()) {
        return autil::StringView::empty_instance();
    }
    return mPackFormatters[packId]->MergeAndFormatUpdateFields(baseAddr, packAttrFields, hasHashKeyInAttrFields,
                                                               buffer);
}

bool PackAttributeAppender::CheckPackAttrFields(const AttributeDocumentPtr& attrDocument)
{
    assert(attrDocument->GetPackFieldCount() > 0);
    if (attrDocument->GetPackFieldCount() != mPackFormatters.size()) {
        IE_LOG(WARN, "check pack field count [%lu:%lu] fail!", attrDocument->GetPackFieldCount(),
               mPackFormatters.size());
        return false;
    }

    for (size_t i = 0; i < mPackFormatters.size(); i++) {
        const StringView& packField = attrDocument->GetPackField((packattrid_t)i);
        if (packField.empty()) {
            IE_LOG(WARN, "pack field [%lu] is empty!", i);
            return false;
        }
        if (!mPackFormatters[i]->CheckLength(packField)) {
            IE_LOG(WARN, "check length [%lu] for encode pack field [%lu] fail!", packField.length(), i);
            return false;
        }
    }
    return true;
}
}} // namespace indexlib::document
