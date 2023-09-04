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
#include "indexlib/document/normal/rewriter/PackAttributeAppender.h"

#include "indexlib/config/ITabletSchema.h"
#include "indexlib/document/normal/AttributeDocument.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "indexlib/index/kv/config/ValueConfig.h"
#include "indexlib/index/pack_attribute/Common.h"
#include "indexlib/index/pack_attribute/PackAttributeConfig.h"
#include "indexlib/table/BuiltinDefine.h"
#include "indexlib/util/ErrorLogCollector.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace indexlibv2::config;

namespace indexlibv2 { namespace document {
AUTIL_LOG_SETUP(indexlib.document, PackAttributeAppender);

pair<Status, bool> PackAttributeAppender::Init(const shared_ptr<ITabletSchema>& schema)
{
    const auto& tableType = schema->GetTableType();
    if (tableType == indexlib::table::TABLE_TYPE_KV || tableType == indexlib::table::TABLE_TYPE_KKV) {
        const auto& pkConfig = schema->GetPrimaryKeyIndexConfig();
        const auto& kvConfig = dynamic_pointer_cast<KVIndexConfig>(pkConfig);
        if (!kvConfig) {
            return {Status::OK(), false};
        }
        const auto& valueConfig = kvConfig->GetValueConfig();
        if (tableType == indexlib::table::TABLE_TYPE_KV && valueConfig->IsSimpleValue()) {
            AUTIL_LOG(INFO, "No need PackAttributeAppender for indexName[%s]", kvConfig->GetIndexName().c_str());
            return {Status::OK(), false};
        }

        if (tableType == indexlib::table::TABLE_TYPE_KKV) {
            AUTIL_LOG(ERROR, "indexlibv2 not support kkv yet");
            assert(0);
            return {Status::OK(), false};
            // KKVIndexConfigPtr kkvConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig,
            // indexSchema->GetPrimaryKeyIndexConfig()); if (kkvConfig->OptimizedStoreSKey()) {
            //     string suffixKeyFieldName = kkvConfig->GetSuffixFieldName();
            //     fieldid_t fieldId = schema->GetFieldSchema(regionId)->GetFieldId(suffixKeyFieldName);
            //     _clearFields.push_back(fieldId);
            // }
        }
        auto [status, packAttrConfig] = valueConfig->CreatePackAttributeConfig();
        RETURN2_IF_STATUS_ERROR(status, false, "value config create pack attribute config failed");
        return {Status::OK(), InitOnePackAttr(packAttrConfig)};
    } else if (tableType == indexlib::table::TABLE_TYPE_NORMAL) {
        const auto& indexConfigs = schema->GetIndexConfigs(indexlibv2::index::PACK_ATTRIBUTE_INDEX_TYPE_STR);
        _packFormatters.reserve(indexConfigs.size());
        for (const auto& indexConfig : indexConfigs) {
            const auto& packAttributeConfig =
                std::dynamic_pointer_cast<indexlibv2::index::PackAttributeConfig>(indexConfig);
            if (!packAttributeConfig) {
                assert(false);
                return {Status::Corruption(), false};
            }
            if (!InitOnePackAttr(packAttributeConfig)) {
                auto status = Status::InternalError("init pack attribute appender for [%s] failed",
                                                    indexConfig->GetIndexName().c_str());
                AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
                return {status, false};
            }
        }
        return {Status::OK(), true};
    }
    return {Status::OK(), false};
}

bool PackAttributeAppender::InitOnePackAttr(const shared_ptr<index::PackAttributeConfig>& packAttrConfig)
{
    assert(packAttrConfig);
    const auto& subAttrConfs = packAttrConfig->GetAttributeConfigVec();

    for (size_t i = 0; i < subAttrConfs.size(); ++i) {
        //_inPackFields.push_back(subAttrConfs[i]->GetFieldId());
        _clearFields.push_back(subAttrConfs[i]->GetFieldId());
    }

    shared_ptr<index::PackAttributeFormatter> packFormatter(new index::PackAttributeFormatter());
    if (!packFormatter->Init(packAttrConfig)) {
        return false;
    }
    _packFormatters.push_back(packFormatter);
    return true;
}

bool PackAttributeAppender::AppendPackAttribute(const shared_ptr<indexlib::document::AttributeDocument>& attrDocument,
                                                Pool* pool)
{
    if (attrDocument->GetPackFieldCount() > 0) {
        AUTIL_LOG(DEBUG, "attributes have already been packed");
        return CheckPackAttrFields(attrDocument);
    }

    for (size_t i = 0; i < _packFormatters.size(); ++i) {
        const index::PackAttributeFormatter::FieldIdVec& fieldIdVec = _packFormatters[i]->GetFieldIds();
        vector<StringView> fieldVec;
        fieldVec.reserve(fieldIdVec.size());
        for (auto fid : fieldIdVec) {
            fieldVec.push_back(attrDocument->GetField(fid));
        }

        StringView packedAttrField = _packFormatters[i]->Format(fieldVec, pool);
        if (packedAttrField.empty()) {
            AUTIL_LOG(WARN, "format pack attr field [%lu] fail!", i);
            ERROR_COLLECTOR_LOG(WARN, "format pack attr field [%lu] fail!", i);
            return false;
        }
        attrDocument->SetPackField(packattrid_t(i), packedAttrField);
    }
    attrDocument->ClearFields(_clearFields);
    return true;
}

bool PackAttributeAppender::EncodePatchValues(const shared_ptr<indexlib::document::AttributeDocument>& attrDocument,
                                              Pool* pool)
{
    if (attrDocument->GetPackFieldCount() > 0) {
        AUTIL_LOG(DEBUG, "attributes have already been encoded");
        return true;
    }

    for (size_t i = 0; i < _packFormatters.size(); ++i) {
        const auto& packConfigPtr = _packFormatters[i]->GetPackAttributeConfig();
        index::PackAttributeFormatter::PackAttributeFields patchFields;
        patchFields.reserve(packConfigPtr->GetAttributeConfigVec().size());
        for (const auto& attrConfig : packConfigPtr->GetAttributeConfigVec()) {
            if (attrDocument->HasField(attrConfig->GetFieldId())) {
                patchFields.push_back(
                    make_pair(attrConfig->GetAttrId(), attrDocument->GetField(attrConfig->GetFieldId())));
            }
        }
        auto bufferLen = _packFormatters[i]->GetEncodePatchValueLen(patchFields);
        auto buffer = pool->allocate(bufferLen);
        size_t encodeLen = _packFormatters[i]->EncodePatchValues(patchFields, (uint8_t*)buffer, bufferLen);
        StringView patchAttrField((char*)buffer, encodeLen);
        if (patchAttrField.empty()) {
            AUTIL_LOG(WARN, "encode patch attr field [%lu] failed!", i);
            ERROR_COLLECTOR_LOG(WARN, "encode patch attr field [%lu] failed!", i);
            return false;
        }
        attrDocument->SetPackField(packattrid_t(i), patchAttrField);
    }
    attrDocument->ClearFields(_clearFields);
    return true;
}

bool PackAttributeAppender::DecodePatchValues(uint8_t* buffer, size_t bufferLen, packattrid_t packId,
                                              index::PackAttributeFormatter::PackAttributeFields& patchFields)
{
    if (packId >= _packFormatters.size()) {
        return false;
    }
    return _packFormatters[packId]->DecodePatchValues(buffer, bufferLen, patchFields);
}

autil::StringView PackAttributeAppender::MergeAndFormatUpdateFields(
    const char* baseAddr, const index::PackAttributeFormatter::PackAttributeFields& packAttrFields,
    bool hasHashKeyInAttrFields, indexlib::util::MemBuffer& buffer, packattrid_t packId)
{
    if (packId >= _packFormatters.size()) {
        return autil::StringView::empty_instance();
    }
    return _packFormatters[packId]->MergeAndFormatUpdateFields(baseAddr, packAttrFields, hasHashKeyInAttrFields,
                                                               buffer);
}

bool PackAttributeAppender::CheckPackAttrFields(const shared_ptr<indexlib::document::AttributeDocument>& attrDocument)
{
    assert(attrDocument->GetPackFieldCount() > 0);
    if (attrDocument->GetPackFieldCount() != _packFormatters.size()) {
        AUTIL_LOG(WARN, "check pack field count [%lu:%lu] fail!", attrDocument->GetPackFieldCount(),
                  _packFormatters.size());
        return false;
    }

    for (size_t i = 0; i < _packFormatters.size(); i++) {
        const StringView& packField = attrDocument->GetPackField((packattrid_t)i);
        if (packField.empty()) {
            AUTIL_LOG(WARN, "pack field [%lu] is empty!", i);
            return false;
        }
        if (!_packFormatters[i]->CheckLength(packField)) {
            AUTIL_LOG(WARN, "check length [%lu] for encode pack field [%lu] fail!", packField.length(), i);
            return false;
        }
    }
    return true;
}
}} // namespace indexlibv2::document
