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
#include "indexlib/document/document_parser/kv_parser/fb_kv_raw_document_parser.h"

#include "autil/EnvUtil.h"
#include "autil/MurmurHash.h"
#include "autil/Scope.h"
#include "autil/StringUtil.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/common/field_format/attribute/type_info.h"
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/document/kv_document/kv_document.h"
#include "indexlib/document/kv_document/kv_message_generated.h"
#include "indexlib/document/raw_document/raw_document_define.h"

namespace {
auto VT_STRING_VALUE = indexlib::document::proto::kv::Field::VT_STRING_VALUE;
auto VT_INTEGER_VALUE = indexlib::document::proto::kv::Field::VT_INTEGER_VALUE;
auto VT_FLOAT_VALUE = indexlib::document::proto::kv::Field::VT_FLOAT_VALUE;
} // namespace

namespace indexlib { namespace document {
IE_LOG_SETUP(document, FbKvRawDocumentParser);

FbKvRawDocumentParser::FbKvRawDocumentParser(const config::IndexPartitionSchemaPtr& schema)
    : _schema(schema)
    , _verify(true)
    , _userTimestampFieldName(HA_RESERVED_TIMESTAMP)
{
    _verify = autil::EnvUtil::getEnv("indexlib_verify_fb_buffer", _verify);
}

bool FbKvRawDocumentParser::construct(const DocumentInitParamPtr& initParam)
{
    _initParam = initParam;
    if (initParam) {
        initParam->GetValue(TIMESTAMP_FIELD, _userTimestampFieldName);
    }
    if (_schema->GetTableType() != tt_kv and _schema->GetTableType() != tt_kkv) {
        assert(false);
        IE_LOG(ERROR, "table [%s] is not kv/kkv", _schema->GetSchemaName().c_str());
        return false;
    }
    if (_schema->GetTableType() == tt_kv && _schema->GetRegionCount() == 1u) {
        const config::IndexSchemaPtr& indexSchema = _schema->GetIndexSchema(DEFAULT_REGIONID);
        config::KVIndexConfigPtr kvConfig =
            DYNAMIC_POINTER_CAST(config::KVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
        const config::ValueConfigPtr& valueConfig = kvConfig->GetValueConfig();
        if (valueConfig->IsSimpleValue()) {
            _simpleValueConvertor.reset(common::AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(
                _schema->GetAttributeSchema()->GetAttributeConfigByFieldId(
                    valueConfig->GetAttributeConfig(0)->GetFieldId()),
                _schema->GetTableType()));
        }
    }
    _regionResources.resize(_schema->GetRegionCount());

    for (regionid_t regionId = 0; regionId < (regionid_t)_schema->GetRegionCount(); ++regionId) {
        if (!initValueConvertor(regionId)) {
            IE_LOG(ERROR, "init pack formatter failed for table [%s] region [%d]", _schema->GetSchemaName().c_str(),
                   regionId);
            return false;
        }
        if (!initFieldProperty(regionId)) {
            IE_LOG(ERROR, "make table [%s] region [%d] field property failed", _schema->GetSchemaName().c_str(),
                   regionId);
            return false;
        }
        if (!initKeyHasher(regionId)) {
            IE_LOG(ERROR, "make table [%s] region [%d] key hasher failed", _schema->GetSchemaName().c_str(), regionId);
            return false;
        }
    }

    return true;
}

bool FbKvRawDocumentParser::initKeyHasher(regionid_t regionId)
{
    const auto& indexSchema = _schema->GetIndexSchema(regionId);
    auto& resource = _regionResources[regionId];
    if (_schema->GetTableType() == tt_kv) {
        auto kvConfig = DYNAMIC_POINTER_CAST(config::KVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
        resource.pKeyFieldType = kvConfig->GetFieldConfig()->GetFieldType();
        resource.pKeyNumberHash = kvConfig->UseNumberHash();
    } else if (_schema->GetTableType() == tt_kkv) {
        auto kkvConfig = DYNAMIC_POINTER_CAST(config::KKVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
        resource.pKeyFieldType = kkvConfig->GetPrefixFieldConfig()->GetFieldType();
        resource.sKeyFieldType = kkvConfig->GetSuffixFieldConfig()->GetFieldType();
        resource.pKeyNumberHash = kkvConfig->UseNumberHash();
    }
    return true;
}

bool FbKvRawDocumentParser::initFieldProperty(regionid_t regionId)
{
    const auto& fieldSchema = _schema->GetFieldSchema(regionId);
    const auto& indexSchema = _schema->GetIndexSchema(regionId);

    auto& fieldPropertyTable = _regionResources[regionId].fieldPropertyTable;
    auto& packFormatter = _regionResources[regionId].packFormatter;
    for (auto iter = fieldSchema->Begin(); iter != fieldSchema->End(); ++iter) {
        const auto& fieldConfig = *iter;
        const std::string& fieldName = fieldConfig->GetFieldName();
        uint64_t fieldHash = hash(fieldName.c_str(), fieldName.size());
        FieldProperty property;
        property.fieldConfig = fieldConfig.get();
        fieldPropertyTable[fieldHash] = property;
    }
    if (packFormatter) {
        const auto& fieldIdVec = packFormatter->GetFieldIds();
        for (size_t idx = 0; idx < fieldIdVec.size(); ++idx) {
            auto fid = fieldIdVec[idx];
            const auto& fieldName = fieldSchema->GetFieldConfig(fid)->GetFieldName();
            uint64_t fieldHash = hash(fieldName.c_str(), fieldName.size());
            fieldPropertyTable[fieldHash].fieldIndexInPack = idx;
        }
    }

    const std::string& pkFieldName = indexSchema->GetPrimaryKeyIndexFieldName();
    if (!addProperty(pkFieldName, regionId, FF_PKEY)) {
        return false;
    }
    if (_schema->GetTableType() == tt_kkv) {
        config::KKVIndexConfigPtr kkvIndexConfig =
            DYNAMIC_POINTER_CAST(config::KKVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
        assert(kkvIndexConfig);
        const std::string& skeyFieldName = kkvIndexConfig->GetSuffixFieldName();
        if (!addProperty(skeyFieldName, regionId, FF_SKEY)) {
            return false;
        }
    }
    if (!addProperty(_userTimestampFieldName, regionId, FF_USER_TS)) {
        return false;
    }
    if (_schema->TTLEnabled(regionId) && _schema->TTLFromDoc(regionId)) {
        const auto& ttlFieldName = _schema->GetTTLFieldName(regionId);
        if (!addProperty(ttlFieldName, regionId, FF_TTL)) {
            return false;
        }
    }
    // TODO(hanyao.hy): region_id fieldName

    return true;
}

bool FbKvRawDocumentParser::initValueConvertor(regionid_t regionId)
{
    if (_simpleValueConvertor) {
        return true;
    }

    const auto& attrSchema = _schema->GetAttributeSchema(regionId);
    if (!attrSchema) {
        IE_LOG(ERROR, "attribute schema not found in [%s]", _schema->GetSchemaName().c_str());
        return false;
    }
    const config::IndexSchemaPtr& indexSchema = _schema->GetIndexSchema(regionId);
    config::KVIndexConfigPtr kvConfig =
        DYNAMIC_POINTER_CAST(config::KVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
    if (!kvConfig) {
        IE_LOG(ERROR, "table [%s] is not kv/kkv type", _schema->GetSchemaName().c_str());
        return false;
    }

    const config::ValueConfigPtr& valueConfig = kvConfig->GetValueConfig();
    auto packFormatter = std::make_unique<common::PackAttributeFormatter>();
    if (!packFormatter->Init(valueConfig->CreatePackAttributeConfig())) {
        IE_LOG(ERROR, "create pack attr format failed, schema[%s], region[%d]", _schema->GetSchemaName().c_str(),
               regionId);
        return false;
    }
    const auto& fieldIdVec = packFormatter->GetFieldIds();
    _regionResources[regionId].packFormatter = std::move(packFormatter);
    _regionResources[regionId].fieldsBuffer.resize(fieldIdVec.size());
    _regionResources[regionId].fieldConvertors.resize(fieldIdVec.size());
    for (size_t i = 0; i < fieldIdVec.size(); ++i) {
        _regionResources[regionId].fieldConvertors[i].reset(
            common::AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(
                _schema->GetAttributeSchema()->GetAttributeConfigByFieldId(fieldIdVec[i]), _schema->GetTableType()));
    }
    return true;
}

bool FbKvRawDocumentParser::addProperty(const std::string& fieldName, regionid_t regionId, FieldFlag flag)
{
    if (fieldName.empty()) {
        return true;
    }
    uint64_t fieldHash = hash(fieldName.c_str(), fieldName.size());
    auto& propertyTable = _regionResources[regionId].fieldPropertyTable;
    auto iter = propertyTable.find(fieldHash);
    if (iter == propertyTable.end()) {
        iter = propertyTable.insert(iter, {fieldHash, FieldProperty()});
    }
    auto& property = iter->second;
    property.flag = static_cast<FieldFlag>(property.flag | flag);
    if (property.flag & FF_PKEY || property.flag & FF_SKEY) {
        if (!property.fieldConfig) {
            IE_LOG(ERROR, "field [%s] is pkey/skey, but not in fieldSchema", fieldName.c_str());
            return false;
        }
    }
    return true;
}

bool FbKvRawDocumentParser::validMessage(DocOperateType opType, bool hasPKey, bool hasSKey,
                                         const std::vector<autil::StringView>& fields) const
{
    if (!hasPKey) {
        IE_LOG(WARN, "pkey not exist");
        return false;
    }
    if (_schema->GetTableType() == tt_kkv && opType == ADD_DOC && !hasSKey) {
        IE_LOG(WARN, "skey is empty for add doc");
        return false;
    }
    for (size_t i = 0; i < fields.size(); ++i) {
        if (fields[i].empty()) {
            IE_LOG(WARN, "No.%lu field in pack is empty", i);
            return false;
        }
    }
    return true;
}

template <typename T>
std::pair<bool, T> FbKvRawDocumentParser::getNumericFieldValue(const document::proto::kv::Field* field) const
{
    assert(field);

    if (flatbuffers::IsFieldPresent(field, VT_STRING_VALUE)) {
        const flatbuffers::String* value = field->string_value();
        autil::StringView data(value->c_str(), value->size());
        T typedValue = T();
        bool ret = common::StrToT(data, typedValue);
        return {ret, typedValue};
    }
    if (flatbuffers::IsFieldPresent(field, VT_INTEGER_VALUE)) {
        return {true, static_cast<T>(field->integer_value())};
    }
    if (flatbuffers::IsFieldPresent(field, VT_FLOAT_VALUE)) {
        return {true, static_cast<T>(field->float_value())};
    }
    return {false, T()};
}

std::pair<bool, uint64_t> FbKvRawDocumentParser::getKeyHash(const document::proto::kv::Field* field,
                                                            regionid_t regionId, FieldFlag flag) const
{
    assert(field);
    auto& resource = _regionResources[regionId];
    if (flatbuffers::IsFieldPresent(field, VT_STRING_VALUE)) {
        const flatbuffers::String* value = field->string_value();
        FieldType fieldType = flag == FF_PKEY ? resource.pKeyFieldType : resource.sKeyFieldType;
        bool useNumberHash = flag == FF_PKEY ? resource.pKeyNumberHash : true;
        uint64_t key = 0;
        bool ret = index::KeyHasherWrapper::GetHashKey(fieldType, useNumberHash, value->c_str(), value->size(), key);
        return {ret, key};
    }
    // TODO(hanyao.hy): restrict PKEY to string_value only.
    if (flatbuffers::IsFieldPresent(field, VT_INTEGER_VALUE)) {
        if (flag == FF_PKEY && !resource.pKeyNumberHash) {
            IE_LOG(WARN, "get pkey failed, field [%s] is integer_value [%ld], but number hash is not setted.",
                   field->name()->c_str(), field->integer_value());
            return {false, 0};
        }
        return {true, static_cast<uint64_t>(field->integer_value())};
    }
    if (flatbuffers::IsFieldPresent(field, VT_FLOAT_VALUE)) {
        IE_LOG(WARN, "get key [%d] hash failed, field [%s] is float value", flag, field->name()->c_str());
        return {false, 0};
    }
    return {false, 0};
}

template <typename FbValue>
autil::StringView FbKvRawDocumentParser::convertNumericValue(const config::FieldConfig& fieldConfig, FbValue fbValue,
                                                             autil::mem_pool::Pool* pool) const
{
#define CONVERT_SINGLE_VALUE(fieldType)                                                                                \
    case fieldType: {                                                                                                  \
        using AttrItemType = config::FieldTypeTraits<fieldType>::AttrItemType;                                         \
        AttrItemType typedValue = static_cast<AttrItemType>(fbValue);                                                  \
        return common::SingleValueAttributeConvertor<AttrItemType>::EncodeValue(typedValue, pool);                     \
    }

    switch (fieldConfig.GetFieldType()) {
        BASIC_NUMBER_FIELD_MACRO_HELPER(CONVERT_SINGLE_VALUE);
    default:
        IE_LOG(ERROR, "invalid field type [%d]", fieldConfig.GetFieldType());
        return autil::StringView::empty_instance();
    }
#undef CONVERT_SINGLE_VALUE
}

autil::StringView FbKvRawDocumentParser::getFieldValue(const document::proto::kv::Field* field, regionid_t regionId,
                                                       const FieldProperty& property, autil::mem_pool::Pool* pool) const
{
    assert(field);
    auto& resource = _regionResources[regionId];

    if (flatbuffers::IsFieldPresent(field, VT_STRING_VALUE)) {
        const flatbuffers::String* value = field->string_value();
        common::AttributeConvertor* convertor = nullptr;
        if (_simpleValueConvertor) {
            convertor = _simpleValueConvertor.get();
        } else {
            convertor = resource.fieldConvertors[property.fieldIndexInPack].get();
        }
        autil::StringView fieldValue;
        if (value) {
            fieldValue = autil::StringView(value->c_str(), value->size());
        }
        bool hasFormatError = false;
        auto convertedValue = convertor->Encode(fieldValue, pool, hasFormatError);
        return hasFormatError ? autil::StringView::empty_instance() : convertedValue;
    } else if (flatbuffers::IsFieldPresent(field, VT_FLOAT_VALUE)) {
        return convertNumericValue(*property.fieldConfig, field->float_value(), pool);
    } else if ((flatbuffers::IsFieldPresent(field, VT_INTEGER_VALUE))) {
        return convertNumericValue(*property.fieldConfig, field->integer_value(), pool);
    } else {
        IE_LOG(ERROR, "unkown fields");
        return autil::StringView::empty_instance();
    }
}

bool FbKvRawDocumentParser::parse(const RawDocumentParser::Message& msg, document::RawDocument& rawDoc)
{
    if (_verify) {
        flatbuffers::Verifier verifier((uint8_t*)msg.data.data(), msg.data.size());
        if (!document::proto::kv::VerifyDocMessageBuffer(verifier)) {
            AUTIL_LOG(ERROR, "verify kv fb doc fail");
            return false;
        }
    }

    auto& kvDoc = static_cast<document::KVDocument&>(rawDoc);
    auto kvIndexDoc = kvDoc.CreateOneDoc();
    autil::ScopeGuard guard([&kvDoc]() { kvDoc.EraseLastDoc(); });

    auto docMessage = document::proto::kv::GetDocMessage(msg.data.data());

    regionid_t regionId = DEFAULT_REGIONID;
    const flatbuffers::String* regionField = docMessage->region_name();
    if (regionField && regionField->size() != 0) {
        regionId = _schema->GetRegionId(regionField->str());
        if (regionId == INVALID_REGIONID) {
            IE_LOG(WARN, "invalid regionname [%s]", regionField->c_str());
            return false;
        }
    }

    kvIndexDoc->SetTimestamp(msg.timestamp);
    kvIndexDoc->SetUserTimestamp(msg.timestamp);
    kvIndexDoc->SetRegionId(regionId);

    auto opType = docMessage->type();
    if (opType == document::proto::kv::OperationType_Add) {
        kvIndexDoc->SetDocOperateType(ADD_DOC);
    } else if (opType == document::proto::kv::OperationType_Delete) {
        kvIndexDoc->SetDocOperateType(DELETE_DOC);
    } else {
        assert(false);
        AUTIL_LOG(WARN, "doc op type [%d] invalid", opType);
        return false;
    }
    auto fields = docMessage->fields();

#define RETURN_IF_FAIL(retpair)                                                                                        \
    if (!ret.first) {                                                                                                  \
        IE_LOG(WARN, "parse [%s] failed", fieldName->c_str());                                                         \
        return false;                                                                                                  \
    }

    bool hasPKey = false;
    bool hasSKey = false;
    auto& fieldPropertyTable = _regionResources[regionId].fieldPropertyTable;
    for (const auto& field : *fields) {
        const flatbuffers::String* fieldName = field->name();
        uint64_t fieldHash = hash(fieldName->c_str(), fieldName->size());
        auto iter = fieldPropertyTable.find(fieldHash);
        if (iter == fieldPropertyTable.end()) {
            continue;
        }
        const auto& property = iter->second;

        if (property.flag & FF_PKEY) {
            auto ret = getKeyHash(field, regionId, FF_PKEY);
            RETURN_IF_FAIL(ret);
            kvIndexDoc->SetPKeyHash(ret.second);
            const flatbuffers::String* fieldValue = field->string_value();
            if (fieldValue) {
                autil::StringView pkFieldName(fieldName->c_str(), fieldName->size());
                autil::StringView pkFieldValue(fieldValue->c_str(), fieldValue->size());
                kvIndexDoc->SetPkField(pkFieldName, pkFieldValue);
            }
            hasPKey = true;
        }
        if (property.flag & FF_SKEY) {
            auto ret = getKeyHash(field, regionId, FF_SKEY);
            RETURN_IF_FAIL(ret);
            kvIndexDoc->SetSKeyHash(ret.second);
            hasSKey = true;
        }
        if (property.flag & FF_USER_TS) {
            auto ret = getNumericFieldValue<int64_t>(field);
            RETURN_IF_FAIL(ret);
            kvIndexDoc->SetUserTimestamp(ret.second);
        }
        if (property.flag & FF_TTL) {
            auto ret = getNumericFieldValue<uint32_t>(field);
            RETURN_IF_FAIL(ret);
            kvIndexDoc->SetTTL(ret.second);
        }

        if (property.fieldIndexInPack >= 0) {
            auto data = getFieldValue(field, regionId, property, kvIndexDoc->GetPool());
            _regionResources[regionId].fieldsBuffer[property.fieldIndexInPack] = data;
        }
    }
    kvIndexDoc->SetRegionId(regionId);
    const auto& convertedFields = _regionResources[regionId].fieldsBuffer;
    if (!validMessage(kvIndexDoc->GetDocOperateType(), hasPKey, hasSKey, convertedFields)) {
        return false;
    }

    autil::StringView value;
    if (!_simpleValueConvertor) {
        auto& packFormatter = _regionResources[regionId].packFormatter;
        assert(packFormatter);
        value = packFormatter->Format(convertedFields, kvIndexDoc->GetPool());
    } else {
        assert(_regionResources[regionId].fieldsBuffer.size() == 1u);
        value = _regionResources[regionId].fieldsBuffer[0];
    }
    if (value.empty()) {
        IE_LOG(WARN, "format pack attribute failed");
        ERROR_COLLECTOR_LOG(WARN, "format pack attribute failed");
        return false;
    }
    kvIndexDoc->SetValueNoCopy(value);

    guard.release();
    kvDoc.setDocTimestamp(msg.timestamp);
    kvDoc.SetDocInfo({msg.hashId, msg.timestamp});
    kvDoc.SetDocOperateType(ADD_DOC);
    return true;
#undef RETURN_IF_FAIL
}

bool FbKvRawDocumentParser::parseMultiMessage(const std::vector<Message>& msgs, document::RawDocument& rawDoc)
{
    bool hasValidDoc = false;
    for (const auto& msg : msgs) {
        if (parse(msg, rawDoc)) {
            hasValidDoc = true;
        } else {
            continue;
        }
    }
    return hasValidDoc;
}

}} // namespace indexlib::document
