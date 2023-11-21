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
#include "indexlib/index/kv/KVIndexFieldsParser.h"

#include "autil/ConstString.h"
#include "indexlib/config/IndexConfigHash.h"
#include "indexlib/document/BuiltinParserInitParam.h"
#include "indexlib/document/ExtendDocument.h"
#include "indexlib/document/RawDocument.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "indexlib/util/DocTracer.h"
#include "indexlib/util/ErrorLogCollector.h"
#include "indexlib/util/PooledUniquePtr.h"
#include "indexlib/util/counter/AccumulativeCounter.h"
#include "indexlib/util/counter/CounterMap.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, KVIndexFieldsParser);

KVIndexFieldsParser::~KVIndexFieldsParser() {}

Status KVIndexFieldsParser::Init(const std::vector<std::shared_ptr<config::IIndexConfig>>& indexConfigs,
                                 const std::shared_ptr<document::DocumentInitParam>& initParam)
{
    _attributeConvertErrorCounter = InitCounter(initParam);
    for (const auto& indexConfig : indexConfigs) {
        const auto& kvConfig = std::dynamic_pointer_cast<config::KVIndexConfig>(indexConfig);
        if (!kvConfig) {
            RETURN_IF_STATUS_ERROR(Status::InternalError(), "cast to KVIndexConfig failed, indexType[%s] indexName[%s]",
                                   indexConfig->GetIndexType().c_str(), indexConfig->GetIndexName().c_str());
        }
        const auto& keyField = kvConfig->GetFieldConfig();
        if (!keyField) {
            RETURN_IF_STATUS_ERROR(Status::ConfigError(), "no key field for [%s:%s]",
                                   indexConfig->GetIndexName().c_str(), indexConfig->GetIndexType().c_str());
        }
        auto keyExtractor =
            std::make_unique<document::KVKeyExtractor>(keyField->GetFieldType(), kvConfig->UseNumberHash());
        auto hash = config::IndexConfigHash::Hash(kvConfig);
        auto valueConvertor = std::make_unique<document::ValueConvertor>(/*parseDelete*/ false);
        auto valueConfig = kvConfig->GetValueConfig();
        auto status = valueConvertor->Init(valueConfig, /*forcePackValue*/ false);
        RETURN_IF_STATUS_ERROR(status, "init value convertor failed, [%s:%s]", indexConfig->GetIndexName().c_str(),
                               indexConfig->GetIndexType().c_str());
        _fieldResources.push_back({hash, kvConfig, std::move(keyExtractor), std::move(valueConvertor)});
    }
    return Status::OK();
}

indexlib::util::PooledUniquePtr<document::IIndexFields>
KVIndexFieldsParser::Parse(const document::ExtendDocument& extendDoc, autil::mem_pool::Pool* pool,
                           bool& hasFormatError) const
{
    hasFormatError = false;
    auto kvIndexFields = indexlib::util::MakePooledUniquePtr<KVIndexFields>(pool, pool);
    auto rawDoc = extendDoc.GetRawDocument();
    auto opType = rawDoc->getDocOperateType();
    kvIndexFields->SetDocOperateType(opType);
    for (const auto& fieldResource : _fieldResources) {
        KVIndexFields::SingleField singleField;
        auto r = ParseSingleField(fieldResource, rawDoc, pool, &singleField, &hasFormatError);
        if (!r) {
            AUTIL_LOG(ERROR, "parse kv index[%s] failed", fieldResource.kvConfig->GetIndexName().c_str());
            return nullptr;
        }
        r = kvIndexFields->AddSingleField(fieldResource.hash, singleField);
        assert(r);
    }
    return kvIndexFields;
}

bool KVIndexFieldsParser::ParseSingleField(const FieldResource& fieldResource,
                                           const std::shared_ptr<document::RawDocument>& rawDoc,
                                           autil::mem_pool::Pool* pool, KVIndexFields::SingleField* singleField,
                                           bool* hasFormatError) const
{
    if (!ParseKey(fieldResource, rawDoc, pool, singleField)) {
        return false;
    }
    const auto& kvConfig = fieldResource.kvConfig;
    ParseTTL(kvConfig, *rawDoc, singleField);
    singleField->userTimestamp = rawDoc->getDocTimestamp(); // user ts
    auto opType = rawDoc->getDocOperateType();
    if (!NeedParseValue(opType)) {
        return true;
    }
    auto result = ParseValue(fieldResource, *rawDoc, pool);
    if (!result.success) {
        AUTIL_LOG(ERROR, "parse value failed, [%s]", kvConfig->GetIndexName().c_str());
        return false;
    }
    if (result.hasFormatError) {
        singleField->hasFormatError = true;
        *hasFormatError = true;
        if (_attributeConvertErrorCounter) {
            _attributeConvertErrorCounter->Increase(1);
        }
    }
    singleField->value = autil::MakeCString(result.convertedValue, pool);
    return true;
}

indexlib::util::PooledUniquePtr<document::IIndexFields> KVIndexFieldsParser::Parse(autil::StringView serializedData,
                                                                                   autil::mem_pool::Pool* pool) const
{
    autil::DataBuffer dataBuffer(const_cast<char*>(serializedData.data()), serializedData.size());
    auto kvIndexFields = indexlib::util::MakePooledUniquePtr<KVIndexFields>(pool, pool);
    try {
        kvIndexFields->deserialize(dataBuffer);
    } catch (autil::legacy::ExceptionBase& e) {
        AUTIL_LOG(ERROR, "deserialize kv index fields failed: [%s]", e.what());
        return nullptr;
    }
    return kvIndexFields;
}

bool KVIndexFieldsParser::ParseKey(const FieldResource& fieldResource,
                                   const std::shared_ptr<document::RawDocument>& rawDoc, autil::mem_pool::Pool* pool,
                                   KVIndexFields::SingleField* singleField) const
{
    const auto& kvConfig = fieldResource.kvConfig;
    autil::StringView fieldName(kvConfig->GetFieldConfig()->GetFieldName());
    auto fieldValue = rawDoc->getField(fieldName);
    if (kvConfig->DenyEmptyPrimaryKey() && fieldValue.empty()) {
        const document::RawDocument* rawDocPtr = rawDoc.get();
        ERROR_COLLECTOR_LOG(ERROR, "key value is empty!");
        IE_RAW_DOC_TRACE(rawDocPtr, "parse error: key value is empty");
        return false;
    }

    singleField->pkFieldName = autil::MakeCString(fieldName, pool);
    singleField->pkFieldValue = autil::MakeCString(fieldValue, pool);

    uint64_t keyHash = 0;
    fieldResource.keyExtractor->HashKey(fieldValue, keyHash);
    singleField->pkeyHash = keyHash;
    return true;
}

bool KVIndexFieldsParser::NeedParseValue(DocOperateType opType) const { return opType != DELETE_DOC; }

document::ValueConvertor::ConvertResult KVIndexFieldsParser::ParseValue(const FieldResource& fieldResource,
                                                                        const document::RawDocument& rawDoc,
                                                                        autil::mem_pool::Pool* pool) const
{
    return fieldResource.valueConvertor->ConvertValue(rawDoc, pool);
}

void KVIndexFieldsParser::ParseTTL(const std::shared_ptr<config::KVIndexConfig>& kvConfig,
                                   const document::RawDocument& rawDoc, KVIndexFields::SingleField* singleField) const
{
    const auto& ttlSettings = kvConfig->GetTTLSettings();
    if (ttlSettings && ttlSettings->enabled && ttlSettings->ttlFromDoc) {
        uint32_t ttl = 0;
        autil::StringView ttlField = rawDoc.getField(autil::StringView(ttlSettings->ttlField));
        if (!ttlField.empty() && autil::StringUtil::strToUInt32(ttlField.data(), ttl)) {
            singleField->ttl = ttl;
        }
    }
}

std::shared_ptr<indexlib::util::AccumulativeCounter>
KVIndexFieldsParser::InitCounter(const std::shared_ptr<document::DocumentInitParam>& initParam) const
{
    if (!initParam) {
        return nullptr;
    }

    auto param = std::dynamic_pointer_cast<document::BuiltInParserInitParam>(initParam);
    if (!param) {
        AUTIL_LOG(WARN, "can not use BuiltInParserInitParam");
        return nullptr;
    }

    const auto& resource = param->GetResource();

    if (resource.counterMap) {
        return resource.counterMap->GetAccCounter(ATTRIBUTE_CONVERT_ERROR_COUNTER_NAME);
    }
    return nullptr;
}

} // namespace indexlibv2::index
