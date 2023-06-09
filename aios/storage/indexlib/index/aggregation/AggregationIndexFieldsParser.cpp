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
#include "indexlib/index/aggregation/AggregationIndexFieldsParser.h"

#include "autil/ConstString.h"
#include "indexlib/config/IndexConfigHash.h"
#include "indexlib/document/ExtendDocument.h"
#include "indexlib/document/RawDocument.h"
#include "indexlib/index/aggregation/AggregationIndexConfig.h"
#include "indexlib/index/aggregation/AggregationKeyHasher.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertor.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertorFactory.h"
#include "indexlib/index/kv/config/ValueConfig.h"
#include "indexlib/util/ErrorLogCollector.h"
#include "indexlib/util/PooledUniquePtr.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, AggregationIndexFieldsParser);

AggregationIndexFieldsParser::~AggregationIndexFieldsParser() {}

Status AggregationIndexFieldsParser::Init(const std::vector<std::shared_ptr<config::IIndexConfig>>& indexConfigs,
                                          const std::shared_ptr<document::DocumentInitParam>& initParam)
{
    std::shared_ptr<config::FieldConfig> versionField;
    for (const auto& indexConfig : indexConfigs) {
        const auto& aggregationConfig = std::dynamic_pointer_cast<config::AggregationIndexConfig>(indexConfig);
        if (!aggregationConfig) {
            RETURN_IF_STATUS_ERROR(Status::InternalError(),
                                   "cast to AggregationIndexConfig failed, indexType[%s] indexName[%s]",
                                   indexConfig->GetIndexType().c_str(), indexConfig->GetIndexName().c_str());
        }
        auto valueConvertor = std::make_unique<document::ValueConvertor>(/*parseDelete*/ false);
        auto valueConfig = aggregationConfig->GetValueConfig();
        auto status = valueConvertor->Init(valueConfig, /*forcePackValue*/ true);
        RETURN_IF_STATUS_ERROR(status, "init value convertor failed, [%s:%s]", indexConfig->GetIndexName().c_str(),
                               indexConfig->GetIndexType().c_str());
        std::unique_ptr<document::DeleteValueEncoder> deleteValueEncoder;
        if (aggregationConfig->SupportDelete()) {
            const auto& deleteConfig = aggregationConfig->GetDeleteConfig();
            const auto& valueConfig = deleteConfig->GetValueConfig();
            if (valueConfig->GetFixedLength() < 0) {
                RETURN_IF_STATUS_ERROR(Status::Unimplement(), "delete data must be fixed size, [%s:%s]",
                                       indexConfig->GetIndexName().c_str(), indexConfig->GetIndexType().c_str());
            }
            deleteValueEncoder = std::make_unique<document::DeleteValueEncoder>();
            status = deleteValueEncoder->Init(valueConfig);
            RETURN_IF_STATUS_ERROR(status, "delete value encoder init failed");
        }
        versionField = aggregationConfig->GetVersionField();
        auto hash = config::IndexConfigHash::Hash(aggregationConfig);
        _fieldResources.push_back({hash, aggregationConfig, std::move(valueConvertor), std::move(deleteValueEncoder)});
    }

    if (versionField) {
        auto attrConfig = std::make_shared<config::AttributeConfig>();
        attrConfig->Init(versionField);
        _versionConvertor.reset(
            index::AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(attrConfig, /*encodeEmpty=*/true));
        if (!_versionConvertor) {
            RETURN_IF_STATUS_ERROR(Status::InternalError(), "create version convertor failed");
        }
    }

    return Status::OK();
}

indexlib::util::PooledUniquePtr<document::IIndexFields>
AggregationIndexFieldsParser::Parse(const document::ExtendDocument& extendDoc, autil::mem_pool::Pool* pool)
{
    auto aggregationIndexFields = indexlib::util::MakePooledUniquePtr<AggregationIndexFields>(pool, pool);
    auto rawDoc = extendDoc.getRawDocument();
    for (const auto& fieldResource : _fieldResources) {
        AggregationIndexFields::SingleField singleField;
        auto r = ParseSingleField(fieldResource, rawDoc, pool, &singleField);
        if (!r) {
            AUTIL_LOG(ERROR, "parse aggregation index field [%s] failed",
                      fieldResource.aggregationConfig->GetIndexName().c_str());
            return nullptr;
        }
        aggregationIndexFields->AddSingleField(singleField);
    }
    if (_versionConvertor) {
        auto versionField = rawDoc->getField(autil::StringView(_versionConvertor->GetFieldName()));
        auto encoded = _versionConvertor->Encode(versionField, pool);
        auto value = _versionConvertor->Decode(encoded).data;
        aggregationIndexFields->SetVersion(value);
    }
    return aggregationIndexFields;
}

bool AggregationIndexFieldsParser::ParseSingleField(const FieldResource& fieldResource,
                                                    const std::shared_ptr<document::RawDocument>& rawDoc,
                                                    autil::mem_pool::Pool* pool,
                                                    AggregationIndexFields::SingleField* singleField) const
{
    singleField->indexNameHash = fieldResource.hash;
    if (!ParseKey(fieldResource, rawDoc, pool, singleField)) {
        return false;
    }
    const auto& aggregationConfig = fieldResource.aggregationConfig;
    // TODO: support ttl
    auto opType = rawDoc->getDocOperateType();
    singleField->opType = opType;
    if (!NeedParseValue(aggregationConfig, opType)) {
        return true;
    }
    auto result = ParseValue(fieldResource, *rawDoc, pool);
    if (!result.success) {
        AUTIL_LOG(ERROR, "parse value failed, [%s]", aggregationConfig->GetIndexName().c_str());
        return false;
    }
    singleField->value = autil::MakeCString(result.convertedValue, pool);
    return true;
}

indexlib::util::PooledUniquePtr<document::IIndexFields>
AggregationIndexFieldsParser::Parse(autil::StringView serializedData, autil::mem_pool::Pool* pool)
{
    autil::DataBuffer dataBuffer(const_cast<char*>(serializedData.data()), serializedData.size());
    auto aggregationIndexFields = indexlib::util::MakePooledUniquePtr<AggregationIndexFields>(pool, pool);
    try {
        aggregationIndexFields->deserialize(dataBuffer);
    } catch (autil::legacy::ExceptionBase& e) {
        AUTIL_LOG(ERROR, "deserialize kv index fields failed: [%s]", e.what());
        return nullptr;
    }
    return aggregationIndexFields;
}

bool AggregationIndexFieldsParser::ParseKey(const FieldResource& fieldResource,
                                            const std::shared_ptr<document::RawDocument>& rawDoc,
                                            autil::mem_pool::Pool* pool,
                                            AggregationIndexFields::SingleField* singleField) const
{
    uint64_t hash = Hash(fieldResource.aggregationConfig, *rawDoc);
    singleField->pkeyHash = hash;
    return true;
}

uint64_t AggregationIndexFieldsParser::Hash(const std::shared_ptr<config::AggregationIndexConfig>& aggregationConfig,
                                            const document::RawDocument& doc) const
{
    const auto& keyFields = aggregationConfig->GetKeyFields();
    if (keyFields.size() == 1) {
        return index::AggregationKeyHasher::Hash(GetFieldValue(doc, keyFields[0]->GetFieldName()));
    } else if (keyFields.size() == 2) {
        return index::AggregationKeyHasher::Hash(GetFieldValue(doc, keyFields[0]->GetFieldName()),
                                                 GetFieldValue(doc, keyFields[1]->GetFieldName()));
    } else if (keyFields.size() == 3) {
        return index::AggregationKeyHasher::Hash(GetFieldValue(doc, keyFields[0]->GetFieldName()),
                                                 GetFieldValue(doc, keyFields[1]->GetFieldName()),
                                                 GetFieldValue(doc, keyFields[2]->GetFieldName()));
    } else {
        std::vector<autil::StringView> values;
        values.reserve(keyFields.size());
        for (size_t i = 0; i < keyFields.size(); ++i) {
            const auto& fieldName = keyFields[i]->GetFieldName();
            autil::StringView key(fieldName.c_str(), fieldName.size());
            values.emplace_back(doc.getField(key));
        }
        return index::AggregationKeyHasher::Hash(values);
    }
}

autil::StringView AggregationIndexFieldsParser::GetFieldValue(const document::RawDocument& doc,
                                                              const std::string& fieldName) const
{
    autil::StringView key(fieldName.c_str(), fieldName.size());
    return doc.getField(key);
}

bool AggregationIndexFieldsParser::NeedParseValue(
    const std::shared_ptr<config::AggregationIndexConfig>& aggregationConfig, DocOperateType opType) const
{
    return opType == ADD_DOC || (opType == DELETE_DOC && aggregationConfig->SupportDelete());
}

document::ValueConvertor::ConvertResult AggregationIndexFieldsParser::ParseValue(const FieldResource& fieldResource,
                                                                                 const document::RawDocument& rawDoc,
                                                                                 autil::mem_pool::Pool* pool) const
{
    auto docType = rawDoc.getDocOperateType();
    if (docType != DELETE_DOC || !fieldResource.aggregationConfig->SupportDelete()) {
        return fieldResource.valueConvertor->ConvertValue(rawDoc, pool);
    }
    document::ValueConvertor::ConvertResult result;
    auto r = fieldResource.deleteValueEncoder->Encode(rawDoc, pool);
    if (!r.IsOK()) {
        result.success = false;
    } else {
        result.success = true;
        result.convertedValue = std::move(r.steal_value());
    }
    return result;
}

} // namespace indexlibv2::index
