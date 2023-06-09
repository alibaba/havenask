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
#include "indexlib/document/normal/AttributeDocumentFieldExtractor.h"

#include "autil/mem_pool/Pool.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/document/normal/AttributeDocument.h"
#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/attribute/config/PackAttributeConfig.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertor.h"
#include "indexlib/util/ErrorLogCollector.h"

namespace indexlibv2::document {
AUTIL_LOG_SETUP(indexlib.framework, AttributeDocumentFieldExtractor);

#define TABLET_LOG(level, format, args...)                                                                             \
    AUTIL_LOG(level, "[%s] [%p] " format, _schema->GetTableName().c_str(), this, ##args)

Status AttributeDocumentFieldExtractor::Init(const std::shared_ptr<config::ITabletSchema>& schema)
{
    assert(schema);
    auto indexConfigs = schema->GetIndexConfigs(index::ATTRIBUTE_INDEX_TYPE_STR);
    for (const auto& indexConfig : indexConfigs) {
        const auto& attrConfig = std::dynamic_pointer_cast<config::AttributeConfig>(indexConfig);
        if (attrConfig) {
            AddAttributeConfig(attrConfig);
        } else {
            const auto& packAttrConfig = std::dynamic_pointer_cast<config::PackAttributeConfig>(indexConfig);
            if (!packAttrConfig) {
                TABLET_LOG(ERROR, "unrecognized attribute[%s]", indexConfig->GetIndexName().c_str());
                return Status::Corruption("unrecognized attribute");
            }
            auto formatter = std::make_unique<index::PackAttributeFormatter>();
            if (!formatter->Init(packAttrConfig)) {
                TABLET_LOG(ERROR, "init pack attribute formatter for attr[%s] failed",
                           indexConfig->GetIndexName().c_str());
                return Status::InternalError("init pack attr formatter failed");
            }
            AddPackAttributeFormatter(std::move(formatter), packAttrConfig->GetPackAttrId());
            for (const auto& configInPack : packAttrConfig->GetAttributeConfigVec()) {
                AddAttributeConfig(configInPack);
            }
        }
    }
    return Status::OK();
}

void AttributeDocumentFieldExtractor::AddPackAttributeFormatter(
    std::unique_ptr<index::PackAttributeFormatter> formatter, packattrid_t packId)
{
    if (packId >= _packFormatters.size()) {
        _packFormatters.resize(packId + 1);
    }
    _packFormatters[packId] = std::move(formatter);
}

void AttributeDocumentFieldExtractor::AddAttributeConfig(const std::shared_ptr<config::AttributeConfig>& attrConfig)
{
    auto fieldId = attrConfig->GetFieldId();
    assert(fieldId >= 0);
    if (static_cast<size_t>(fieldId) >= _attrConfigs.size()) {
        _attrConfigs.resize(fieldId + 1);
    }
    _attrConfigs[fieldId] = attrConfig;
}

const std::shared_ptr<config::AttributeConfig>&
AttributeDocumentFieldExtractor::GetAttributeConfig(fieldid_t fieldId) const
{
    if (fieldId < 0 || static_cast<size_t>(fieldId) >= _attrConfigs.size()) {
        static std::shared_ptr<config::AttributeConfig> NULL_ATTR_CONF;
        return NULL_ATTR_CONF;
    }
    return _attrConfigs[fieldId];
}

autil::StringView
AttributeDocumentFieldExtractor::GetField(const std::shared_ptr<indexlib::document::AttributeDocument>& attrDoc,
                                          fieldid_t fieldId, autil::mem_pool::Pool* pool)
{
    if (!attrDoc) {
        return autil::StringView::empty_instance();
    }

    if (attrDoc->HasField(fieldId)) {
        return attrDoc->GetField(fieldId);
    }

    // find field in pack attribute
    const auto& attrConfig = GetAttributeConfig(fieldId);
    if (!attrConfig) {
        TABLET_LOG(ERROR, "get field[%d] failed, it does not belong to any attribute.", fieldId);
        ERROR_COLLECTOR_LOG(ERROR, "get field[%d] failed, it does not belong to any attribute.", fieldId);
        return autil::StringView::empty_instance();
    }

    config::PackAttributeConfig* packAttrConfig = attrConfig->GetPackAttributeConfig();
    if (!packAttrConfig) {
        // empty single value field will trigger this log
        TABLET_LOG(DEBUG,
                   "get field[%d] failed, "
                   "it is not in attributeDocument.",
                   fieldId);
        ERROR_COLLECTOR_LOG(ERROR,
                            "get field[%d] failed, "
                            "it is not in attributeDocument.",
                            fieldId);
        return autil::StringView::empty_instance();
    }

    packattrid_t packId = packAttrConfig->GetPackAttrId();
    const auto& formatter = _packFormatters[packId];
    assert(formatter);
    const autil::StringView& packValue = attrDoc->GetPackField(packId);
    if (packValue.empty()) {
        TABLET_LOG(ERROR,
                   "get field[%d] failed, "
                   "due to pack attribute[%ud] is empty.",
                   fieldId, packId);
        ERROR_COLLECTOR_LOG(ERROR,
                            "get field[%d] failed, "
                            "due to pack attribute[%ud] is empty.",
                            fieldId, packId);
        return autil::StringView::empty_instance();
    }

    // rawValue is un-encoded fieldValue
    auto rawField = formatter->GetFieldValueFromPackedValue(packValue, attrConfig->GetAttrId());
    auto convertor = formatter->GetAttributeConvertorByAttrId(attrConfig->GetAttrId());
    assert(pool);
    uint64_t dummyKey = uint64_t(-1);
    if (rawField.isFixedValueLen || rawField.hasCountInValueStr) {
        index::AttrValueMeta attrValueMeta(dummyKey, rawField.valueStr);
        return convertor->EncodeFromAttrValueMeta(attrValueMeta, pool);
    }

    // in pack value store without count, need encode count
    size_t bufferSize =
        rawField.valueStr.size() + autil::MultiValueFormatter::getEncodedCountLength(rawField.valueCount);
    char* buffer = (char*)pool->allocate(bufferSize);
    size_t countLen = autil::MultiValueFormatter::encodeCount(rawField.valueCount, buffer, bufferSize);
    memcpy(buffer + countLen, rawField.valueStr.data(), rawField.valueStr.size());
    index::AttrValueMeta attrValueMeta(dummyKey, autil::StringView(buffer, bufferSize));
    return convertor->EncodeFromAttrValueMeta(attrValueMeta, pool);
}

} // namespace indexlibv2::document
