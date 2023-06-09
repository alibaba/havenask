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
#include "indexlib/document/kv/ValueConvertor.h"

#include "indexlib/document/RawDocument.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertor.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertorFactory.h"
#include "indexlib/index/common/field_format/pack_attribute/PackAttributeFormatter.h"
#include "indexlib/index/kv/config/ValueConfig.h"
#include "indexlib/util/DocTracer.h"
#include "indexlib/util/ErrorLogCollector.h"

namespace indexlibv2::document {
AUTIL_LOG_SETUP(indexlib.document, ValueConvertor);

ValueConvertor::ValueConvertor(bool parseDelete) : _parseDelete(parseDelete) {}

ValueConvertor::~ValueConvertor() {}

Status ValueConvertor::Init(const std::shared_ptr<config::ValueConfig>& valueConfig, bool forcePackValue)
{
    _valueConfig = valueConfig;
    size_t attrCount = valueConfig->GetAttributeCount();
    for (size_t i = 0; i < attrCount; ++i) {
        const auto& attrConfig = valueConfig->GetAttributeConfig(i);
        auto fieldId = attrConfig->GetFieldConfig()->GetFieldId();
        assert(fieldId >= 0);
        auto convertor = index::AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(attrConfig);
        if (!convertor) {
            AUTIL_LOG(ERROR, "create attr convertor for [%s] failed", attrConfig->GetIndexName().c_str());
            return Status::InternalError();
        }
        if (fieldId >= _fieldConvertors.size()) {
            _fieldConvertors.resize(fieldId + 1);
        }
        _fieldConvertors[fieldId].reset(convertor);
    }
    auto [status, packAttrConfig] = valueConfig->CreatePackAttributeConfig();
    RETURN_IF_STATUS_ERROR(status, "create pack attribute failed");
    _packAttrConfig = packAttrConfig;
    if (forcePackValue || !valueConfig->IsSimpleValue()) {
        _packFormatter = std::make_unique<index::PackAttributeFormatter>();
        if (!_packFormatter->Init(packAttrConfig)) {
            RETURN_IF_STATUS_ERROR(Status::InternalError(), "init pack attribute formatter failed");
        }
    }
    return Status::OK();
}

autil::StringView ValueConvertor::ConvertField(const RawDocument& rawDoc, const config::FieldConfig& fieldConfig,
                                               bool emptyFieldNotEncode, autil::mem_pool::Pool* pool,
                                               bool* hasFormatError) const
{
    const autil::StringView& rawField = rawDoc.getField(autil::StringView(fieldConfig.GetFieldName()));
    auto fieldId = fieldConfig.GetFieldId();
    assert(fieldId >= 0 && static_cast<size_t>(fieldId) < _fieldConvertors.size());
    const auto& convertor = _fieldConvertors[fieldId];
    assert(convertor);
    if (rawField.data() == nullptr) {
        if (fieldConfig.IsEnableNullField()) {
            return autil::StringView::empty_instance();
        }
        if (rawDoc.getDocOperateType() == UPDATE_FIELD) {
            return autil::StringView::empty_instance();
        }
        AUTIL_LOG(DEBUG, "field [%s] not exist in RawDocument", fieldConfig.GetFieldName().c_str());
        ERROR_COLLECTOR_LOG(ERROR, "field [%s] not exist in RawDocument [%s]!", fieldConfig.GetFieldName().c_str(),
                            rawDoc.toString().c_str());
        const auto rawDocPtr = &rawDoc;
        IE_RAW_DOC_FORMAT_TRACE(rawDocPtr, "parse error: field [%s] not exist in RawDocument!",
                                fieldConfig.GetFieldName().c_str());
        *hasFormatError = true;
        if (emptyFieldNotEncode) {
            return autil::StringView::empty_instance();
        }
    }
    if (fieldConfig.IsEnableNullField() && rawField == autil::StringView(fieldConfig.GetNullFieldLiteralString())) {
        return autil::StringView::empty_instance();
    }
    return convertor->Encode(rawField, pool, *hasFormatError);
}

ValueConvertor::ConvertResult ValueConvertor::ConvertValue(const RawDocument& rawDoc, autil::mem_pool::Pool* pool)
{
    DocOperateType opType = rawDoc.getDocOperateType();
    bool validDoc = opType == ADD_DOC || opType == UPDATE_FIELD || (opType == DELETE_DOC && _parseDelete);
    if (!validDoc) {
        return ConvertResult();
    }
    auto attrCount = _valueConfig->GetAttributeCount();
    std::vector<autil::StringView> fixLenFields;
    std::vector<attrid_t> fixLenAttrIds;
    std::vector<autil::StringView> varLenFields;
    std::vector<attrid_t> varLenAttrIds;
    bool hasFormatError = false;
    for (size_t i = 0; i < attrCount; ++i) {
        const auto& attrConfig = _valueConfig->GetAttributeConfig(i);
        bool emptyFieldNotEncode = (opType == UPDATE_FIELD);
        assert(attrConfig->GetFieldId() >= 0 && attrConfig->GetFieldId() < _fieldConvertors.size());
        bool formatError = false;
        auto result = ConvertField(rawDoc, *attrConfig->GetFieldConfig(), emptyFieldNotEncode, pool, &formatError);
        hasFormatError |= formatError;
        if (attrConfig->IsLengthFixed()) {
            fixLenFields.push_back(std::move(result));
            fixLenAttrIds.push_back(attrConfig->GetAttrId());
        } else {
            varLenFields.push_back(std::move(result));
            varLenAttrIds.push_back(attrConfig->GetAttrId());
        }
    }
    std::vector<autil::StringView> fields(std::move(fixLenFields));
    fields.insert(fields.end(), varLenFields.begin(), varLenFields.end());
    std::vector<attrid_t> attrIds(std::move(fixLenAttrIds));
    attrIds.insert(attrIds.end(), varLenAttrIds.begin(), varLenAttrIds.end());
    autil::StringView convertedValue;
    if (_packFormatter) {
        if (opType == UPDATE_FIELD) {
            index::PackAttributeFormatter::PackAttributeFields patchFields;
            patchFields.reserve(fields.size());
            for (size_t i = 0; i < fields.size(); ++i) {
                const auto& field = fields[i];
                if (!field.empty()) {
                    patchFields.emplace_back(attrIds[i], field);
                }
            }
            auto bufferLen = _packFormatter->GetEncodePatchValueLen(patchFields);
            auto buffer = pool->allocate(bufferLen);
            size_t encodeLen = _packFormatter->EncodePatchValues(patchFields, (uint8_t*)buffer, bufferLen);
            autil::StringView patchAttrField((char*)buffer, encodeLen);
            convertedValue = patchAttrField;
            if (convertedValue.empty()) {
                AUTIL_LOG(WARN, "encode patch attr fields failed!");
                ERROR_COLLECTOR_LOG(WARN, "encode patch attr fields failed!");
                return ConvertResult();
            }
        } else {
            convertedValue = _packFormatter->Format(fields, pool);
            if (convertedValue.empty()) {
                AUTIL_LOG(WARN, "format pack attr fields fail!");
                ERROR_COLLECTOR_LOG(WARN, "format pack attr fields fail!");
                return ConvertResult();
            }
        }
    } else {
        assert(fields.size() == 1u);
        convertedValue = fields[0];
    }
    return ConvertResult(convertedValue, hasFormatError, true);
}

} // namespace indexlibv2::document
