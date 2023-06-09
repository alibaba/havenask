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
#include "indexlib/document/aggregation/DeleteValueEncoder.h"

#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertorFactory.h"

namespace indexlibv2::document {

Status DeleteValueEncoder::Init(const std::shared_ptr<config::ValueConfig>& valueConfig)
{
    _valueSize = valueConfig->GetFixedLength();
    if (_valueSize <= 0) {
        return Status::ConfigError("delete data must be fixed-size");
    }
    for (size_t i = 0; i < valueConfig->GetAttributeCount(); ++i) {
        const auto& attrConfig = valueConfig->GetAttributeConfig(i);
        std::unique_ptr<index::AttributeConvertor> convertor(
            index::AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(attrConfig, /*encodeEmpty=*/true));
        if (!convertor) {
            return Status::InternalError("create convertor for %s failed", attrConfig->GetAttrName().c_str());
        }
        _attrConvertors.emplace_back(std::move(convertor));
    }
    return Status::OK();
}

StatusOr<autil::StringView> DeleteValueEncoder::Encode(const RawDocument& rawDoc, autil::mem_pool::Pool* pool)
{
    char* buf = (char*)pool->allocate(_valueSize);
    size_t pos = 0;
    for (const auto& convertor : _attrConvertors) {
        const auto& fieldValue = rawDoc.getField(autil::StringView(convertor->GetFieldName()));
        auto encoded = convertor->Encode(fieldValue, pool);
        auto value = convertor->Decode(encoded).data;
        if (pos + value.size() > _valueSize) {
            return {Status::InternalError("encode for %s failed", convertor->GetFieldName().c_str())};
        }
        memcpy(buf + pos, value.data(), value.size());
        pos += value.size();
    }
    if (pos != _valueSize) {
        return {Status::InternalError("value incorrect")};
    }
    return {autil::StringView(buf, _valueSize)};
}

} // namespace indexlibv2::document
