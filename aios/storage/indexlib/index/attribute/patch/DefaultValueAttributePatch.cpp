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
#include "indexlib/index/attribute/patch/DefaultValueAttributePatch.h"

#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertorFactory.h"
#include "indexlib/index/common/field_format/attribute/CompressFloatAttributeConvertor.h"
#include "indexlib/index/common/field_format/attribute/CompressSingleFloatAttributeConvertor.h"
#include "indexlib/index/common/field_format/attribute/DateAttributeConvertor.h"
#include "indexlib/index/common/field_format/attribute/DefaultAttributeValueInitializer.h"
#include "indexlib/index/common/field_format/attribute/LineAttributeConvertor.h"
#include "indexlib/index/common/field_format/attribute/LocationAttributeConvertor.h"
#include "indexlib/index/common/field_format/attribute/MultiStringAttributeConvertor.h"
#include "indexlib/index/common/field_format/attribute/MultiValueAttributeConvertor.h"
#include "indexlib/index/common/field_format/attribute/PolygonAttributeConvertor.h"
#include "indexlib/index/common/field_format/attribute/SingleValueAttributeConvertor.h"
#include "indexlib/index/common/field_format/attribute/StringAttributeConvertor.h"
#include "indexlib/index/common/field_format/attribute/TimeAttributeConvertor.h"
#include "indexlib/index/common/field_format/attribute/TimestampAttributeConvertor.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, DefaultValueAttributePatch);

Status DefaultValueAttributePatch::Open(const std::shared_ptr<config::AttributeConfig>& attrConfig)
{
    auto [status, defaultValue] = GetDecodedDefaultValue(attrConfig, &_pool);
    RETURN_STATUS_DIRECTLY_IF_ERROR(status);
    _defaultValue = defaultValue;
    return Status::OK();
}

std::pair<Status, autil::StringView>
DefaultValueAttributePatch::GetDecodedDefaultValue(const std::shared_ptr<config::AttributeConfig>& attrConfig,
                                                   autil::mem_pool::Pool* pool)
{
    auto fieldConfigs = attrConfig->GetFieldConfigs();
    assert(fieldConfigs.size() == 1);
    const auto& fieldConfig = fieldConfigs[0];
    if (fieldConfig->IsEnableNullField()) {
        std::string errorInfo =
            "attribute [" + fieldConfig->GetFieldName() + "] not support null in default value indexer";
        AUTIL_LOG(ERROR, "%s", errorInfo.c_str());
        return {Status::ConfigError(errorInfo.c_str()), autil::StringView()};
    }

    std::string defaultValue = fieldConfig->GetDefaultValue();
    if (defaultValue.empty()) {
        defaultValue = DefaultAttributeValueInitializer::GetDefaultValueStr(fieldConfig);
    }

    std::unique_ptr<AttributeConvertor> convertor(
        AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(attrConfig));
    convertor->SetEncodeEmpty(true);
    bool hasFormatError = false;
    autil::StringView value =
        convertor->Encode(autil::StringView(defaultValue.data(), defaultValue.length()), pool, hasFormatError);
    if (hasFormatError) {
        std::string errorInfo = "attribute [" + fieldConfig->GetFieldName() + "]" + " with default value [" +
                                defaultValue + "] has format error";
        AUTIL_LOG(ERROR, "%s", errorInfo.c_str());
        return {Status::ConfigError(errorInfo.c_str()), autil::StringView()};
    }
    AttrValueMeta valueMeta = convertor->Decode(value);
    return {Status::OK(), valueMeta.data};
}

void DefaultValueAttributePatch::SetPatchReader(const std::shared_ptr<IAttributePatch>& patchReader)
{
    assert(!_patchReader);
    _patchReader = patchReader;
}

std::pair<Status, size_t> DefaultValueAttributePatch::Seek(docid_t docId, uint8_t* value, size_t maxLen, bool& isNull)
{
    if (_patchReader) {
        auto [status, ret] = _patchReader->Seek(docId, value, maxLen, isNull);
        RETURN2_IF_STATUS_ERROR(status, 0, "read patch info fail");
        if (ret) {
            return {status, ret};
        }
    }

    assert(maxLen >= _defaultValue.size());
    memcpy(value, _defaultValue.data(), _defaultValue.size());
    isNull = false;
    return {Status::OK(), _defaultValue.size()};
}

bool DefaultValueAttributePatch::UpdateField(docid_t docId, const autil::StringView& value, bool isNull)
{
    assert(false);
    return false;
}

uint32_t DefaultValueAttributePatch::GetMaxPatchItemLen() const
{
    uint32_t ret = _defaultValue.size();
    if (_patchReader) {
        ret = std::max(ret, _patchReader->GetMaxPatchItemLen());
    }
    return ret;
}

} // namespace indexlibv2::index
