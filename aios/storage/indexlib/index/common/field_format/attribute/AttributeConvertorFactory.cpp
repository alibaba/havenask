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
#include "indexlib/index/common/field_format/attribute/AttributeConvertorFactory.h"

#include <string>

#include "indexlib/config/CompressTypeOption.h"
#include "indexlib/config/FieldConfig.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/attribute/config/PackAttributeConfig.h"
#include "indexlib/index/common/field_format/attribute/CompressFloatAttributeConvertor.h"
#include "indexlib/index/common/field_format/attribute/CompressSingleFloatAttributeConvertor.h"
#include "indexlib/index/common/field_format/attribute/DateAttributeConvertor.h"
#include "indexlib/index/common/field_format/attribute/LineAttributeConvertor.h"
#include "indexlib/index/common/field_format/attribute/LocationAttributeConvertor.h"
#include "indexlib/index/common/field_format/attribute/MultiStringAttributeConvertor.h"
#include "indexlib/index/common/field_format/attribute/MultiValueAttributeConvertor.h"
#include "indexlib/index/common/field_format/attribute/PolygonAttributeConvertor.h"
#include "indexlib/index/common/field_format/attribute/SingleValueAttributeConvertor.h"
#include "indexlib/index/common/field_format/attribute/StringAttributeConvertor.h"
#include "indexlib/index/common/field_format/attribute/TimeAttributeConvertor.h"
#include "indexlib/index/common/field_format/attribute/TimestampAttributeConvertor.h"

// compatitable with legacy code, when FieldConfig/PackAttributeConfig has replaced in new dir, delete it
using namespace indexlib::config;

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, AttributeConvertorFactory);

AttributeConvertor* AttributeConvertorFactory::CreatePackAttrConvertor(
    const std::shared_ptr<indexlibv2::config::PackAttributeConfig>& packAttrConfig)
{
    return CreateAttrConvertor(packAttrConfig->CreateAttributeConfig());
}

AttributeConvertor*
AttributeConvertorFactory::CreateAttrConvertor(const std::shared_ptr<config::AttributeConfig>& attrConfig)
{
    if (attrConfig->IsMultiValue()) {
        return CreateMultiAttrConvertor(attrConfig);
    } else {
        return CreateSingleAttrConvertor(attrConfig);
    }
}

AttributeConvertor*
AttributeConvertorFactory::CreateAttrConvertor(const std::shared_ptr<config::AttributeConfig>& attrConfig,
                                               bool encodeEmpty)
{
    AttributeConvertor* convertor = CreateAttrConvertor(attrConfig);
    if (convertor && encodeEmpty) {
        // kv & kkv table not support update, should encode empty field
        // only update_field document not encode empty field for single value
        convertor->SetEncodeEmpty(true);
    }
    return convertor;
}

AttributeConvertor*
AttributeConvertorFactory::CreateSingleAttrConvertor(const std::shared_ptr<config::AttributeConfig>& attrConfig)
{
    auto fieldConfigs = attrConfig->GetFieldConfigs();
    assert(fieldConfigs.size() == 1);
    const auto& fieldConfig = fieldConfigs[0];
    FieldType fieldType = attrConfig->GetFieldType();
    bool needHash = attrConfig->IsUniqEncode();
    const std::string& fieldName = fieldConfig->GetFieldName();
    auto compressType = attrConfig->GetCompressType();
    switch (fieldType) {
    case ft_string:
        return new StringAttributeConvertor(needHash, fieldName, attrConfig->GetFixedMultiValueCount());
    case ft_int32:
        return new SingleValueAttributeConvertor<int32_t>(needHash, fieldName);
    case ft_float:
        if (compressType.HasFp16EncodeCompress()) {
            return new CompressSingleFloatAttributeConvertor<int16_t>(compressType, fieldName);
        }
        if (compressType.HasInt8EncodeCompress()) {
            return new CompressSingleFloatAttributeConvertor<int8_t>(compressType, fieldName);
        }
        return new SingleValueAttributeConvertor<float>(needHash, fieldName);
    case ft_double:
        return new SingleValueAttributeConvertor<double>(needHash, fieldName);
    case ft_int64:
        return new SingleValueAttributeConvertor<int64_t>(needHash, fieldName);
    case ft_uint32:
        return new SingleValueAttributeConvertor<uint32_t>(needHash, fieldName);
    case ft_uint64:
    case ft_hash_64: // uint64:  only used for primary key
        return new SingleValueAttributeConvertor<uint64_t>(needHash, fieldName);
    case ft_hash_128: // uint128: only used for primary key
        return new SingleValueAttributeConvertor<autil::uint128_t>(needHash, fieldName);
    case ft_fp8:
    case ft_int8:
        return new SingleValueAttributeConvertor<int8_t>(needHash, fieldName);
    case ft_uint8:
        return new SingleValueAttributeConvertor<uint8_t>(needHash, fieldName);
    case ft_fp16:
    case ft_int16:
        return new SingleValueAttributeConvertor<int16_t>(needHash, fieldName);
    case ft_uint16:
        return new SingleValueAttributeConvertor<uint16_t>(needHash, fieldName);
    case ft_time:
        return new TimeAttributeConvertor(needHash, fieldName);
    case ft_timestamp:
        return new TimestampAttributeConvertor(needHash, fieldName, fieldConfig->GetDefaultTimeZoneDelta());
    case ft_date:
        return new DateAttributeConvertor(needHash, fieldName);
    case ft_location:
    case ft_line:
    case ft_polygon:
    case ft_online:
    case ft_property:
    case ft_enum:
    case ft_text:
    case ft_unknown:
    default:
        AUTIL_LOG(ERROR, "Unsupport Attribute Convertor: fieldType:[%s]",
                  indexlibv2::config::FieldConfig::FieldTypeToStr(fieldType));
        return nullptr;
    }
    return nullptr;
}

AttributeConvertor*
AttributeConvertorFactory::CreateMultiAttrConvertor(const std::shared_ptr<config::AttributeConfig>& attrConfig)
{
    auto fieldConfigs = attrConfig->GetFieldConfigs();
    assert(fieldConfigs.size() == 1);
    const auto& fieldConfig = fieldConfigs[0];
    FieldType fieldType = attrConfig->GetFieldType();
    bool needHash = attrConfig->IsUniqEncode();
    const std::string& fieldName = attrConfig->GetAttrName();
    bool isBinary = fieldConfig->IsBinary();
    int32_t fixedValueCount = attrConfig->GetFixedMultiValueCount();
    CompressTypeOption compressType = attrConfig->GetCompressType();
    const std::string& separator = fieldConfig->GetSeparator();

    switch (fieldType) {
    case ft_string:
        return new MultiStringAttributeConvertor(needHash, fieldName, isBinary, separator);
    case ft_int32:
        return new MultiValueAttributeConvertor<int32_t>(needHash, fieldName, fixedValueCount, separator);
    case ft_float:
        if (compressType.HasBlockFpEncodeCompress() || compressType.HasFp16EncodeCompress() ||
            compressType.HasInt8EncodeCompress()) {
            return new CompressFloatAttributeConvertor(compressType, needHash, fieldName, fixedValueCount, separator);
        }
        return new MultiValueAttributeConvertor<float>(needHash, fieldName, fixedValueCount, separator);
    case ft_double:
        return new MultiValueAttributeConvertor<double>(needHash, fieldName, fixedValueCount, separator);
    case ft_location:
        return new LocationAttributeConvertor(needHash, fieldName, separator);
    case ft_line:
        return new LineAttributeConvertor(needHash, fieldName, separator);
    case ft_polygon:
        return new PolygonAttributeConvertor(needHash, fieldName, separator);
    case ft_int64:
        return new MultiValueAttributeConvertor<int64_t>(needHash, fieldName, fixedValueCount, separator);
    case ft_uint32:
        return new MultiValueAttributeConvertor<uint32_t>(needHash, fieldName, fixedValueCount, separator);
    case ft_uint64:
    case ft_hash_64: // uint64:  only used for primary key
        return new MultiValueAttributeConvertor<uint64_t>(needHash, fieldName, fixedValueCount, separator);
    case ft_hash_128: // uint128: only used for primary key
        return new MultiValueAttributeConvertor<autil::uint128_t>(needHash, fieldName, -1, separator);
    case ft_int8:
        return new MultiValueAttributeConvertor<int8_t>(needHash, fieldName, fixedValueCount, separator);
    case ft_uint8:
        return new MultiValueAttributeConvertor<uint8_t>(needHash, fieldName, fixedValueCount, separator);
    case ft_int16:
        return new MultiValueAttributeConvertor<int16_t>(needHash, fieldName, fixedValueCount, separator);
    case ft_uint16:
        return new MultiValueAttributeConvertor<uint16_t>(needHash, fieldName, fixedValueCount, separator);

    case ft_fp8: // multi value not support ft_fp8 & ft_fp16
    case ft_fp16:
    case ft_time:
    case ft_timestamp:
    case ft_date:
        assert(false);

    case ft_online:
    case ft_property:
    case ft_enum:
    case ft_text:
    case ft_unknown:
    default:
        AUTIL_LOG(ERROR, "unsupport attribute convertor, field type[%s]",
                  indexlibv2::config::FieldConfig::FieldTypeToStr(fieldType));
        return nullptr;
    }
    return nullptr;
}
} // namespace indexlibv2::index
