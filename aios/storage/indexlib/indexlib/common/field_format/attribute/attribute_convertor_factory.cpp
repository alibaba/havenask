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
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"

#include "indexlib/common/field_format/attribute/compress_float_attribute_convertor.h"
#include "indexlib/common/field_format/attribute/compress_single_float_attribute_convertor.h"
#include "indexlib/common/field_format/attribute/date_attribute_convertor.h"
#include "indexlib/common/field_format/attribute/line_attribute_convertor.h"
#include "indexlib/common/field_format/attribute/location_attribute_convertor.h"
#include "indexlib/common/field_format/attribute/multi_string_attribute_convertor.h"
#include "indexlib/common/field_format/attribute/polygon_attribute_convertor.h"
#include "indexlib/common/field_format/attribute/single_value_attribute_convertor.h"
#include "indexlib/common/field_format/attribute/string_attribute_convertor.h"
#include "indexlib/common/field_format/attribute/time_attribute_convertor.h"
#include "indexlib/common/field_format/attribute/timestamp_attribute_convertor.h"
#include "indexlib/common/field_format/attribute/var_num_attribute_convertor.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace indexlib::config;

namespace indexlib { namespace common {
IE_LOG_SETUP(common, AttributeConvertorFactory);

AttributeConvertorFactory::~AttributeConvertorFactory() {}

AttributeConvertor*
AttributeConvertorFactory::CreateAttrConvertor(const std::shared_ptr<indexlib::config::AttributeConfig>& attrConf,
                                               TableType tableType)
{
    AttributeConvertor* convertor = CreateAttrConvertor(attrConf);
    if (convertor && (tableType == tt_kv || tableType == tt_kkv)) {
        // kv & kkv table not support update, should encode empty field
        // only update_field document not encode empty field for single value
        convertor->SetEncodeEmpty(true);
    }
    return convertor;
}

AttributeConvertor* AttributeConvertorFactory::CreatePackAttrConvertor(const PackAttributeConfigPtr& packAttrConfig)
{
    return CreateAttrConvertor(packAttrConfig->CreateAttributeConfig());
}

AttributeConvertor*
AttributeConvertorFactory::CreateAttrConvertor(const std::shared_ptr<indexlib::config::AttributeConfig>& attrConfig)
{
    if (attrConfig->IsMultiValue()) {
        return CreateMultiAttrConvertor(attrConfig);
    } else {
        return CreateSingleAttrConvertor(attrConfig);
    }
}

AttributeConvertor* AttributeConvertorFactory::CreateSingleAttrConvertor(const AttributeConfigPtr& attrConfig)
{
    FieldType fieldType = attrConfig->GetFieldType();
    bool needHash = attrConfig->IsUniqEncode();
    const string& fieldName = attrConfig->GetAttrName();
    CompressTypeOption compressType = attrConfig->GetCompressType();
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
        return new TimestampAttributeConvertor(needHash, fieldName,
                                               attrConfig->GetFieldConfig()->GetDefaultTimeZoneDelta());
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
        stringstream ss;
        ss << "Unsupport Attribute Convertor: fieldType = " << FieldConfig::FieldTypeToStr(fieldType);
        INDEXLIB_THROW(util::UnImplementException, "%s", ss.str().c_str());
        return NULL;
    }
    return NULL;
}

AttributeConvertor* AttributeConvertorFactory::CreateMultiAttrConvertor(const AttributeConfigPtr& attrConfig)
{
    FieldType fieldType = attrConfig->GetFieldType();
    bool needHash = attrConfig->IsUniqEncode();
    const string& fieldName = attrConfig->GetAttrName();
    bool isBinary = attrConfig->GetFieldConfig()->IsBinary();
    int32_t fixedValueCount = attrConfig->GetFixedMultiValueCount();
    CompressTypeOption compressType = attrConfig->GetCompressType();
    const string& separator = attrConfig->GetFieldConfig()->GetSeparator();

    switch (fieldType) {
    case ft_string:
        return new MultiStringAttributeConvertor(needHash, fieldName, isBinary, separator);
    case ft_int32:
        return new VarNumAttributeConvertor<int32_t>(needHash, fieldName, fixedValueCount, separator);
    case ft_float:
        if (compressType.HasBlockFpEncodeCompress() || compressType.HasFp16EncodeCompress() ||
            compressType.HasInt8EncodeCompress()) {
            return new CompressFloatAttributeConvertor(compressType, needHash, fieldName, fixedValueCount, separator);
        }
        return new VarNumAttributeConvertor<float>(needHash, fieldName, fixedValueCount, separator);
    case ft_double:
        return new VarNumAttributeConvertor<double>(needHash, fieldName, fixedValueCount, separator);
    case ft_location:
        return new LocationAttributeConvertor(needHash, fieldName, separator);
    case ft_line:
        return new LineAttributeConvertor(needHash, fieldName, separator);
    case ft_polygon:
        return new PolygonAttributeConvertor(needHash, fieldName, separator);
    case ft_int64:
        return new VarNumAttributeConvertor<int64_t>(needHash, fieldName, fixedValueCount, separator);
    case ft_uint32:
        return new VarNumAttributeConvertor<uint32_t>(needHash, fieldName, fixedValueCount, separator);
    case ft_uint64:
    case ft_hash_64: // uint64:  only used for primary key
        return new VarNumAttributeConvertor<uint64_t>(needHash, fieldName, fixedValueCount, separator);
    case ft_hash_128: // uint128: only used for primary key
        return new VarNumAttributeConvertor<autil::uint128_t>(needHash, fieldName, -1, separator);
    case ft_int8:
        return new VarNumAttributeConvertor<int8_t>(needHash, fieldName, fixedValueCount, separator);
    case ft_uint8:
        return new VarNumAttributeConvertor<uint8_t>(needHash, fieldName, fixedValueCount, separator);
    case ft_int16:
        return new VarNumAttributeConvertor<int16_t>(needHash, fieldName, fixedValueCount, separator);
    case ft_uint16:
        return new VarNumAttributeConvertor<uint16_t>(needHash, fieldName, fixedValueCount, separator);

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
        stringstream ss;
        ss << "Unsupport Attribute Convertor: fieldType = " << FieldConfig::FieldTypeToStr(fieldType);
        INDEXLIB_THROW(util::UnImplementException, "%s", ss.str().c_str());
        return NULL;
    }
    return NULL;
}
}} // namespace indexlib::common
