#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/misc/exception.h"
#include "indexlib/common/field_format/attribute/string_attribute_convertor.h"
#include "indexlib/common/field_format/attribute/multi_string_attribute_convertor.h"
#include "indexlib/common/field_format/attribute/single_value_attribute_convertor.h"
#include "indexlib/common/field_format/attribute/var_num_attribute_convertor.h"
#include "indexlib/common/field_format/attribute/location_attribute_convertor.h"
#include "indexlib/common/field_format/attribute/line_attribute_convertor.h"
#include "indexlib/common/field_format/attribute/polygon_attribute_convertor.h"
#include "indexlib/common/field_format/attribute/compress_float_attribute_convertor.h"
#include "indexlib/common/field_format/attribute/compress_single_float_attribute_convertor.h"

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, AttributeConvertorFactory);

AttributeConvertorFactory::~AttributeConvertorFactory() 
{
}

AttributeConvertor* AttributeConvertorFactory::CreateAttrConvertor(
        const FieldConfigPtr& fieldConfig, TableType tableType)
{
    AttributeConvertor* convertor = CreateAttrConvertor(fieldConfig);
    if (convertor && (tableType == tt_kv || tableType == tt_kkv))
    {
        // kv & kkv table not support update, should encode empty field
        // only update_field document not encode empty field for single value
        convertor->SetEncodeEmpty(true);
    }
    return convertor;
}

AttributeConvertor* AttributeConvertorFactory::CreatePackAttrConvertor(
        const PackAttributeConfigPtr& packAttrConfig)
{
    return CreateAttrConvertor(packAttrConfig->CreateAttributeConfig()->GetFieldConfig());
}

AttributeConvertor* AttributeConvertorFactory::CreateAttrConvertor(
        const FieldConfigPtr& fieldConfig)
{
    if(fieldConfig->IsMultiValue())
    {
        return CreateMultiAttrConvertor(fieldConfig);
    }
    else
    {
        return CreateSingleAttrConvertor(fieldConfig);
    }
}

AttributeConvertor* AttributeConvertorFactory::CreateSingleAttrConvertor(
        const FieldConfigPtr& fieldConfig)
{
    FieldType fieldType = fieldConfig->GetFieldType();
    bool needHash = fieldConfig->IsUniqEncode();
    const string& fieldName = fieldConfig->GetFieldName();
    CompressTypeOption compressType = fieldConfig->GetCompressType();
    switch (fieldType)
    {
    case ft_string:
        return new StringAttributeConvertor(needHash, fieldName,
                fieldConfig->GetFixedMultiValueCount());
    case ft_int32:
        return new SingleValueAttributeConvertor<int32_t>(needHash, fieldName);
    case ft_float:
        if (compressType.HasFp16EncodeCompress()) {
            return new CompressSingleFloatAttributeConvertor<int16_t>(
                    compressType, fieldName);
        }
        if (compressType.HasInt8EncodeCompress()) {
            return new CompressSingleFloatAttributeConvertor<int8_t>(
                    compressType, fieldName);
        }
        return new SingleValueAttributeConvertor<float>(needHash, fieldName);
    case ft_double:
        return new SingleValueAttributeConvertor<double>(needHash, fieldName);
    case ft_int64:
        return new SingleValueAttributeConvertor<int64_t>(needHash, fieldName);
    case ft_uint32:
        return new SingleValueAttributeConvertor<uint32_t>(needHash, fieldName);
    case ft_uint64:
    case ft_hash_64:            // uint64:  only used for primary key
        return new SingleValueAttributeConvertor<uint64_t>(needHash, fieldName);
    case ft_hash_128:           // uint128: only used for primary key
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
        INDEXLIB_THROW(misc::UnImplementException, "%s", ss.str().c_str());
        return NULL;
    }
    return NULL;
}

AttributeConvertor* AttributeConvertorFactory::CreateMultiAttrConvertor(
        const FieldConfigPtr& fieldConfig)
{
    FieldType fieldType = fieldConfig->GetFieldType();
    bool needHash = fieldConfig->IsUniqEncode();
    const string& fieldName = fieldConfig->GetFieldName();
    bool isBinary = fieldConfig->IsBinary();
    int32_t fixedValueCount = fieldConfig->GetFixedMultiValueCount();
    CompressTypeOption compressType = fieldConfig->GetCompressType();
    
    switch (fieldType)
    {
    case ft_string:
        return new MultiStringAttributeConvertor(needHash, fieldName, isBinary);
    case ft_int32:
        return new VarNumAttributeConvertor<int32_t>(needHash, fieldName, fixedValueCount);
    case ft_float:
        if (compressType.HasBlockFpEncodeCompress() ||
            compressType.HasFp16EncodeCompress() ||
            compressType.HasInt8EncodeCompress())
        {
            return new CompressFloatAttributeConvertor(compressType, needHash,
                                                       fieldName, fixedValueCount);
        }
        return new VarNumAttributeConvertor<float>(needHash, fieldName, fixedValueCount);
    case ft_fp8:
        // multi value not support ft_fp8 & ft_fp16
        assert(false);
    case ft_fp16:
        assert(false);
    case ft_double:
        return new VarNumAttributeConvertor<double>(needHash, fieldName, fixedValueCount);
    case ft_location:
        return new LocationAttributeConvertor(needHash, fieldName);
    case ft_line:
        return new LineAttributeConvertor(needHash, fieldName);
    case ft_polygon:
        return new PolygonAttributeConvertor(needHash, fieldName);
    case ft_int64:
        return new VarNumAttributeConvertor<int64_t>(needHash, fieldName, fixedValueCount);
    case ft_uint32:
        return new VarNumAttributeConvertor<uint32_t>(needHash, fieldName, fixedValueCount);
    case ft_uint64:
    case ft_hash_64:            // uint64:  only used for primary key
        return new VarNumAttributeConvertor<uint64_t>(needHash, fieldName, fixedValueCount);
    case ft_hash_128:           // uint128: only used for primary key
        return new VarNumAttributeConvertor<autil::uint128_t>(needHash, fieldName);
    case ft_int8:
        return new VarNumAttributeConvertor<int8_t>(needHash, fieldName, fixedValueCount);
    case ft_uint8:
        return new VarNumAttributeConvertor<uint8_t>(needHash, fieldName, fixedValueCount);
    case ft_int16:
        return new VarNumAttributeConvertor<int16_t>(needHash, fieldName, fixedValueCount);
    case ft_uint16:
        return new VarNumAttributeConvertor<uint16_t>(needHash, fieldName, fixedValueCount);
    case ft_time:
    case ft_online:
    case ft_property:
    case ft_enum:
    case ft_text:
    case ft_unknown:
    default:
        stringstream ss;
        ss << "Unsupport Attribute Convertor: fieldType = " << FieldConfig::FieldTypeToStr(fieldType);
        INDEXLIB_THROW(misc::UnImplementException, "%s", ss.str().c_str());
        return NULL;
    }
    return NULL;
}

IE_NAMESPACE_END(common);

