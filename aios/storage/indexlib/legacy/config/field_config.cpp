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
#include "indexlib/config/field_config.h"

#include "autil/legacy/any.h"
#include "autil/legacy/json.h"
#include "indexlib/config/config_define.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/field_type_traits.h"
#include "indexlib/config/schema_configurator.h"
#include "indexlib/index/attribute/Constant.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/Status2Exception.h"
#include "indexlib/util/TimestampUtil.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace indexlib { namespace config {
AUTIL_LOG_SETUP(indexlib.config, FieldConfig);

class FieldConfigImpl
{
public:
    uint64_t mU32OffsetThreshold = index::ATTRIBUTE_U32OFFSET_THRESHOLD_MAX;
    bool mUpdatableMultiValue = false;
    std::string mUserDefineAttrNullValue;
    CompressTypeOption mCompressType;
    uint64_t mDefragSlicePercent = index::ATTRIBUTE_DEFAULT_DEFRAG_SLICE_PERCENT;
    // util::KeyValueMap mUserDefinedParam;
    IndexStatus mStatus = is_normal;
    schema_opid_t mOwnerOpId = INVALID_SCHEMA_OP_ID;
    bool isBuiltInField = false;
};

FieldConfig::FieldConfig() : FieldConfig("", ft_unknown, false) {}
FieldConfig::FieldConfig(const string& fieldName, FieldType fieldType, bool multiValue)
    : FieldConfig(fieldName, fieldType, multiValue, false, false)
{
}
FieldConfig::FieldConfig(const string& fieldName, FieldType fieldType, bool multiValue, bool isVirtual, bool isBinary)
    : indexlibv2::config::FieldConfig(fieldName, fieldType, multiValue, isVirtual, isBinary)
    , mImpl(new FieldConfigImpl())
{
}

bool FieldConfig::IsBuiltInField() const { return mImpl->isBuiltInField; }
void FieldConfig::SetBuiltInField(bool isBuiltIn) { mImpl->isBuiltInField = isBuiltIn; }

void FieldConfig::SetFieldType(FieldType type) { indexlibv2::config::FieldConfig::SetFieldType(type); }
uint64_t FieldConfig::GetU32OffsetThreshold() { return mImpl->mU32OffsetThreshold; }
void FieldConfig::SetU32OffsetThreshold(uint64_t offsetThreshold) { mImpl->mU32OffsetThreshold = offsetThreshold; }

void FieldConfig::SetUpdatableMultiValue(bool IsUpdatableMultiValue)
{
    mImpl->mUpdatableMultiValue = IsUpdatableMultiValue;
}
void FieldConfig::SetCompressType(const string& compressStr)
{
    auto status = mImpl->mCompressType.Init(compressStr);
    THROW_IF_STATUS_ERROR(status);
}

void FieldConfig::SetUniqEncode(bool isUniqEncode)
{
    if (isUniqEncode) {
        mImpl->mCompressType.SetUniqEncode();
    }
}

void FieldConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    indexlibv2::config::FieldConfig::Jsonize(json);

    if (json.GetMode() == TO_JSON) {
        if (mImpl->mUpdatableMultiValue) {
            json.Jsonize(ATTRIBUTE_UPDATABLE_MULTI_VALUE, mImpl->mUpdatableMultiValue);
        }

        if (mImpl->mU32OffsetThreshold != index::ATTRIBUTE_U32OFFSET_THRESHOLD_MAX) {
            json.Jsonize(index::ATTRIBUTE_U32OFFSET_THRESHOLD, mImpl->mU32OffsetThreshold);
        }

        if (mImpl->mCompressType.HasCompressOption()) {
            string compressStr = mImpl->mCompressType.GetCompressStr();
            assert(!compressStr.empty());
            json.Jsonize(index::COMPRESS_TYPE, compressStr);
        }

        if (mImpl->mDefragSlicePercent != index::ATTRIBUTE_DEFAULT_DEFRAG_SLICE_PERCENT) {
            json.Jsonize(index::ATTRIBUTE_DEFRAG_SLICE_PERCENT, mImpl->mDefragSlicePercent);
        }

        if (!mImpl->mUserDefineAttrNullValue.empty()) {
            json.Jsonize(FIELD_USER_DEFINE_ATTRIBUTE_NULL_VALUE, mImpl->mUserDefineAttrNullValue);
        }
        if (mImpl->isBuiltInField) {
            json.Jsonize(FIELD_IS_BUILT_IN, mImpl->isBuiltInField);
        }
    } else {
        bool uniqEncode = false;
        json.Jsonize(ATTRIBUTE_UNIQ_ENCODE, uniqEncode, uniqEncode);

        string compressType;
        json.Jsonize(index::COMPRESS_TYPE, compressType, compressType);
        auto status = mImpl->mCompressType.Init(compressType);
        THROW_IF_STATUS_ERROR(status);
        if (uniqEncode) {
            mImpl->mCompressType.SetUniqEncode();
        }

        json.Jsonize(ATTRIBUTE_UPDATABLE_MULTI_VALUE, mImpl->mUpdatableMultiValue, mImpl->mUpdatableMultiValue);
        json.Jsonize(index::ATTRIBUTE_U32OFFSET_THRESHOLD, mImpl->mU32OffsetThreshold, mImpl->mU32OffsetThreshold);
        json.Jsonize(index::ATTRIBUTE_DEFRAG_SLICE_PERCENT, mImpl->mDefragSlicePercent, mImpl->mDefragSlicePercent);
        if (mImpl->mU32OffsetThreshold > index::ATTRIBUTE_U32OFFSET_THRESHOLD_MAX) {
            mImpl->mU32OffsetThreshold = index::ATTRIBUTE_U32OFFSET_THRESHOLD_MAX;
        }

        // UpdateLegacyUserDefineParam();

        json.Jsonize(FIELD_USER_DEFINE_ATTRIBUTE_NULL_VALUE, mImpl->mUserDefineAttrNullValue,
                     mImpl->mUserDefineAttrNullValue);
        json.Jsonize(FIELD_IS_BUILT_IN, mImpl->isBuiltInField, mImpl->isBuiltInField);
    }
}
void FieldConfig::AssertEqual(const FieldConfig& other) const
{
    IE_CONFIG_ASSERT_EQUAL(GetFieldId(), other.GetFieldId(), "FieldId not equal");
    IE_CONFIG_ASSERT_EQUAL(IsMultiValue(), other.IsMultiValue(), "MultiValue not equal");
    IE_CONFIG_ASSERT_EQUAL(IsBinary(), other.IsBinary(), "BinaryField not equal");
    IE_CONFIG_ASSERT_EQUAL(GetFieldName(), other.GetFieldName(), "FieldName not equal");
    IE_CONFIG_ASSERT_EQUAL(GetFixedMultiValueCount(), other.GetFixedMultiValueCount(),
                           "FixedMultiValueCount not equal");
    IE_CONFIG_ASSERT_EQUAL(GetSeparator(), other.GetSeparator(), "separator not equal");
    IE_CONFIG_ASSERT_EQUAL(GetDefaultTimeZoneDelta(), other.GetDefaultTimeZoneDelta(),
                           "DefaultTimeZoneDelta not equal");
    IE_CONFIG_ASSERT_EQUAL(GetDefaultValue(), other.GetDefaultValue(), "DefaultValue not equal");
    IE_CONFIG_ASSERT_EQUAL(IsEnableNullField(), other.IsEnableNullField(), "SupportNull not equal");
    IE_CONFIG_ASSERT_EQUAL(GetNullFieldLiteralString(), other.GetNullFieldLiteralString(), "NullFieldString not equal");
    IE_CONFIG_ASSERT_EQUAL(GetAnalyzerName(), other.GetAnalyzerName(), "AnalyzerName not equal");
    IE_CONFIG_ASSERT_EQUAL(mImpl->mUpdatableMultiValue, other.mImpl->mUpdatableMultiValue,
                           "UpdatableMultiValue not equal");
    IE_CONFIG_ASSERT_EQUAL(mImpl->mDefragSlicePercent, other.mImpl->mDefragSlicePercent,
                           "defrag slice percent not equal");
    // IE_CONFIG_ASSERT_EQUAL(mImpl->mUserDefinedParam, other.mImpl->mUserDefinedParam, "user defined param not equal");
    // IE_CONFIG_ASSERT_EQUAL(_userDefinedParamV2, other._userDefinedParamV2, "user defined param not equal");
    THROW_IF_STATUS_ERROR(mImpl->mCompressType.AssertEqual(other.mImpl->mCompressType));
    CheckFieldTypeEqual(other);
}

FieldType FieldConfig::StrToFieldType(const string& typeStr)
{
    if (!strcasecmp(typeStr.c_str(), "text")) {
        return ft_text;
    } else if (!strcasecmp(typeStr.c_str(), "string")) {
        return ft_string;
    } else if (!strcasecmp(typeStr.c_str(), "integer")) {
        return ft_integer;
    } else if (!strcasecmp(typeStr.c_str(), "enum")) {
        return ft_enum;
    } else if (!strcasecmp(typeStr.c_str(), "float")) {
        return ft_float;
    } else if (!strcasecmp(typeStr.c_str(), "fp8")) {
        return ft_fp8;
    } else if (!strcasecmp(typeStr.c_str(), "fp16")) {
        return ft_fp16;
    } else if (!strcasecmp(typeStr.c_str(), "long")) {
        return ft_long;
    } else if (!strcasecmp(typeStr.c_str(), "time")) {
        return ft_time;
    } else if (!strcasecmp(typeStr.c_str(), "timestamp")) {
        return ft_timestamp;
    } else if (!strcasecmp(typeStr.c_str(), "date")) {
        return ft_date;
    } else if (!strcasecmp(typeStr.c_str(), "location")) {
        return ft_location;
    } else if (!strcasecmp(typeStr.c_str(), "polygon")) {
        return ft_polygon;
    } else if (!strcasecmp(typeStr.c_str(), "line")) {
        return ft_line;
    } else if (!strcasecmp(typeStr.c_str(), "online")) {
        return ft_online;
    } else if (!strcasecmp(typeStr.c_str(), "property")) {
        return ft_property;
    } else if (!strcasecmp(typeStr.c_str(), "uint8")) {
        return ft_uint8;
    } else if (!strcasecmp(typeStr.c_str(), "int8")) {
        return ft_int8;
    } else if (!strcasecmp(typeStr.c_str(), "uint16")) {
        return ft_uint16;
    } else if (!strcasecmp(typeStr.c_str(), "int16")) {
        return ft_int16;
    } else if (!strcasecmp(typeStr.c_str(), "uint32")) {
        return ft_uint32;
    } else if (!strcasecmp(typeStr.c_str(), "int32")) {
        return ft_int32;
    } else if (!strcasecmp(typeStr.c_str(), "uint64")) {
        return ft_uint64;
    } else if (!strcasecmp(typeStr.c_str(), "int64")) {
        return ft_int64;
    } else if (!strcasecmp(typeStr.c_str(), "hash_64")) {
        return ft_hash_64;
    } else if (!strcasecmp(typeStr.c_str(), "hash_128")) {
        return ft_hash_128;
    } else if (!strcasecmp(typeStr.c_str(), "double")) {
        return ft_double;
    } else if (!strcasecmp(typeStr.c_str(), "raw")) {
        return ft_raw;
    }

    stringstream ss;
    ss << "Unknown field_type: " << typeStr << ", support field_type are: ";
    for (int ft = 0; ft < (int)ft_unknown; ++ft) {
        ss << FieldTypeToStr((FieldType)ft) << ",";
    }

    INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
    return ft_unknown;
}
bool FieldConfig::IsIntegerType(FieldType fieldType)
{
    return indexlibv2::config::FieldConfig::IsIntegerType(fieldType);
}
void FieldConfig::Check() const
{
    indexlibv2::config::FieldConfig::Check();
    CheckUniqEncode();
    CheckEquivalentCompress();
    CheckBlockFpEncode();
    CheckFp16Encode();
    CheckFloatInt8Encode();
    CheckUserDefineNullAttrValue();
}

FieldConfig* FieldConfig::Clone() const
{
    FieldConfig* cloneFieldConfig = new FieldConfig();
    autil::legacy::Any any = autil::legacy::ToJson(*this);
    FromJson(*cloneFieldConfig, any);
    return cloneFieldConfig;
}
void FieldConfig::SetDefragSlicePercent(uint64_t defragPercent) { mImpl->mDefragSlicePercent = defragPercent; }

void FieldConfig::SetMultiValue(bool multiValue) { indexlibv2::config::FieldConfig::SetIsMultiValue(multiValue); }
void FieldConfig::ClearCompressType() { mImpl->mCompressType.ClearCompressType(); }

void FieldConfig::Delete() { mImpl->mStatus = is_deleted; }

bool FieldConfig::IsDeleted() const { return mImpl->mStatus == is_deleted; }

bool FieldConfig::IsNormal() const { return mImpl->mStatus == is_normal; }

IndexStatus FieldConfig::GetStatus() const { return mImpl->mStatus; }

void FieldConfig::SetOwnerModifyOperationId(schema_opid_t opId) { mImpl->mOwnerOpId = opId; }

schema_opid_t FieldConfig::GetOwnerModifyOperationId() const { return mImpl->mOwnerOpId; }

void FieldConfig::RewriteFieldType()
{
    if (GetFieldType() != ft_float || IsMultiValue() || GetFixedMultiValueCount() != -1) {
        return;
    }

    if (mImpl->mCompressType.HasInt8EncodeCompress()) {
        indexlibv2::config::FieldConfig::SetFieldType(ft_fp8);
    } else if (mImpl->mCompressType.HasFp16EncodeCompress()) {
        indexlibv2::config::FieldConfig::SetFieldType(ft_fp16);
    }
}

const string& FieldConfig::GetUserDefineAttributeNullValue() const { return mImpl->mUserDefineAttrNullValue; }

void FieldConfig::SetUserDefineAttributeNullValue(const string& nullStr) { mImpl->mUserDefineAttrNullValue = nullStr; }

bool FieldConfig::IsUpdatableMultiValue() const { return mImpl->mUpdatableMultiValue; }

CompressTypeOption FieldConfig::GetCompressType() const { return mImpl->mCompressType; }

uint64_t FieldConfig::GetDefragSlicePercent() const { return mImpl->mDefragSlicePercent; }

void FieldConfig::CheckUniqEncode() const
{
    if (mImpl->mCompressType.HasUniqEncodeCompress()) {
        if (IsMultiValue() || GetFieldType() == ft_string) {
            // do nothing
        } else {
            stringstream ss;
            ss << "uniqEncode invalid for field " << GetFieldName();
            INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
        }
    }
}

void FieldConfig::CheckBlockFpEncode() const
{
    if (!mImpl->mCompressType.HasBlockFpEncodeCompress()) {
        return;
    }
    if (!IsMultiValue() || GetFieldType() != ft_float || GetFixedMultiValueCount() == -1) {
        stringstream ss;
        ss << "BlockFpEncode invalid for field " << GetFieldName();
        INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
    }
}

void FieldConfig::CheckFp16Encode() const
{
    if (GetFieldType() == ft_fp16) {
        // fieldType set to ft_fp16 means already checked
        return;
    }

    if (!mImpl->mCompressType.HasFp16EncodeCompress()) {
        return;
    }
    if (GetFieldType() != ft_float) {
        stringstream ss;
        ss << "Fp16Encode invalid for field " << GetFieldName();
        INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
    }
    if (!IsMultiValue() && mImpl->mCompressType.HasEquivalentCompress()) {
        INDEXLIB_FATAL_ERROR(Schema, "can not use fp16 & equalCompress for field[%s] at the same time",
                             GetFieldName().c_str());
    }
}

void FieldConfig::CheckFloatInt8Encode() const
{
    if (GetFieldType() == ft_fp8) {
        return;
    }

    if (!mImpl->mCompressType.HasInt8EncodeCompress()) {
        return;
    }
    if (GetFieldType() != ft_float) {
        stringstream ss;
        ss << "Int8FloatEncode invalid for field " << GetFieldName();
        INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
    }

    if (!IsMultiValue() && mImpl->mCompressType.HasEquivalentCompress()) {
        INDEXLIB_FATAL_ERROR(Schema, "can not use int8Compress & equalCompress for field[%s] at the same time",
                             GetFieldName().c_str());
    }
}

void FieldConfig::CheckEquivalentCompress() const
{
    // only not equal type allows to use compress
    if (mImpl->mCompressType.HasEquivalentCompress()) {
        if (!IsMultiValue() && GetFieldType() != ft_string && IsEnableNullField() &&
            mImpl->mUserDefineAttrNullValue.empty()) {
            INDEXLIB_FATAL_ERROR(Schema,
                                 "equivalent compress invalid for field [%s], "
                                 "not support null with equal compress ",
                                 GetFieldName().c_str());
        }

        if (IsMultiValue() || GetFieldType() == ft_integer || GetFieldType() == ft_uint32 ||
            GetFieldType() == ft_long || GetFieldType() == ft_uint64 || GetFieldType() == ft_int8 ||
            GetFieldType() == ft_uint8 || GetFieldType() == ft_int16 || GetFieldType() == ft_uint16 ||
            GetFieldType() == ft_float || GetFieldType() == ft_fp8 || GetFieldType() == ft_fp16 ||
            GetFieldType() == ft_double || GetFieldType() == ft_string) {
            // do nothing
        } else {
            INDEXLIB_FATAL_ERROR(Schema, "equivalent compress invalid for field [%s]", GetFieldName().c_str());
        }
    }
}

void FieldConfig::CheckUserDefineNullAttrValue() const
{
    if (mImpl->mUserDefineAttrNullValue.empty()) {
        return;
    }

    if (IsMultiValue() || GetFieldType() == ft_string) {
        return;
    }

    if (GetFieldType() == ft_integer || GetFieldType() == ft_uint32 || GetFieldType() == ft_long ||
        GetFieldType() == ft_uint64 || GetFieldType() == ft_int8 || GetFieldType() == ft_uint8 ||
        GetFieldType() == ft_double || GetFieldType() == ft_float || GetFieldType() == ft_int16 ||
        GetFieldType() == ft_uint16) {
#define CHECK_NULL_ATTR_VALUE(type)                                                                                    \
    case type: {                                                                                                       \
        typedef FieldTypeTraits<type>::AttrItemType ValueType;                                                         \
        ValueType value;                                                                                               \
        if (!autil::StringUtil::fromString(mImpl->mUserDefineAttrNullValue, value)) {                                  \
            INDEXLIB_FATAL_ERROR(Schema, "invalid user_define_attribute_null_value [%s] for Field [%s] type [%s]",     \
                                 mImpl->mUserDefineAttrNullValue.c_str(), GetFieldName().c_str(),                      \
                                 FieldTypeToStr(GetFieldType()));                                                      \
        }                                                                                                              \
    } break

        switch (GetFieldType()) {
            NUMBER_FIELD_MACRO_HELPER(CHECK_NULL_ATTR_VALUE);
        default:
            break;
        }
#undef CHECK_NULL_ATTR_VALUE
    }
}

void FieldConfig::CheckFieldTypeEqual(const FieldConfig& other) const
{
    if (IsMultiValue()) {
        IE_CONFIG_ASSERT_EQUAL(GetFieldType(), other.GetFieldType(), "FieldType not equal");
        return;
    }
    // only single float will be rewrited
    if (mImpl->mCompressType.HasFp16EncodeCompress()) {
        if ((GetFieldType() == ft_fp16 || GetFieldType() == ft_float) &&
            (other.GetFieldType() == ft_fp16 || other.GetFieldType() == ft_float)) {
            return;
        }
    }

    if (mImpl->mCompressType.HasInt8EncodeCompress()) {
        if ((GetFieldType() == ft_fp8 || GetFieldType() == ft_float) &&
            (other.GetFieldType() == ft_fp8 || other.GetFieldType() == ft_float)) {
            return;
        }
    }

    IE_CONFIG_ASSERT_EQUAL(GetFieldType(), other.GetFieldType(), "FieldType not equal");
}

}} // namespace indexlib::config
