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
#include "indexlib/config/FieldConfig.h"

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "indexlib/base/Constant.h"
#include "indexlib/config/ConfigDefine.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/TimestampUtil.h"

namespace indexlibv2::config {
AUTIL_LOG_SETUP(indexlib.config, FieldConfig);

struct FieldConfig::Impl {
    fieldid_t fieldId = INVALID_FIELDID;
    std::string fieldName;
    FieldType fieldType = ft_unknown;
    int32_t defaultTimeZoneDelta = 0;
    std::string defaultStrValue;
    autil::legacy::json::JsonMap userDefinedParam;
    int32_t fixedMultiValueCount = -1;
    std::string separator = INDEXLIB_MULTI_VALUE_SEPARATOR_STR;
    std::string nullFieldString = DEFAULT_NULL_FIELD_STRING;
    std::string analyzerName;
    bool isMultiValue = false;
    bool supportNull = false;
    bool isSortField = false;
    bool isBinaryField = false;
    bool isVirtual = false;
    bool isBuiltInField = false;
};

FieldConfig::FieldConfig() : FieldConfig("", ft_unknown, false) {}
FieldConfig::FieldConfig(const std::string& fieldName, FieldType fieldType, bool isMultiValue)
    : FieldConfig(fieldName, fieldType, isMultiValue, /*isVirtual*/ false, /*isBinary*/ false)
{
}
FieldConfig::FieldConfig(const std::string& fieldName, FieldType fieldType, bool multiValue, bool isVirtual,
                         bool isBinary)
    : _impl(new FieldConfig::Impl {.fieldName = fieldName,
                                   .fieldType = fieldType,
                                   .isMultiValue = multiValue,
                                   .isBinaryField = isBinary,
                                   .isVirtual = isVirtual})
{
    if (fieldType == ft_location || fieldType == ft_line || fieldType == ft_polygon) {
        SetIsMultiValue(true);
    }
}
FieldConfig::FieldConfig(const FieldConfig& other) : _impl(std::make_unique<FieldConfig::Impl>(*other._impl)) {}
FieldConfig& FieldConfig::operator=(const FieldConfig& other)
{
    if (this != &other) {
        _impl = std::make_unique<FieldConfig::Impl>(*other._impl);
    }
    return *this;
}
FieldConfig::~FieldConfig() {}

void FieldConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize(FIELD_NAME, _impl->fieldName);

    if (json.GetMode() == TO_JSON) {
        json.Jsonize(FIELD_TYPE, std::string(FieldTypeToStr(_impl->fieldType)));
        if (_impl->isBinaryField) {
            json.Jsonize(FIELD_BINARY, _impl->isBinaryField);
        }
        if (_impl->isMultiValue) {
            json.Jsonize(FIELD_MULTI_VALUE, _impl->isMultiValue);
        }
        if (!_impl->userDefinedParam.empty()) {
            json.Jsonize(FIELD_USER_DEFINED_PARAM, _impl->userDefinedParam);
        }
        if (_impl->defaultStrValue != FIELD_DEFAULT_STR_VALUE) {
            json.Jsonize(FIELD_DEFAULT_VALUE, _impl->defaultStrValue);
        }
        if (_impl->fixedMultiValueCount != -1) {
            json.Jsonize(FIELD_FIXED_MULTI_VALUE_COUNT, _impl->fixedMultiValueCount);
        }
        if (_impl->isSortField) {
            json.Jsonize(SORT_FIELD, _impl->isSortField);
        }
        if (_impl->supportNull) {
            json.Jsonize(FIELD_SUPPORT_NULL, _impl->supportNull);
        }
        if (_impl->defaultTimeZoneDelta != 0) {
            json.Jsonize(FIELD_DEFAULT_TIME_ZONE,
                         indexlib::util::TimestampUtil::TimeZoneDeltaToString(_impl->defaultTimeZoneDelta));
        }
        if (_impl->separator != INDEXLIB_MULTI_VALUE_SEPARATOR_STR) {
            json.Jsonize(FIELD_SEPARATOR, _impl->separator);
        }
        if (_impl->nullFieldString != DEFAULT_NULL_FIELD_STRING) {
            json.Jsonize(FIELD_DEFAULT_NULL_STRING_VALUE, _impl->nullFieldString);
        }
        if (_impl->fieldType == ft_text) {
            json.Jsonize(FIELD_ANALYZER, _impl->analyzerName);
        }
        if (_impl->isVirtual) {
            json.Jsonize(FIELD_VIRTUAL, _impl->isVirtual);
        }
        if (_impl->isBuiltInField) {
            json.Jsonize(FIELD_IS_BUILT_IN, _impl->isBuiltInField);
        }
    } else { // FROM_JSON
        std::string typeStr;
        json.Jsonize(FIELD_TYPE, typeStr);
        if (!StrToFieldType(typeStr, _impl->fieldType)) {
            INDEXLIB_FATAL_ERROR(Schema, "invalid field type [%s]", typeStr.c_str());
        }
        json.Jsonize(FIELD_ANALYZER, _impl->analyzerName, _impl->analyzerName);
        json.Jsonize(FIELD_BINARY, _impl->isBinaryField, _impl->isBinaryField);
        json.Jsonize(FIELD_MULTI_VALUE, _impl->isMultiValue, _impl->isMultiValue);
        json.Jsonize(FIELD_USER_DEFINED_PARAM, _impl->userDefinedParam, _impl->userDefinedParam);
        json.Jsonize(FIELD_FIXED_MULTI_VALUE_COUNT, _impl->fixedMultiValueCount, _impl->fixedMultiValueCount);
        json.Jsonize(SORT_FIELD, _impl->isSortField, _impl->isSortField);
        json.Jsonize(FIELD_SUPPORT_NULL, _impl->supportNull, _impl->supportNull);
        json.Jsonize(FIELD_DEFAULT_VALUE, _impl->defaultStrValue, _impl->defaultStrValue);
        std::string timeZoneStr;
        json.Jsonize(FIELD_DEFAULT_TIME_ZONE, timeZoneStr, timeZoneStr);
        if (timeZoneStr.empty()) {
            _impl->defaultTimeZoneDelta = 0;
        } else if (!indexlib::util::TimestampUtil::ParseTimeZone(autil::StringView(timeZoneStr),
                                                                 _impl->defaultTimeZoneDelta)) {
            INDEXLIB_FATAL_ERROR(Schema, "invalid default_time_zone [%s]", timeZoneStr.c_str());
        }
        json.Jsonize(FIELD_SEPARATOR, _impl->separator, _impl->separator);
        json.Jsonize(FIELD_DEFAULT_NULL_STRING_VALUE, _impl->nullFieldString, _impl->nullFieldString);
        json.Jsonize(FIELD_VIRTUAL, _impl->isVirtual, _impl->isVirtual);
        json.Jsonize(FIELD_IS_BUILT_IN, _impl->isBuiltInField, _impl->isBuiltInField);
        auto fieldType = _impl->fieldType;
        if (fieldType == ft_location || fieldType == ft_line || fieldType == ft_polygon) {
            SetIsMultiValue(true);
        }
        Check();
    }
}

void FieldConfig::CheckFixedLengthMultiValue() const
{
    if (GetFixedMultiValueCount() == -1) {
        return;
    }

    if (IsEnableNullField()) {
        INDEXLIB_FATAL_ERROR(Schema,
                             "Field [%s] does not support enable_null & "
                             "set fixed_multi_value_count at same time.",
                             GetFieldName().c_str());
    }

    if (IsMultiValue()) {
        if (GetFieldType() == ft_integer || GetFieldType() == ft_uint32 || GetFieldType() == ft_long ||
            GetFieldType() == ft_uint64 || GetFieldType() == ft_int8 || GetFieldType() == ft_uint8 ||
            GetFieldType() == ft_double || GetFieldType() == ft_float || GetFieldType() == ft_int16 ||
            GetFieldType() == ft_uint16) {
            if (GetFixedMultiValueCount() == 0) {
                INDEXLIB_FATAL_ERROR(Schema,
                                     "Field [%s] does not support "
                                     "set fixed_multi_value_count = 0",
                                     GetFieldName().c_str());
            }
            return;
        }
        INDEXLIB_FATAL_ERROR(Schema,
                             "Field [%s] , FieldType[%d] does not support "
                             "set fixed_multi_value_count",
                             GetFieldName().c_str(), int(GetFieldType()));
    }

    // single value
    if (GetFieldType() == ft_string) {
        if (GetFixedMultiValueCount() == 0) {
            INDEXLIB_FATAL_ERROR(Schema,
                                 "Field [%s] does not support "
                                 "set fixed_multi_value_count = 0",
                                 GetFieldName().c_str());
        }
        return;
    }
    INDEXLIB_FATAL_ERROR(Schema,
                         "Field [%s] , FieldType[%d] does not support "
                         "set fixed_multi_value_count",
                         GetFieldName().c_str(), int(GetFieldType()));
}

void FieldConfig::Check() const
{
    if (_impl->fieldType == ft_text) {
        if (_impl->analyzerName.empty()) {
            INDEXLIB_FATAL_ERROR(Schema, "analyzer_name must be set in text field, and not empty, field[%s]",
                                 _impl->fieldName.c_str());
        }
    } else {
        if (!_impl->analyzerName.empty()) {
            INDEXLIB_FATAL_ERROR(Schema, "analyzer_name must be empty in non text field, field[%s]",
                                 _impl->fieldName.c_str());
        }
    }
    if (_impl->isBinaryField) {
        if (!_impl->isMultiValue || _impl->fieldType != ft_string) {
            INDEXLIB_FATAL_ERROR(Schema, "Only MultiString field support isBinary = true, field[%s]",
                                 _impl->fieldName.c_str());
        }
    }
    if ((GetFieldType() == ft_time || GetFieldType() == ft_timestamp || GetFieldType() == ft_date) && IsMultiValue()) {
        INDEXLIB_FATAL_ERROR(Schema, "field [%s] type [%s] not support multi value", GetFieldName().c_str(),
                             FieldTypeToStr(GetFieldType()));
    }

    if (GetFieldType() == ft_raw && IsEnableNullField()) {
        INDEXLIB_FATAL_ERROR(Schema, "raw field [%s] not support [%s] = true", GetFieldName().c_str(),
                             FIELD_SUPPORT_NULL.c_str());
    }

    if (GetFieldType() == ft_text && GetSeparator() != INDEXLIB_MULTI_VALUE_SEPARATOR_STR) {
        INDEXLIB_FATAL_ERROR(Schema, "text field [%s] does not support other separator %s", GetFieldName().c_str(),
                             GetSeparator().c_str());
    }
    CheckFixedLengthMultiValue();
}

bool FieldConfig::StrToFieldType(const std::string& typeStr, FieldType& type)
{
    type = ft_unknown;
    if (!strcasecmp(typeStr.c_str(), "text")) {
        type = ft_text;
    } else if (!strcasecmp(typeStr.c_str(), "string")) {
        type = ft_string;
    } else if (!strcasecmp(typeStr.c_str(), "integer")) {
        type = ft_integer;
    } else if (!strcasecmp(typeStr.c_str(), "enum")) {
        type = ft_enum;
    } else if (!strcasecmp(typeStr.c_str(), "float")) {
        type = ft_float;
    } else if (!strcasecmp(typeStr.c_str(), "fp8")) {
        type = ft_fp8;
    } else if (!strcasecmp(typeStr.c_str(), "fp16")) {
        type = ft_fp16;
    } else if (!strcasecmp(typeStr.c_str(), "long")) {
        type = ft_long;
    } else if (!strcasecmp(typeStr.c_str(), "time")) {
        type = ft_time;
    } else if (!strcasecmp(typeStr.c_str(), "timestamp")) {
        type = ft_timestamp;
    } else if (!strcasecmp(typeStr.c_str(), "date")) {
        type = ft_date;
    } else if (!strcasecmp(typeStr.c_str(), "location")) {
        type = ft_location;
    } else if (!strcasecmp(typeStr.c_str(), "polygon")) {
        type = ft_polygon;
    } else if (!strcasecmp(typeStr.c_str(), "line")) {
        type = ft_line;
    } else if (!strcasecmp(typeStr.c_str(), "online")) {
        type = ft_online;
    } else if (!strcasecmp(typeStr.c_str(), "property")) {
        type = ft_property;
    } else if (!strcasecmp(typeStr.c_str(), "uint8")) {
        type = ft_uint8;
    } else if (!strcasecmp(typeStr.c_str(), "int8")) {
        type = ft_int8;
    } else if (!strcasecmp(typeStr.c_str(), "uint16")) {
        type = ft_uint16;
    } else if (!strcasecmp(typeStr.c_str(), "int16")) {
        type = ft_int16;
    } else if (!strcasecmp(typeStr.c_str(), "uint32")) {
        type = ft_uint32;
    } else if (!strcasecmp(typeStr.c_str(), "int32")) {
        type = ft_int32;
    } else if (!strcasecmp(typeStr.c_str(), "uint64")) {
        type = ft_uint64;
    } else if (!strcasecmp(typeStr.c_str(), "int64")) {
        type = ft_int64;
    } else if (!strcasecmp(typeStr.c_str(), "hash_64")) {
        type = ft_hash_64;
    } else if (!strcasecmp(typeStr.c_str(), "hash_128")) {
        type = ft_hash_128;
    } else if (!strcasecmp(typeStr.c_str(), "double")) {
        type = ft_double;
    } else if (!strcasecmp(typeStr.c_str(), "raw")) {
        type = ft_raw;
    }
    if (type == ft_unknown) {
        std::stringstream ss;
        ss << "Unknown field_type: " << typeStr << ", support field_type are: ";
        for (int ft = 0; ft < (int)ft_unknown; ++ft) {
            ss << FieldTypeToStr((FieldType)ft) << ",";
        }
        AUTIL_LOG(ERROR, "%s", ss.str().c_str());
        return false;
    }
    return true;
}

const char* FieldConfig::FieldTypeToStr(FieldType fieldType)
{
    switch (fieldType) {
    case ft_text:
        return "TEXT";
    case ft_string:
        return "STRING";
    case ft_enum:
        return "ENUM";
    case ft_integer:
        return "INTEGER";
    case ft_fp8:
    case ft_fp16:
    case ft_float:
        return "FLOAT";
    case ft_long:
        return "LONG";
    case ft_time:
        return "TIME";
    case ft_date:
        return "DATE";
    case ft_timestamp:
        return "TIMESTAMP";
    case ft_location:
        return "LOCATION";
    case ft_line:
        return "LINE";
    case ft_polygon:
        return "POLYGON";
    case ft_online:
        return "ONLINE";
    case ft_property:
        return "PROPERTY";
    case ft_uint32:
        return "UINT32";
    case ft_uint64:
        return "UINT64";
    case ft_hash_64:
        return "HASH_64";
    case ft_hash_128:
        return "HASH_128";
    case ft_int8:
        return "INT8";
    case ft_uint8:
        return "UINT8";
    case ft_int16:
        return "INT16";
    case ft_uint16:
        return "UINT16";
    case ft_double:
        return "DOUBLE";
    case ft_raw:
        return "RAW";
    default:
        return "UNKNOWN";
    }
    return "UNKNOWN";
}

bool FieldConfig::IsMultiValue() const { return _impl->isMultiValue; }
fieldid_t FieldConfig::GetFieldId() const { return _impl->fieldId; }
const std::string& FieldConfig::GetFieldName() const { return _impl->fieldName; }
FieldType FieldConfig::GetFieldType() const { return _impl->fieldType; }
const std::string& FieldConfig::GetDefaultValue() const { return _impl->defaultStrValue; };
const autil::legacy::json::JsonMap& FieldConfig::GetUserDefinedParam() const { return _impl->userDefinedParam; }

void FieldConfig::SetFieldId(fieldid_t fieldId) { _impl->fieldId = fieldId; }
void FieldConfig::SetFieldName(const std::string& fieldName) { _impl->fieldName = fieldName; }
void FieldConfig::SetFieldType(FieldType fieldType) { _impl->fieldType = fieldType; }
void FieldConfig::SetIsMultiValue(bool isMultiValue) { _impl->isMultiValue = isMultiValue; }
void FieldConfig::SetDefaultValue(const std::string& defaultValue) { _impl->defaultStrValue = defaultValue; }
void FieldConfig::SetUserDefinedParam(const autil::legacy::json::JsonMap& param) { _impl->userDefinedParam = param; }
int32_t FieldConfig::GetFixedMultiValueCount() const { return _impl->fixedMultiValueCount; }

void FieldConfig::SetFixedMultiValueCount(int32_t fixedMultiValueCount)
{
    _impl->fixedMultiValueCount = fixedMultiValueCount;
}

bool FieldConfig::IsEnableNullField() const { return _impl->supportNull; }

void FieldConfig::SetEnableNullField(bool supportNull) { _impl->supportNull = supportNull; }

void FieldConfig::SetBinary(bool isBinary) { _impl->isBinaryField = isBinary; }

bool FieldConfig::IsBinary() const { return _impl->isBinaryField; }

int32_t FieldConfig::GetDefaultTimeZoneDelta() const { return _impl->defaultTimeZoneDelta; }
void FieldConfig::SetDefaultTimeZoneDelta(int32_t timeZoneDelta) { _impl->defaultTimeZoneDelta = timeZoneDelta; }

const std::string& FieldConfig::GetSeparator() const { return _impl->separator; }
void FieldConfig::SetSeparator(const std::string& separator) { _impl->separator = separator; }
const std::string& FieldConfig::GetAnalyzerName() const { return _impl->analyzerName; }
void FieldConfig::SetAnalyzerName(const std::string& analyzerName) { _impl->analyzerName = analyzerName; }

bool FieldConfig::IsVirtual() const { return _impl->isVirtual; }
void FieldConfig::SetVirtual(bool isVirtual) { _impl->isVirtual = isVirtual; }

bool FieldConfig::IsBuiltInField() const { return _impl->isBuiltInField; }
void FieldConfig::SetBuiltInField(bool isBuiltIn) { _impl->isBuiltInField = isBuiltIn; }

const std::string& FieldConfig::GetNullFieldLiteralString() const { return _impl->nullFieldString; }
void FieldConfig::SetNullFieldLiteralString(const std::string& nullStr) { _impl->nullFieldString = nullStr; }
bool FieldConfig::SupportSort() const
{
    if (IsMultiValue()) {
        return false;
    }
    auto fieldType = GetFieldType();
    return fieldType == ft_integer || fieldType == ft_uint32 || fieldType == ft_long || fieldType == ft_uint64 ||
           fieldType == ft_int8 || fieldType == ft_uint8 || fieldType == ft_int16 || fieldType == ft_uint16 ||
           fieldType == ft_double || fieldType == ft_float || fieldType == ft_fp8 || fieldType == ft_fp16 ||
           fieldType == ft_time || fieldType == ft_date || fieldType == ft_timestamp;
}

Status FieldConfig::CheckEqual(const FieldConfig& other) const
{
    CHECK_CONFIG_EQUAL(GetFieldId(), other.GetFieldId(), "FieldId not equal");
    CHECK_CONFIG_EQUAL(IsMultiValue(), other.IsMultiValue(), "MultiValue not equal");
    CHECK_CONFIG_EQUAL(IsBinary(), other.IsBinary(), "BinaryField not equal");
    CHECK_CONFIG_EQUAL(GetFieldName(), other.GetFieldName(), "FieldName not equal");
    CHECK_CONFIG_EQUAL(GetFixedMultiValueCount(), other.GetFixedMultiValueCount(), "FixedMultiValueCount not equal");
    CHECK_CONFIG_EQUAL(GetSeparator(), other.GetSeparator(), "separator not equal");
    CHECK_CONFIG_EQUAL(GetFieldType(), other.GetFieldType(), "FieldType not equal");
    CHECK_CONFIG_EQUAL(GetDefaultTimeZoneDelta(), other.GetDefaultTimeZoneDelta(), "DefaultTimeZoneDelta not equal");
    CHECK_CONFIG_EQUAL(GetDefaultValue(), other.GetDefaultValue(), "DefaultValue not equal");
    CHECK_CONFIG_EQUAL(IsEnableNullField(), other.IsEnableNullField(), "SupportNull not equal");
    CHECK_CONFIG_EQUAL(GetNullFieldLiteralString(), other.GetNullFieldLiteralString(), "NullFieldString not equal");
    CHECK_CONFIG_EQUAL(GetAnalyzerName(), other.GetAnalyzerName(), "AnalyzerName not equal");
    if (!autil::legacy::json::Equal(GetUserDefinedParam(), other.GetUserDefinedParam())) {
        AUTIL_LOG(ERROR, "UserDefinedParamV2 not equal");
        return Status::ConfigError("UserDefinedParamV2 not equal");
    }
    return Status::OK();
}

bool FieldConfig::IsIntegerType(FieldType fieldType)
{
    switch (fieldType) {
    case ft_int32:
    case ft_int64:
    case ft_uint32:
    case ft_uint64:
    case ft_int8:
    case ft_uint8:
    case ft_int16:
    case ft_uint16:
        return true;
    default:
        return false;
    }
    return false;
}

std::unique_ptr<FieldConfig> FieldConfig::TEST_Create(const std::string& fieldStr)
{
    static constexpr char DELIM = ':';
    auto strVec = autil::StringTokenizer::tokenize(autil::StringView(fieldStr), DELIM);
    if (strVec.size() < 2) {
        return nullptr;
    }
    FieldType fieldType;
    if (!StrToFieldType(strVec[1], fieldType)) {
        return nullptr;
    }
    bool isMulti = false;
    if (strVec.size() > 2 && !autil::StringUtil::fromString<bool>(strVec[2], isMulti)) {
        return nullptr;
    }
    return std::make_unique<FieldConfig>(strVec[0], fieldType, isMulti);
}

std::vector<std::shared_ptr<FieldConfig>> FieldConfig::TEST_CreateFields(const std::string& fieldStr)
{
    static constexpr char DELIM = ';';
    auto strVec = autil::StringTokenizer::tokenize(autil::StringView(fieldStr), DELIM);
    std::vector<std::shared_ptr<FieldConfig>> fields;
    fieldid_t fieldId = 0;
    for (const auto& str : strVec) {
        auto field = TEST_Create(str);
        if (!field) {
            return {};
        }
        field->SetFieldId(fieldId++);
        fields.emplace_back(std::move(field));
    }
    return fields;
}

} // namespace indexlibv2::config
