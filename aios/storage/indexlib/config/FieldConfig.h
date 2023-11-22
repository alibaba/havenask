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
#pragma once

#include <memory>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "autil/legacy/json.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/base/FieldType.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"

namespace indexlibv2::config {

class FieldConfig : public autil::legacy::Jsonizable
{
public:
    FieldConfig();
    FieldConfig(const std::string& fieldName, FieldType fieldType, bool multiValue);
    FieldConfig(const std::string& fieldName, FieldType fieldType, bool multiValue, bool isVirtual, bool isBinary);
    FieldConfig(const FieldConfig& other);
    FieldConfig& operator=(const FieldConfig& other);
    ~FieldConfig();

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void Check() const;

public:
    fieldid_t GetFieldId() const;
    const std::string& GetFieldName() const;
    FieldType GetFieldType() const;
    bool IsMultiValue() const;
    const std::string& GetDefaultValue() const;
    int32_t GetFixedMultiValueCount() const;
    bool IsEnableNullField() const;
    bool IsSortField() const;
    bool IsBinary() const;
    int32_t GetDefaultTimeZoneDelta() const;
    const std::string& GetSeparator() const;
    const std::string& GetNullFieldLiteralString() const;
    bool SupportSort() const;
    const std::string& GetAnalyzerName() const;
    bool IsVirtual() const;

    const autil::legacy::json::JsonMap& GetUserDefinedParam() const;

public:
    void SetFieldId(fieldid_t fieldId);
    void SetFieldName(const std::string& fieldName);
    void SetFieldType(FieldType fieldType);
    void SetIsMultiValue(bool isMultiValue);
    void SetDefaultValue(const std::string& defaultValue);
    void SetFixedMultiValueCount(int32_t fixedMultiValueCount);
    void SetEnableNullField(bool supportNull);
    void SetBinary(bool isBinary);
    void SetDefaultTimeZoneDelta(int32_t timeZoneDelta);
    void SetSeparator(const std::string& separator);
    void SetNullFieldLiteralString(const std::string& nullStr);
    void SetAnalyzerName(const std::string& analyzerName);
    void SetVirtual(bool isVirtual);
    void SetUserDefinedParam(const autil::legacy::json::JsonMap& param);

public:
    Status CheckEqual(const FieldConfig& other) const;

public:
    static const char* FieldTypeToStr(FieldType fieldType);
    static bool StrToFieldType(const std::string& typeStr, FieldType& type);
    static bool IsIntegerType(FieldType fieldType);

private:
    void CheckFixedLengthMultiValue() const;

public:
    // format0: fieldName:fieldType:isMulti
    static std::unique_ptr<FieldConfig> TEST_Create(const std::string& fieldStr);
    // format1: format0;format0;format0
    static std::vector<std::shared_ptr<FieldConfig>> TEST_CreateFields(const std::string& fieldStr);

public:
    inline static const std::string FIELD_NAME = "field_name";
    inline static const std::string FIELD_TYPE = "field_type";
    inline static const std::string FIELD_MULTI_VALUE = "multi_value";
    inline static const std::string FIELD_USER_DEFINED_PARAM = "user_defined_param";
    inline static const std::string FIELD_DEFAULT_VALUE = "default_value";
    inline static const std::string FIELD_DEFAULT_STR_VALUE = "";
    inline static const std::string FIELD_FIXED_MULTI_VALUE_COUNT = "fixed_multi_value_count";
    inline static const std::string SORT_FIELD = "sort_field";
    inline static const std::string FIELD_SUPPORT_NULL = "enable_null";
    inline static const std::string FIELD_BINARY = "binary_field";
    inline static const std::string FIELD_DEFAULT_TIME_ZONE = "default_time_zone";
    inline static const std::string FIELD_SEPARATOR = "separator";
    inline static const std::string DEFAULT_NULL_FIELD_STRING = "__NULL__";
    inline static const std::string FIELD_DEFAULT_NULL_STRING_VALUE = "default_null_string";
    inline static const std::string FIELD_ANALYZER = "analyzer";
    inline static const std::string FIELD_VIRTUAL = "virtual";

private:
    struct Impl;

    std::unique_ptr<Impl> _impl;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::config
