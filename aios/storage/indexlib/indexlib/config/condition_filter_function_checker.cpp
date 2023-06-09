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
#include "indexlib/config/condition_filter_function_checker.h"

#include "autil/StringUtil.h"
#include "indexlib/config/field_type_traits.h"
#include "indexlib/index_define.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace indexlib::util;

namespace indexlib { namespace config {
IE_LOG_SETUP(config, ConditionFilterFunctionChecker);

ConditionFilterFunctionChecker::ConditionFilterFunctionChecker() {}

ConditionFilterFunctionChecker::~ConditionFilterFunctionChecker() {}

void ConditionFilterFunctionChecker::init(const AttributeSchemaPtr& attrSchema)
{
    mBuildInFunc.insert(TIME_TO_NOW_FUNCTION);
    mAttrSchema = attrSchema;
}

void ConditionFilterFunctionChecker::CheckFunction(const ConditionFilterImpl::FilterFunctionConfig& functionConfig,
                                                   const string& fieldName, const std::string& valueType)
{
    if (mBuildInFunc.find(functionConfig.funcName) == mBuildInFunc.end() && !functionConfig.funcName.empty()) {
        INDEXLIB_FATAL_ERROR(Schema, "function [%s] is not buildin func", functionConfig.funcName.c_str());
    }
    CheckFuncValid(functionConfig, fieldName, valueType);
}

void ConditionFilterFunctionChecker::CheckFuncValid(const ConditionFilterImpl::FilterFunctionConfig& functionConfig,
                                                    const string& fieldName, const string& valueType)
{
    if (functionConfig.funcName == TIME_TO_NOW_FUNCTION) {
        AttributeConfigPtr attrConfig = mAttrSchema->GetAttributeConfig(fieldName);
        assert(attrConfig);
        FieldType fieldType = attrConfig->GetFieldType();
        switch (fieldType) {
#define MACRO(type)                                                                                                    \
    case type: {                                                                                                       \
        break;                                                                                                         \
    }
            BASIC_NUMBER_FIELD_MACRO_HELPER(MACRO)
#undef MACRO
        default:
            INDEXLIB_FATAL_ERROR(Schema, "time_to_now func not support attribute [%s] not number type",
                                 fieldName.c_str());
        }
        if (valueType == "STRING") {
            INDEXLIB_FATAL_ERROR(Schema, "time_to_now func not value type string");
        }
        string timeUnit = GetValueFromKeyValueMap(functionConfig.param, "time_unit");
        if (!timeUnit.empty() && timeUnit != "second" && timeUnit != "millisecond") {
            INDEXLIB_FATAL_ERROR(Schema, "time_to_now time unit not support [%s], only support second, millisecond",
                                 timeUnit.c_str());
        }
    }
}

#define EQUAL_FILTER_FIELD_TYPE(MACRO)                                                                                 \
    MACRO(ft_int8);                                                                                                    \
    MACRO(ft_int16);                                                                                                   \
    MACRO(ft_int32);                                                                                                   \
    MACRO(ft_int64);                                                                                                   \
    MACRO(ft_uint8);                                                                                                   \
    MACRO(ft_uint16);                                                                                                  \
    MACRO(ft_uint32);                                                                                                  \
    MACRO(ft_uint64);                                                                                                  \
    MACRO(ft_float);                                                                                                   \
    MACRO(ft_double);                                                                                                  \
    MACRO(ft_string);

void ConditionFilterFunctionChecker::CheckValue(const std::string& type, const std::string& fieldName,
                                                const std::string& strValue, const std::string& valueType)
{
    AttributeConfigPtr attrConfig = mAttrSchema->GetAttributeConfig(fieldName);

    if (attrConfig == nullptr) {
        INDEXLIB_FATAL_ERROR(Schema, "field [%s] is not in schema", fieldName.c_str());
    }
    if (attrConfig->IsMultiValue()) {
        INDEXLIB_FATAL_ERROR(Schema, "field [%s] cannot be multi value", fieldName.c_str());
    }
    FieldType valueFieldType = FieldConfig::StrToFieldType(valueType);
    if (valueFieldType != FieldType::ft_string && valueFieldType != FieldType::ft_uint64 &&
        valueFieldType != FieldType::ft_int64 && valueFieldType != FieldType::ft_double) {
        INDEXLIB_FATAL_ERROR(Schema, "value type [%s] is invalid.", valueType.c_str());
    }
    if (valueFieldType == FieldType::ft_string && attrConfig->GetFieldType() != FieldType::ft_string) {
        INDEXLIB_FATAL_ERROR(Schema, "field [%s] is not string type, value type must be not string", fieldName.c_str());
    }
    if (type == "Range") {
        FieldType fieldType = attrConfig->GetFieldType();
        switch (fieldType) {
#define MACRO(type)                                                                                                    \
    case type: {                                                                                                       \
        break;                                                                                                         \
    }
            BASIC_NUMBER_FIELD_MACRO_HELPER(MACRO)
#undef MACRO
        default:
            INDEXLIB_FATAL_ERROR(Schema, "range field [%s] is not number type", fieldName.c_str());
        }

        if (!((strValue.find("[") != string::npos || strValue.find("(") != string::npos) &&
              (strValue.find("]") != string::npos || strValue.find(")") != string::npos))) {
            INDEXLIB_FATAL_ERROR(Schema, "[%s] range filer formate is invalid", fieldName.c_str());
        }
        vector<string> values = autil::StringUtil::split(strValue, ",");
        if (values.size() != 2) {
            INDEXLIB_FATAL_ERROR(Schema, "[%s] range filer formate is invalid", fieldName.c_str());
        }

        if (valueFieldType == FieldType::ft_string) {
            INDEXLIB_FATAL_ERROR(Schema, "[%s] range valueType not support string", fieldName.c_str());
        }
        return;
    } else if (type == "Equal") {
        FieldType fieldType = attrConfig->GetFieldType();
        switch (fieldType) {
#define MACRO(type)                                                                                                    \
    case type: {                                                                                                       \
        break;                                                                                                         \
    }
            EQUAL_FILTER_FIELD_TYPE(MACRO)
#undef MACRO
        default:
            INDEXLIB_FATAL_ERROR(Schema, "equal not support field type [%d]", fieldType);
        }
        return;
    } else {
        INDEXLIB_FATAL_ERROR(Schema, "type [%s] is invalid, it should be Range or Equal", type.c_str());
    }
}

#undef EQUAL_FILTER_FIELD_TYPE
}} // namespace indexlib::config
