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
#include "indexlib/config/field_config_loader.h"

#include "indexlib/config/configurator_define.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/TimestampUtil.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace autil;

using namespace indexlib::util;

namespace indexlib { namespace config {
AUTIL_LOG_SETUP(indexlib.config, FieldConfigLoader);

FieldConfigLoader::FieldConfigLoader() {}

FieldConfigLoader::~FieldConfigLoader() {}

void FieldConfigLoader::Load(const Any& any, FieldSchemaPtr& fieldSchema)
{
    JsonArray fields = AnyCast<JsonArray>(any);
    if (fields.empty()) {
        AUTIL_LOG(INFO, "empty fields section defined");
    }

    JsonArray::iterator iter = fields.begin();
    for (; iter != fields.end(); ++iter) {
        LoadFieldConfig(*iter, fieldSchema);
    }
}

void FieldConfigLoader::Load(const Any& any, FieldSchemaPtr& fieldSchema, vector<FieldConfigPtr>& fieldConfigs)
{
    JsonArray fields = AnyCast<JsonArray>(any);
    if (fields.empty()) {
        AUTIL_LOG(INFO, "empty fields section defined");
    }

    JsonArray::iterator iter = fields.begin();
    for (; iter != fields.end(); ++iter) {
        fieldConfigs.push_back(LoadFieldConfig(*iter, fieldSchema));
    }
}

FieldConfigPtr FieldConfigLoader::LoadFieldConfig(const Any& any, FieldSchemaPtr& fieldSchema)
{
    JsonMap field = AnyCast<JsonMap>(any);
    string fieldName;
    FieldType fieldType;
    string analyzerName;

    ParseBasicElements(field, fieldName, fieldType, analyzerName);
    bool multiValue = false;
    auto iter = field.find(FIELD_MULTI_VALUE);
    if (iter != field.end()) {
        multiValue = AnyCast<bool>(iter->second);
    }

    string seperator = INDEXLIB_MULTI_VALUE_SEPARATOR_STR;
    if (multiValue) {
        iter = field.find(FIELD_SEPARATOR);
        if (iter != field.end()) {
            seperator = AnyCast<string>(iter->second);
        }
    }

    bool isBinary = false;
    iter = field.find(FIELD_BINARY);
    if (iter != field.end()) {
        isBinary = AnyCast<bool>(iter->second);
    }

    bool isVirtual = false;
    iter = field.find(FIELD_VIRTUAL);
    if (iter != field.end()) {
        isVirtual = AnyCast<bool>(iter->second);
    }

    if (fieldType == ft_enum) {
        iter = field.find(FIELD_VALID_VALUES);
        if (iter == field.end()) {
            INDEXLIB_FATAL_ERROR(Schema, "no valid_values for enum field");
        }
        // TODO, how to handle other type, such as integer, float.
        JsonArray values = AnyCast<JsonArray>(iter->second);
        vector<string> validValues;
        validValues.reserve(values.size());
        for (JsonArray::iterator it = values.begin(); it != values.end(); ++it) {
            validValues.push_back(AnyCast<string>(*it));
        }
        return AddEnumFieldConfig(fieldSchema, fieldName, fieldType, validValues, multiValue);
    }

    FieldConfigPtr result = AddFieldConfig(fieldSchema, fieldName, fieldType, multiValue, isVirtual, isBinary);
    result->SetAnalyzerName(analyzerName);
    result->SetSeparator(seperator);
    LoadPropertysForFieldConfig(result, field);
    result->Check();
    return result;
}

FieldConfigPtr FieldConfigLoader::AddFieldConfig(FieldSchemaPtr& fieldSchema, const std::string& fieldName,
                                                 FieldType fieldType, bool multiValue, bool isVirtual, bool isBinary)
{
    if (isBinary) {
        if (!multiValue || fieldType != ft_string) {
            AUTIL_LOG(ERROR, "Only MultiString field support isBinary = true");
            isBinary = false;
        }
    }

    FieldConfigPtr fieldConfig(new FieldConfig(fieldName, fieldType, multiValue, isVirtual, isBinary));
    if (fieldSchema.get() == NULL) {
        fieldSchema.reset(new FieldSchema);
    }
    fieldSchema->AddFieldConfig(fieldConfig);
    return fieldConfig;
}

EnumFieldConfigPtr FieldConfigLoader::AddEnumFieldConfig(FieldSchemaPtr& fieldSchema, const std::string& fieldName,
                                                         FieldType fieldType, std::vector<std::string>& validValues,
                                                         bool multiValue)
{
    EnumFieldConfigPtr enumField(new EnumFieldConfig(fieldName, false));
    if (fieldSchema.get() == NULL) {
        fieldSchema.reset(new FieldSchema);
    }
    fieldSchema->AddFieldConfig(static_pointer_cast<FieldConfig>(enumField));
    enumField->AddValidValue(validValues);
    return enumField;
}

void FieldConfigLoader::ParseBasicElements(const JsonMap& field, string& fieldName, FieldType& fieldType,
                                           string& analyzerName)
{
    // parse field name.
    JsonMap::const_iterator iter = field.find(FIELD_NAME);
    if (iter == field.end()) {
        INDEXLIB_FATAL_ERROR(Schema, "field_name absent in field define");
    }

    fieldName = AnyCast<string>(iter->second);
    if (fieldName.empty()) {
        INDEXLIB_FATAL_ERROR(Schema, "field_name must not be empty");
    }

    if (fieldName.find('.') != string::npos) {
        INDEXLIB_FATAL_ERROR(Schema, "field_name[%s] must not contain dot charactor", fieldName.c_str());
    }

    // parse field_type
    iter = field.find(FIELD_TYPE);
    if (iter == field.end()) {
        INDEXLIB_FATAL_ERROR(Schema, "field_type absent in field define, field_name[%s]", fieldName.c_str());
    }
    fieldType = FieldConfig::StrToFieldType(AnyCast<string>(iter->second));

    // parse analyzer name.
    iter = field.find(FIELD_ANALYZER);
    if (fieldType == ft_text) {
        if (iter == field.end()) {
            stringstream ss;
            ss << "analyzer_name absent in text field definition: fieldName = " << fieldName;
            INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
        }
        analyzerName = AnyCast<string>(iter->second);
        if (analyzerName.empty()) {
            stringstream ss;
            ss << "analyzer_name must not be empty: fieldName = " << fieldName;
            INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
        }
    } else if (iter != field.end()) {
        stringstream ss;
        ss << "analyzer_name must be empty in non text field definition: fieldName = " << fieldName;
        INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
    }
}

void FieldConfigLoader::LoadPropertysForFieldConfig(const FieldConfigPtr& config, JsonMap& field)
{
    assert(config);
    bool uniqEncode = false;
    auto iter = field.find(ATTRIBUTE_UNIQ_ENCODE);
    if (iter != field.end()) {
        uniqEncode = AnyCast<bool>(iter->second);
    }

    // decode compress
    iter = field.find(index::COMPRESS_TYPE);
    if (iter != field.end()) {
        string compressStr = AnyCast<string>(iter->second);
        config->SetCompressType(compressStr);
    }

    iter = field.find(FIELD_DEFAULT_VALUE);
    if (iter != field.end()) {
        string defaultAttributeValue = AnyCast<string>(iter->second);
        config->SetDefaultValue(defaultAttributeValue);
    }

    config->SetUniqEncode(uniqEncode);
    iter = field.find(ATTRIBUTE_UPDATABLE_MULTI_VALUE);
    if (iter != field.end()) {
        config->SetUpdatableMultiValue(AnyCast<bool>(iter->second));
    }

    iter = field.find(FieldConfig::FIELD_FIXED_MULTI_VALUE_COUNT);
    if (iter != field.end()) {
        config->SetFixedMultiValueCount(JsonNumberCast<int32_t>(iter->second));
    }

    iter = field.find(index::ATTRIBUTE_DEFRAG_SLICE_PERCENT);
    if (iter != field.end()) {
        config->SetDefragSlicePercent(JsonNumberCast<uint64_t>(iter->second));
    }

    iter = field.find(FIELD_USER_DEFINED_PARAM);
    if (iter != field.end()) {
        const JsonMap& jsonMap = AnyCast<JsonMap>(iter->second);
        config->SetUserDefinedParam(jsonMap);
    }

    iter = field.find(FIELD_SUPPORT_NULL);
    if (iter != field.end()) {
        config->SetEnableNullField(AnyCast<bool>(iter->second));
    }

    iter = field.find(FIELD_DEFAULT_NULL_STRING_VALUE);
    if (iter != field.end()) {
        config->SetNullFieldLiteralString(AnyCast<string>(iter->second));
    }

    iter = field.find(FIELD_USER_DEFINE_ATTRIBUTE_NULL_VALUE);
    if (iter != field.end()) {
        config->SetUserDefineAttributeNullValue(AnyCast<string>(iter->second));
    }

    iter = field.find(FIELD_DEFAULT_TIME_ZONE);
    if (iter != field.end()) {
        string timeZoneDelta = AnyCast<string>(iter->second);
        int32_t defaultTimeZoneDelta;
        if (!TimestampUtil::ParseTimeZone(StringView(timeZoneDelta), defaultTimeZoneDelta)) {
            INDEXLIB_FATAL_ERROR(Schema, "Given default time zone delta %s is invalid", timeZoneDelta.c_str());
        } else {
            config->SetDefaultTimeZoneDelta(defaultTimeZoneDelta);
        }
    }

    iter = field.find(FIELD_IS_BUILT_IN);
    if (iter != field.end()) {
        config->SetBuiltInField(AnyCast<bool>(iter->second));
    }
}
}} // namespace indexlib::config
