#include "indexlib/config/field_config_loader.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/misc/exception.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace autil;

IE_NAMESPACE_USE(util);

IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, FieldConfigLoader);

FieldConfigLoader::FieldConfigLoader() 
{
}

FieldConfigLoader::~FieldConfigLoader() 
{
}

void FieldConfigLoader::Load(const Any& any, FieldSchemaPtr& fieldSchema)
{
    JsonArray fields = AnyCast<JsonArray>(any);
    if (fields.empty())
    {
        IE_LOG(INFO, "empty fields section defined");
    }

    JsonArray::iterator iter = fields.begin();
    for (; iter != fields.end(); ++iter)
    {
        LoadFieldConfig(*iter, fieldSchema);
    }
}

void FieldConfigLoader::Load(const Any& any, FieldSchemaPtr& fieldSchema,
                             vector<FieldConfigPtr> &fieldConfigs)
{
    JsonArray fields = AnyCast<JsonArray>(any);
    if (fields.empty())
    {
        IE_LOG(INFO, "empty fields section defined");
    }

    JsonArray::iterator iter = fields.begin();
    for (; iter != fields.end(); ++iter)
    {
        fieldConfigs.push_back(LoadFieldConfig(*iter, fieldSchema));
    }
}

FieldConfigPtr FieldConfigLoader::LoadFieldConfig(
        const Any& any, FieldSchemaPtr& fieldSchema)
{
    JsonMap field = AnyCast<JsonMap>(any);

    //parse field name.
    JsonMap::iterator iter = field.find(FIELD_NAME);
    if (iter == field.end())
    {
        INDEXLIB_FATAL_ERROR(Schema, "field_name absent in field define");
    }
    string fieldName = AnyCast<string>(iter->second);
    if (fieldName.empty())
    {
        INDEXLIB_FATAL_ERROR(Schema, "field_name must not be empty");
    }

    if (fieldName.find('.') != string::npos)
    {
        INDEXLIB_FATAL_ERROR(Schema,
                       "field_name[%s] must not contain dot charactor", fieldName.c_str());
    }

    //parse field_type
    iter = field.find(FIELD_TYPE);
    if (iter == field.end())
    {
        INDEXLIB_FATAL_ERROR(Schema, "field_type absent in field define");
    }
    std::string tmp = AnyCast<string>(iter->second);
    FieldType fieldType = FieldConfig::StrToFieldType(tmp);

    //parse analyzer name.
    iter = field.find(FIELD_ANALYZER);
    string analyzerName;
    if (fieldType == ft_text)
    {
        if (iter == field.end())
        {
            stringstream ss;
            ss << "analyzer_name absent in text field definition: fieldName = " << fieldName;
            INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
        }
        analyzerName = AnyCast<string>(iter->second);
        if (analyzerName.empty())
        {
            stringstream ss;
            ss << "analyzer_name must not be empty: fieldName = " << fieldName;
            INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
        }
    }
    else if (iter != field.end())
    {
        stringstream ss;
        ss << "analyzer_name must be empty in non text field definition: fieldName = " << fieldName;
        INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
    }

    bool multiValue = false;
    iter = field.find(FIELD_MULTI_VALUE);
    if (iter != field.end())
    {
        multiValue = AnyCast<bool>(iter->second);
    }

    bool isBinary = false;
    iter = field.find(FIELD_BINARY);
    if (iter != field.end())
    {
        isBinary = AnyCast<bool>(iter->second);
    }

    bool isVirtual = false;
    iter = field.find(FIELD_VIRTUAL);
    if (iter != field.end())
    {
        isVirtual = AnyCast<bool>(iter->second);
    }

    if (fieldType == ft_enum)
    {
        iter = field.find(FIELD_VALID_VALUES);
        if (iter == field.end())
        {
            INDEXLIB_FATAL_ERROR(Schema, "no valid_values for enum field");
        }
        //TODO, how to handle other type, such as integer, float.
        JsonArray values = AnyCast<JsonArray>(iter->second);
        vector<string> validValues;
        for (JsonArray::iterator it = values.begin();
             it != values.end(); ++it)
        {
            validValues.push_back(AnyCast<string>(*it));
        }
        return AddEnumFieldConfig(fieldSchema, fieldName, fieldType, validValues, multiValue);
    }

    bool uniqEncode = false;
    iter = field.find(FIELD_UNIQ_ENCODE);
    if (iter != field.end())
    {
        uniqEncode = AnyCast<bool>(iter->second);
    }

    FieldConfigPtr result = AddFieldConfig(fieldSchema,
            fieldName, fieldType, multiValue, isVirtual, isBinary);
    //decode compress
    iter = field.find(COMPRESS_TYPE);
    if (iter != field.end())
    {
        string compressStr = AnyCast<string>(iter->second);
        result->SetCompressType(compressStr);
    }

    iter = field.find(FIELD_DEFAULT_VALUE);
    if (iter != field.end())
    {
        string defaultAttributeValue = AnyCast<string>(iter->second);
        result->SetDefaultValue(defaultAttributeValue);
    }
    
    
    result->SetAnalyzerName(analyzerName);
    result->SetUniqEncode(uniqEncode);

    iter = field.find(FIELD_UPDATABLE_MULTI_VALUE);
    if (iter != field.end())
    {
        result->SetUpdatableMultiValue(AnyCast<bool>(iter->second));
    }

    iter = field.find(FIELD_FIXED_MULTI_VALUE_COUNT);
    if (iter != field.end())
    {
        result->SetFixedMultiValueCount(JsonNumberCast<int32_t>(iter->second));
    }    

    iter = field.find(FIELD_DEFRAG_SLICE_PERCENT);
    if (iter != field.end())
    {
        result->SetDefragSlicePercent(JsonNumberCast<uint64_t>(iter->second));
    }

    iter = field.find(FIELD_USER_DEFINED_PARAM);
    if (iter != field.end())
    {
        KeyValueMap kvMap;
        const JsonMap& jsonMap = AnyCast<JsonMap>(iter->second);
        for (JsonMap::const_iterator it = jsonMap.begin();
             it != jsonMap.end(); ++it)
        {
            kvMap[it->first] = AnyCast<string>(it->second);
        }
        result->SetUserDefinedParam(kvMap);
    }
    result->Check();
    return result;
}

FieldConfigPtr FieldConfigLoader::AddFieldConfig(
        FieldSchemaPtr& fieldSchema, const std::string& fieldName, 
        FieldType fieldType, bool multiValue,
        bool isVirtual, bool isBinary)
{
    if (isBinary)
    {
        if (!multiValue || fieldType != ft_string)
        {
            IE_LOG(ERROR, "Only MultiString field support isBinary = true");
            isBinary = false;
        }
    }

    FieldConfigPtr fieldConfig(new FieldConfig(fieldName,
                    fieldType, multiValue, isVirtual, isBinary));
    if (fieldSchema.get() == NULL)
    {
        fieldSchema.reset(new FieldSchema);
    }
    fieldSchema->AddFieldConfig(fieldConfig);
    return fieldConfig;
}
    
EnumFieldConfigPtr FieldConfigLoader::AddEnumFieldConfig(
        FieldSchemaPtr& fieldSchema, const std::string& fieldName, 
        FieldType fieldType, std::vector<std::string>& validValues, 
        bool multiValue)
{
    EnumFieldConfigPtr enumField(new EnumFieldConfig(fieldName, false));
    if (fieldSchema.get() == NULL)
    {
        fieldSchema.reset(new FieldSchema);
    }
    fieldSchema->AddFieldConfig(static_pointer_cast<FieldConfig>(enumField));
    enumField->AddValidValue(validValues);
    return enumField;
}

IE_NAMESPACE_END(config);

