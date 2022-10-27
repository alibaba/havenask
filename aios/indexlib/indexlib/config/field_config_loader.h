#ifndef __INDEXLIB_FIELD_CONFIG_LOADER_H
#define __INDEXLIB_FIELD_CONFIG_LOADER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/field_schema.h"

IE_NAMESPACE_BEGIN(config);

class FieldConfigLoader
{
public:
    FieldConfigLoader();
    ~FieldConfigLoader();
    
public:
    static void Load(const autil::legacy::Any& any, FieldSchemaPtr& fieldSchema);

    static void Load(const autil::legacy::Any& any, FieldSchemaPtr& fieldSchema,
                     std::vector<FieldConfigPtr> &fieldConfigs);
    
    static FieldConfigPtr LoadFieldConfig(const autil::legacy::Any& any,
            FieldSchemaPtr& fieldSchema);


    static FieldConfigPtr AddFieldConfig(
            FieldSchemaPtr& fieldSchema, const std::string& fieldName, 
            FieldType fieldType, bool multiValue,
            bool isVirtual, bool isBinary);
    
    static EnumFieldConfigPtr AddEnumFieldConfig(
            FieldSchemaPtr& fieldSchema, const std::string& fieldName, 
            FieldType fieldType, std::vector<std::string>& validValues, 
            bool multiValue = false);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FieldConfigLoader);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_FIELD_CONFIG_LOADER_H
