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
#ifndef __INDEXLIB_FIELD_CONFIG_LOADER_H
#define __INDEXLIB_FIELD_CONFIG_LOADER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/field_schema.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace config {

class FieldConfigLoader
{
public:
    FieldConfigLoader();
    ~FieldConfigLoader();

public:
    static void Load(const autil::legacy::Any& any, FieldSchemaPtr& fieldSchema);

    static void Load(const autil::legacy::Any& any, FieldSchemaPtr& fieldSchema,
                     std::vector<FieldConfigPtr>& fieldConfigs);

    static FieldConfigPtr LoadFieldConfig(const autil::legacy::Any& any, FieldSchemaPtr& fieldSchema);

    static FieldConfigPtr AddFieldConfig(FieldSchemaPtr& fieldSchema, const std::string& fieldName, FieldType fieldType,
                                         bool multiValue, bool isVirtual, bool isBinary);

    static EnumFieldConfigPtr AddEnumFieldConfig(FieldSchemaPtr& fieldSchema, const std::string& fieldName,
                                                 FieldType fieldType, std::vector<std::string>& validValues,
                                                 bool multiValue = false);

private:
    static void ParseBasicElements(const autil::legacy::json::JsonMap& field, std::string& fieldName,
                                   FieldType& fieldType, std::string& analyzerName);

    static void LoadPropertysForFieldConfig(const FieldConfigPtr& config, autil::legacy::json::JsonMap& field);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FieldConfigLoader);
}} // namespace indexlib::config

#endif //__INDEXLIB_FIELD_CONFIG_LOADER_H
