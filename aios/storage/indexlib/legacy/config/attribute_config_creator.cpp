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
#include "indexlib/config/attribute_config_creator.h"

#include "indexlib/config/FileCompressConfig.h"
#include "indexlib/config/IndexConfigDeserializeResource.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace indexlib { namespace config {
AUTIL_LOG_SETUP(indexlib.config, AttributeConfigCreator);

AttributeConfigCreator::AttributeConfigCreator() {}

AttributeConfigCreator::~AttributeConfigCreator() {}

AttributeConfigPtr AttributeConfigCreator::Create(const FieldSchemaPtr& fieldSchema,
                                                  const std::shared_ptr<FileCompressSchema>& fileCompressSchema,
                                                  const Any& any)
{
    JsonMap attribute = AnyCast<JsonMap>(any);
    CustomizedConfigVector customizedConfigs;
    auto customizedConfigIter = attribute.find(CUSTOMIZED_CONFIG);
    if (customizedConfigIter != attribute.end()) {
        JsonArray customizedConfigVec = AnyCast<JsonArray>(customizedConfigIter->second);
        customizedConfigs.reserve(customizedConfigVec.size());
        for (JsonArray::iterator configIter = customizedConfigVec.begin(); configIter != customizedConfigVec.end();
             ++configIter) {
            JsonMap customizedConfigMap = AnyCast<JsonMap>(*configIter);
            Jsonizable::JsonWrapper jsonWrapper(customizedConfigMap);
            CustomizedConfigPtr customizedConfig(new CustomizedConfig());
            customizedConfig->Jsonize(jsonWrapper);
            customizedConfigs.push_back(customizedConfig);
        }
    }

    auto fieldIter = attribute.find(FIELD_NAME);
    if (fieldIter == attribute.end()) {
        INDEXLIB_FATAL_ERROR(Schema,
                             "new-attribute format undefined field name, like \"field\" : \"name\", schema is[%s]",
                             ToJsonString(any).c_str());
    }
    string fieldName = AnyCast<string>(fieldIter->second);
    AttributeConfigPtr config = Create(fieldSchema, fieldName, customizedConfigs);
    auto fileCompressIter = attribute.find(FILE_COMPRESS);
    if (fileCompressIter != attribute.end()) {
        string fileCompress = AnyCast<string>(fileCompressIter->second);
        std::shared_ptr<FileCompressConfig> fileCompressConfig;
        if (fileCompressSchema) {
            fileCompressConfig = fileCompressSchema->GetFileCompressConfig(fileCompress);
        }
        if (!fileCompressConfig && !fileCompress.empty()) {
            fileCompressConfig = std::make_shared<FileCompressConfig>(fileCompress);
        }
        config->SetFileCompressConfig(fileCompressConfig);
    }

    indexlibv2::config::MutableJson runtimeSetting;
    indexlibv2::config::IndexConfigDeserializeResource resource({fieldSchema->GetFieldConfig(fieldName)},
                                                                runtimeSetting);
    config->Deserialize(any, INVALID_ATTRID, resource);
    return config;
}

AttributeConfigPtr AttributeConfigCreator::Create(const FieldSchemaPtr& fieldSchema, const string& fieldName,
                                                  const CustomizedConfigVector& customizedConfigs)
{
    assert(fieldSchema);
    FieldConfigPtr fieldConfig = fieldSchema->GetFieldConfig(fieldName);
    if (!fieldConfig) {
        INDEXLIB_FATAL_ERROR(Schema, "No such field defined: fieldName:%s", fieldName.c_str());
    }
    AttributeConfigPtr attrConfig(new AttributeConfig(AttributeConfig::ct_normal));
    attrConfig->Init(fieldConfig, common::AttributeValueInitializerCreatorPtr(), customizedConfigs);
    return attrConfig;
}
}} // namespace indexlib::config
