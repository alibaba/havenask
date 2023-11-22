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
#include "indexlib/config/virtual_attribute_config_creator.h"

#include "indexlib/config/field_config.h"

using namespace std;

namespace indexlib { namespace config {
AUTIL_LOG_SETUP(indexlib.config, VirtualAttributeConfigCreator);

AttributeConfigPtr VirtualAttributeConfigCreator::Create(const string& name, FieldType type, bool multiValue,
                                                         const string& defaultValue,
                                                         const common::AttributeValueInitializerCreatorPtr& creator)
{
    FieldConfigPtr fieldConfig(new FieldConfig(name, type, multiValue, true, false));
    fieldConfig->SetDefaultValue(defaultValue);
    AttributeConfigPtr attrConfig(new AttributeConfig(AttributeConfig::ct_virtual));
    attrConfig->Init(fieldConfig, creator);
    return attrConfig;
}
}} // namespace indexlib::config
