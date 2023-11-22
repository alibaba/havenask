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
#include "indexlib/config/module_class_config.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;

namespace indexlib { namespace config {
AUTIL_LOG_SETUP(indexlib.config, ModuleClassConfig);

ModuleClassConfig::ModuleClassConfig() {}

ModuleClassConfig::~ModuleClassConfig() {}

void ModuleClassConfig::Jsonize(Jsonizable::JsonWrapper& json)
{
    json.Jsonize("class_name", className, className);
    json.Jsonize("module_name", moduleName, moduleName);
    json.Jsonize("parameters", parameters, parameters);
}

bool ModuleClassConfig::operator==(const ModuleClassConfig& other) const
{
    return className == other.className && moduleName == other.moduleName && parameters == other.parameters;
}

ModuleClassConfig& ModuleClassConfig::operator=(const ModuleClassConfig& other)
{
    className = other.className;
    moduleName = other.moduleName;
    parameters = other.parameters;
    return *this;
}
}} // namespace indexlib::config
