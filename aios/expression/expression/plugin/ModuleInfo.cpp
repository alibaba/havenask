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
#include "expression/plugin/ModuleInfo.h"

using namespace std;
namespace expression {


void ModuleInfo::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
    json.Jsonize("module_name", moduleName);
    json.Jsonize("module_path", modulePath);
    json.Jsonize("parameters", parameters, KeyValueMap());
}

bool ModuleInfo::operator==(const ModuleInfo &other) const {
    if (&other == this) {
        return true;
    }
    return moduleName == other.moduleName
        && modulePath == other.modulePath
        && parameters == other.parameters;
}

}
