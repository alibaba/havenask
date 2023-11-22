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
#include "build_service/config/TaskInputConfig.h"

#include <iosfwd>
#include <utility>

#include "autil/legacy/exception.h"

using namespace std;
using namespace autil::legacy;

namespace build_service { namespace config {
BS_LOG_SETUP(config, TaskInputConfig);

TaskInputConfig::TaskInputConfig() {}

TaskInputConfig::~TaskInputConfig() {}

void TaskInputConfig::Jsonize(Jsonizable::JsonWrapper& json)
{
    json.Jsonize("type", _type, _type);
    json.Jsonize("module_name", _moduleName, _moduleName);
    json.Jsonize("params", _parameters, KeyValueMap());
}

void TaskInputConfig::addIdentifier(const std::string& id) { _parameters["IOIdentifier"] = id; }

string TaskInputConfig::getIdentifier() const
{
    string identifier;
    getParam("IOIdentifier", &identifier);
    return identifier;
}

bool TaskInputConfig::getParam(const std::string& key, std::string* value) const
{
    auto iter = _parameters.find(key);
    if (iter == _parameters.end()) {
        return false;
    }
    *value = iter->second;
    return true;
}

}} // namespace build_service::config
