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
#include "build_service/config/TaskControllerConfig.h"

using namespace std;

namespace build_service { namespace config {
BS_LOG_SETUP(config, TaskControllerConfig);

TaskControllerConfig::TaskControllerConfig() : _type(CT_BUILDIN) {}

TaskControllerConfig::~TaskControllerConfig() {}

void TaskControllerConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    if (json.GetMode() == FROM_JSON) {
        string type;
        json.Jsonize("type", type);
        _type = strToType(type);
    } else {
        string typeStr = typeToStr();
        json.Jsonize("type", typeStr);
    }
    json.Jsonize("target_configs", _targets);
}

std::string TaskControllerConfig::typeToStr()
{
    if (_type == CT_BUILDIN) {
        return "BUILDIN";
    }
    if (_type == CT_CUSTOM) {
        return "CUSTOM";
    }
    return "UNKOWN";
}

TaskControllerConfig::Type TaskControllerConfig::strToType(const std::string& str)
{
    if (str == "BUILDIN") {
        return CT_BUILDIN;
    }

    if (str == "CUSTOM") {
        return CT_CUSTOM;
    }
    return CT_UNKOWN;
}

}} // namespace build_service::config
