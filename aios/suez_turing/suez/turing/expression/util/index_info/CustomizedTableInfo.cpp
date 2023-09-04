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
#include "suez/turing/expression/util/CustomizedTableInfo.h"

#include <cstddef>

#include "alog/Logger.h"
#include "autil/legacy/exception.h"

using namespace std;
using namespace autil::legacy;

namespace suez {
namespace turing {

AUTIL_LOG_SETUP(expression, CustomizedTableInfo);

CustomizedTableInfo::CustomizedTableInfo() {}

CustomizedTableInfo::~CustomizedTableInfo() {}

void CustomizedTableInfo::Jsonize(JsonWrapper &json) {
    json.Jsonize("identifier", _identifier);
    json.Jsonize("plugin_name", _pluginName);
    json.Jsonize("parameters", _parameters);
}

void CustomizedTableInfo::setPluginName(const std::string &pluginName) { _pluginName = pluginName; }

void CustomizedTableInfo::setIdentifier(const std::string &identifier) { _identifier = identifier; }

void CustomizedTableInfo::setParameters(const std::map<std::string, std::string> &parameters) {
    _parameters = parameters;
}

const std::string &CustomizedTableInfo::getPluginName() const { return _pluginName; }

const std::string &CustomizedTableInfo::getIdentifier() const { return _identifier; }

bool CustomizedTableInfo::getParameters(const std::string &key, std::string &value) const {
    const auto &it = _parameters.find(key);
    if (it != _parameters.end()) {
        value = it->second;
        return true;
    }
    return false;
}

const std::map<std::string, std::string> &CustomizedTableInfo::getParameters() const { return _parameters; }

} // namespace turing
} // namespace suez
