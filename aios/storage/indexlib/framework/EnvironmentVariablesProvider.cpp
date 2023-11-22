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
#include "indexlib/framework/EnvironmentVariablesProvider.h"

#include "autil/EnvUtil.h"

namespace indexlibv2::framework {

EnvironmentVariablesProvider::EnvironmentVariablesProvider() {}

EnvironmentVariablesProvider::~EnvironmentVariablesProvider() {}

void EnvironmentVariablesProvider::Init(const std::map<std::string, std::string>& defaultKeyValueList)
{
    _defaults = defaultKeyValueList;
}

std::string EnvironmentVariablesProvider::Get(const std::string& key, const std::string& defaultValue) const
{
    std::string ret;
    if (autil::EnvUtil::getEnvWithoutDefault(key, ret)) {
        return ret;
    }
    auto it = _defaults.find(key);
    if (it != _defaults.end()) {
        return it->second;
    }
    return defaultValue;
}

} // namespace indexlibv2::framework
