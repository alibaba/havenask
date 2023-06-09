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
#pragma once

#include "autil/legacy/jsonizable.h"
#include <unordered_map>

namespace navi {

struct NaviRegistryConfig : public autil::legacy::Jsonizable
{
public:
    NaviRegistryConfig()
        : config(nullptr)
    {
    }
public:
    bool loadConfig(const std::string &name, const std::string &configStr);
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;
public:
    std::shared_ptr<autil::legacy::RapidDocument> _document;
    std::string name;
    autil::legacy::RapidValue *config;
    std::map<std::string, bool> dependResourceMap;
};

class NaviRegistryConfigMap {
public:
    bool init(const std::vector<NaviRegistryConfig> &configVec);
    bool hasConfig(const std::string &name) const;
    const NaviRegistryConfig *getRegistryConfig(const std::string &name) const;
    autil::legacy::RapidValue *getConfig(const std::string &name) const;
    std::unordered_map<std::string, NaviRegistryConfig> &getConfigMap();
private:
    std::unordered_map<std::string, NaviRegistryConfig> _configMap;
};

}
