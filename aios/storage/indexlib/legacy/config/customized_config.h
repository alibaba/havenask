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

#include <memory>

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/legacy/indexlib.h"

namespace indexlib::config {
class CustomizedConfigImpl;
typedef std::shared_ptr<CustomizedConfigImpl> CustomizedConfigImplPtr;
} // namespace indexlib::config

namespace indexlib { namespace config {

class CustomizedConfig : public autil::legacy::Jsonizable
{
public:
    CustomizedConfig();
    CustomizedConfig(const CustomizedConfig& other);
    ~CustomizedConfig();

public:
    const std::string& GetPluginName() const;
    bool GetParameters(const std::string& key, std::string& value) const;
    const std::map<std::string, std::string>& GetParameters() const;
    const std::string& GetId() const;

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void AssertEqual(const CustomizedConfig& other) const;

public:
    // for test
    void SetId(const std::string& id);
    void SetPluginName(const std::string& pluginName);

private:
    CustomizedConfigImplPtr mImpl;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<CustomizedConfig> CustomizedConfigPtr;
typedef std::vector<CustomizedConfigPtr> CustomizedConfigVector;

class CustomizedConfigHelper
{
public:
    static CustomizedConfigPtr FindCustomizedConfig(const CustomizedConfigVector& configs, const std::string& id)
    {
        for (auto config : configs) {
            if (config->GetId() == id) {
                return config;
            }
        }
        return CustomizedConfigPtr();
    }
};
}} // namespace indexlib::config
