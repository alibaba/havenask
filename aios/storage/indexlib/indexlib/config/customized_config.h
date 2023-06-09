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
#ifndef __INDEXLIB_CUSTOMIZED_CONFIG_H
#define __INDEXLIB_CUSTOMIZED_CONFIG_H

#include <memory>

#include "autil/legacy/jsonizable.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, CustomizedConfigImpl);

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
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CustomizedConfig);
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

#endif //__INDEXLIB_CUSTOMIZED_CONFIG_H
