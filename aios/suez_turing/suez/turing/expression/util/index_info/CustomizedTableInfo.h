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

#include <map>
#include <stddef.h>
#include <stdint.h>
#include <string>

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"

namespace suez {
namespace turing {

class CustomizedTableInfo : public autil::legacy::Jsonizable {
public:
    CustomizedTableInfo();
    ~CustomizedTableInfo();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;

public:
    void setPluginName(const std::string &pluginName);
    void setIdentifier(const std::string &identifier);
    void setParameters(const std::map<std::string, std::string> &parameters);

    const std::string &getPluginName() const;
    const std::string &getIdentifier() const;
    bool getParameters(const std::string &key, std::string &value) const;
    const std::map<std::string, std::string> &getParameters() const;

private:
    std::string _identifier;
    std::string _pluginName;
    std::map<std::string, std::string> _parameters;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace turing
} // namespace suez
