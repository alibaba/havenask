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
#include <string>

#include "autil/legacy/jsonizable.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace config {

class TaskInputConfig : public autil::legacy::Jsonizable
{
public:
    TaskInputConfig();
    TaskInputConfig(const std::string& type, const std::string& moduleName, const KeyValueMap& parameters)
        : _type(type)
        , _moduleName(moduleName)
        , _parameters(parameters)
    {
    }
    ~TaskInputConfig();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void setType(const std::string& type) { _type = type; }
    const std::string& getType() const { return _type; }
    const KeyValueMap& getParameters() const { return _parameters; }
    void addParameters(const std::string& key, const std::string& value) { _parameters[key] = value; }
    bool getParam(const std::string& key, std::string* value) const;
    const std::string& getModuleName() const { return _moduleName; }
    void addIdentifier(const std::string& id);
    std::string getIdentifier() const;

private:
    std::string _type;
    std::string _moduleName;
    KeyValueMap _parameters;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(TaskInputConfig);

}} // namespace build_service::config
