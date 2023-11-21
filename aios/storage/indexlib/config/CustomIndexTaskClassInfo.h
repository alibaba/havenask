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

namespace indexlibv2::config {

class CustomIndexTaskClassInfo : public autil::legacy::Jsonizable
{
public:
    CustomIndexTaskClassInfo();
    ~CustomIndexTaskClassInfo();

public:
    const std::string& GetClassName() const { return _className; }
    const std::map<std::string, std::string>& GetParameters() const { return _parameters; }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

public:
    void TEST_SetClassName(const std::string& className) { _className = className; }
    void TEST_AddParameter(const std::string& key, const std::string& value) { _parameters[key] = value; }

private:
    std::string _className;
    std::map<std::string, std::string> _parameters;
};

} // namespace indexlibv2::config
