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
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace config {

class ProcessorInfo : public autil::legacy::Jsonizable
{
public:
    ProcessorInfo(const std::string& className_ = "", const std::string& moduleName_ = "",
                  const KeyValueMap& parameters_ = KeyValueMap())
        : className(className_)
        , moduleName(moduleName_)
        , parameters(parameters_)
    {
    }
    ~ProcessorInfo() {}

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool operator==(const ProcessorInfo& other)
    {
        return className == other.className && moduleName == other.moduleName && parameters == other.parameters;
    }

public:
    std::string className;
    std::string moduleName;
    KeyValueMap parameters;
};

typedef std::vector<ProcessorInfo> ProcessorInfos;

}} // namespace build_service::config
