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

#include "autil/OptionParser.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace config {

class CLIOption
{
public:
    CLIOption(std::string shortOptName_, std::string longOptName_, std::string key_, bool required_ = false,
              std::string defaultValue_ = std::string())
        : shortOptName(shortOptName_)
        , longOptName(longOptName_)
        , key(key_)
        , required(required_)
        , defaultValue(defaultValue_)
    {
    }
    ~CLIOption() {}

public:
    void registerOption(autil::OptionParser& optionParser) const
    {
        optionParser.addOption(shortOptName, longOptName, key, autil::OptionParser::OPT_STRING, required);
    }

public:
    std::string shortOptName;
    std::string longOptName;
    std::string key;
    bool required;
    std::string defaultValue;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(CLIOption);

}} // namespace build_service::config
