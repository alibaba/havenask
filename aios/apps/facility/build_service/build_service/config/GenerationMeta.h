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
#include <utility>

#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace config {

class GenerationMeta
{
public:
    GenerationMeta();
    ~GenerationMeta();

public:
    static const std::string FILE_NAME;

public:
    bool loadFromFile(const std::string& rootDir);

    const std::string& getValue(const std::string& key) const
    {
        std::map<std::string, std::string>::const_iterator it = _meta.find(key);
        if (it == _meta.end()) {
            static std::string empty_string;
            return empty_string;
        }
        return it->second;
    }

    bool operator==(const GenerationMeta& other) const;
    bool operator!=(const GenerationMeta& other) const { return !(*this == other); }

private:
    std::map<std::string, std::string> _meta;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(GenerationMeta);

}} // namespace build_service::config
