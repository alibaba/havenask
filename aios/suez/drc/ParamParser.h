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

#include "autil/StringUtil.h"

namespace suez {

class ParamParser {
public:
    static std::string getParameter(const std::map<std::string, std::string> &params, const std::string &key) {
        auto it = params.find(key);
        if (it != params.end()) {
            return it->second;
        } else {
            return "";
        }
    }

    template <typename T>
    static bool getParameterT(const std::map<std::string, std::string> &params, const std::string &key, T &val) {
        auto it = params.find(key);
        if (it == params.end()) {
            return false;
        }
        return autil::StringUtil::fromString<T>(it->second, val);
    }
};

} // namespace suez
