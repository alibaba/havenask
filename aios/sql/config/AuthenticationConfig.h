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

#include "autil/Log.h" // IWYU pragma: keep
#include "autil/legacy/jsonizable.h"

namespace sql {

class AuthenticationConfig : public autil::legacy::Jsonizable {
public:
    AuthenticationConfig() = default;
    ~AuthenticationConfig() = default;

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
        json.Jsonize("enable", enable, enable);
        json.Jsonize("token_pairs", tokenPairs, tokenPairs);
    }

public:
    bool enable = false;
    std::map<std::string, std::string> tokenPairs;
};

} // namespace sql
