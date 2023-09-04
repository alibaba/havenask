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

#include "autil/Log.h"
#include "swift/common/Common.h"

namespace swift {
namespace config {
class AuthorizerInfo;
class ConfigReader;

class AuthorizerParser {
public:
    AuthorizerParser();
    ~AuthorizerParser();

private:
    AuthorizerParser(const AuthorizerParser &);
    AuthorizerParser &operator=(const AuthorizerParser &);

public:
    bool parseAuthorizer(config::ConfigReader &reader, AuthorizerInfo &conf);

private:
    void parseUsers(ConfigReader &reader, const std::string &prefix, std::map<std::string, std::string> &userMap);
    bool validate(bool enable,
                  const std::map<std::string, std::string> &sysUsers,
                  const std::map<std::string, std::string> &normalUsers);

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(AuthorizerParser);

} // namespace config
} // end namespace swift
