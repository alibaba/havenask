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
#include "swift/config/AuthorizerParser.h"

#include <iosfwd>
#include <utility>

#include "swift/config/AuthorizerInfo.h"
#include "swift/config/ConfigDefine.h"
#include "swift/config/ConfigReader.h"

using namespace std;

namespace swift {
namespace config {
AUTIL_LOG_SETUP(swift, AuthorizerParser);

AuthorizerParser::AuthorizerParser() {}

AuthorizerParser::~AuthorizerParser() {}

bool AuthorizerParser::parseAuthorizer(ConfigReader &reader, AuthorizerInfo &conf) {

    bool enable = false;
    if (!reader.getOption(SECTION_COMMON, ENABLE_AUTHENTICATION, enable)) {
        enable = false;
    }
    string innerSysUser;
    reader.getOption(SECTION_COMMON, INNER_SYS_USER, innerSysUser);

    map<string, string> sysUsers;
    map<string, string> normalUsers;
    parseUsers(reader, SYS_USER_PREFIX, sysUsers);
    parseUsers(reader, NORMAL_USER_PREFIX, normalUsers);
    if (!validate(enable, sysUsers, normalUsers)) {
        AUTIL_LOG(ERROR, "no internal authentication configured");
        return false;
    }
    if (enable) {
        AUTIL_LOG(INFO, "Enable authentication");
    } else {
        AUTIL_LOG(INFO, "Disable authentication");
    }
    conf.setEnable(enable);
    conf.setSysUsers(sysUsers);
    conf.setNormalUsers(normalUsers);
    conf.setInnerSysUser(innerSysUser);
    return true;
}

void AuthorizerParser::parseUsers(ConfigReader &reader, const string &prefix, map<string, string> &userMap) {
    const map<string, string> &content = reader.getKVByPrefix(SECTION_COMMON, prefix);
    for (const auto &iterator : content) {
        userMap[iterator.first.substr(prefix.size())] = iterator.second;
    }
}

bool AuthorizerParser::validate(bool enable,
                                const map<string, string> &sysUsers,
                                const map<string, string> &normalUsers) {
    if (enable && sysUsers.empty()) {
        AUTIL_LOG(ERROR, "lack of sysUsers when authentication is enabled");
        return false;
    }
    if (!sysUsers.empty() && !normalUsers.empty()) {
        for (const auto &user : sysUsers) {
            auto iter = normalUsers.find(user.first);
            if (iter != normalUsers.end()) {
                AUTIL_LOG(ERROR, "duplicated user exists in normalUsers and sysUsers[%s]", user.first.c_str());
                return false;
            }
        }
    }
    return true;
}

} // namespace config
} // end namespace swift
