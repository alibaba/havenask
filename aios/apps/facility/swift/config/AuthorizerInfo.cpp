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
#include "swift/config/AuthorizerInfo.h"

#include <utility>

namespace swift {
namespace config {
AUTIL_LOG_SETUP(swift, AuthorizerInfo);

AuthorizerInfo::AuthorizerInfo() {}

AuthorizerInfo::~AuthorizerInfo() {}

void AuthorizerInfo::setEnable(bool enable) { _enable = enable; }

bool AuthorizerInfo::getEnable() const { return _enable; }

void AuthorizerInfo::setSysUsers(const AuthenticationMap &userMap) { _sysUsers = userMap; }
const AuthorizerInfo::AuthenticationMap &AuthorizerInfo::getSysUsers() const { return _sysUsers; }

void AuthorizerInfo::setNormalUsers(const AuthenticationMap &userMap) { _normalUsers = userMap; }
const AuthorizerInfo::AuthenticationMap &AuthorizerInfo::getNormalUsers() const { return _normalUsers; }

void AuthorizerInfo::setInnerSysUser(const std::string &username) { _innerSysUser = username; }

void AuthorizerInfo::getInnerSysUserPasswd(std::string &username, std::string &passwd) {
    if (_innerSysUser.empty()) {
        if (!_sysUsers.empty()) {
            username = _sysUsers.begin()->first;
            passwd = _sysUsers.begin()->second;
        }
    } else {
        auto iter = _sysUsers.find(_innerSysUser);
        if (iter != _sysUsers.end()) {
            username = iter->first;
            passwd = iter->second;
        } else {
            username = _innerSysUser;
        }
    }
}

} // namespace config
} // end namespace swift
