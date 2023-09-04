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

class AuthorizerInfo {
    typedef std::map<std::string, std::string> AuthenticationMap;

public:
    AuthorizerInfo();
    ~AuthorizerInfo();

private:
    AuthorizerInfo &operator=(const AuthorizerInfo &);

public:
    void setEnable(bool enable);
    bool getEnable() const;

    void setSysUsers(const AuthenticationMap &userMap);
    const AuthenticationMap &getSysUsers() const;

    void setNormalUsers(const AuthenticationMap &userMap);
    const AuthenticationMap &getNormalUsers() const;

    void setInnerSysUser(const std::string &username);
    void getInnerSysUserPasswd(std::string &username, std::string &passwd);

private:
    bool _enable = false;
    AuthenticationMap _sysUsers;
    AuthenticationMap _normalUsers;
    std::string _innerSysUser;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(AuthorizerInfo);

} // namespace config
} // end namespace swift
