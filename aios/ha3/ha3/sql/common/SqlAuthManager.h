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

#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace sql {

class AuthenticationConfig;

class SqlAuthItem {
public:
    SqlAuthItem() = default;
    SqlAuthItem(std::string secret);
    ~SqlAuthItem() = default;
public:
    std::string sign(const std::string &queryStr, const std::string &kvPairStr) const;
    bool checkSecret(const std::string &secret) const;
private:
    std::string _secret;
};

class SqlAuthManager
{
public:
    SqlAuthManager();
    ~SqlAuthManager() = default;
    SqlAuthManager(const SqlAuthManager &) = delete;
    SqlAuthManager& operator=(const SqlAuthManager &) = delete;
public:
    bool init(const AuthenticationConfig &config);
    const SqlAuthItem *getAuthItem(const std::string &token) const;
private:
    std::map<std::string, SqlAuthItem> _authMap;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SqlAuthManager> SqlAuthManagerPtr;

} //end namespace sql
} //end namespace isearch
