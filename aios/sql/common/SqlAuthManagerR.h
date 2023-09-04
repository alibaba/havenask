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
#include <memory>
#include <string>

#include "navi/common.h"
#include "navi/engine/Resource.h"
#include "navi/engine/ResourceConfigContext.h"
#include "sql/config/AuthenticationConfig.h"

namespace navi {
class ResourceDefBuilder;
class ResourceInitContext;
} // namespace navi

namespace sql {

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

class SqlAuthManagerR : public navi::Resource {
public:
    SqlAuthManagerR();
    ~SqlAuthManagerR() = default;
    SqlAuthManagerR(const SqlAuthManagerR &) = delete;
    SqlAuthManagerR &operator=(const SqlAuthManagerR &) = delete;

public:
    bool init(const AuthenticationConfig &config);
    void def(navi::ResourceDefBuilder &builder) const override;
    bool config(navi::ResourceConfigContext &ctx) override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;

public:
    bool check(const std::string &sqlStr,
               const std::string &kvPairStr,
               const std::string &token,
               const std::string &signature) const;

private:
    const SqlAuthItem *getAuthItem(const std::string &token) const;

private:
    AuthenticationConfig _authenticationConfig;
    std::map<std::string, SqlAuthItem> _authMap;

public:
    static const std::string RESOURCE_ID;
};

typedef std::shared_ptr<SqlAuthManagerR> SqlAuthManagerRPtr;

} // namespace sql
