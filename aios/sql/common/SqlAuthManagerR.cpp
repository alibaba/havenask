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
#include "sql/common/SqlAuthManagerR.h"

#include <iosfwd>
#include <stdint.h>
#include <utility>

#include "autil/legacy/exception.h"
#include "autil/legacy/md5.h"
#include "navi/builder/ResourceDefBuilder.h"
#include "navi/engine/Resource.h"
#include "navi/proto/KernelDef.pb.h"
#include "sql/common/Log.h"
#include "sql/config/AuthenticationConfig.h"

namespace navi {
class ResourceInitContext;
} // namespace navi

using namespace std;
using namespace autil::legacy;

namespace sql {

SqlAuthItem::SqlAuthItem(std::string secret)
    : _secret(secret) {}

std::string SqlAuthItem::sign(const std::string &queryStr, const std::string &kvPairStr) const {
    Md5Stream stream;
    stream.Put((const uint8_t *)queryStr.c_str(), queryStr.length());
    stream.Put((const uint8_t *)kvPairStr.c_str(), kvPairStr.length());
    stream.Put((const uint8_t *)_secret.c_str(), _secret.length());
    return stream.GetMd5String();
}

bool SqlAuthItem::checkSecret(const std::string &secret) const {
    return _secret == secret;
}

const std::string SqlAuthManagerR::RESOURCE_ID = "sql.auth_manager_r";

SqlAuthManagerR::SqlAuthManagerR() {}

void SqlAuthManagerR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID, navi::RS_BIZ);
}

bool SqlAuthManagerR::config(navi::ResourceConfigContext &ctx) {
    ctx.Jsonize("authentication_config", _authenticationConfig, _authenticationConfig);
    return true;
}

navi::ErrorCode SqlAuthManagerR::init(navi::ResourceInitContext &ctx) {
    if (!_authenticationConfig.enable) {
        SQL_LOG(INFO, "sql auth manager disabled");
        return navi::EC_NONE;
    }
    if (!init(_authenticationConfig)) {
        SQL_LOG(ERROR, "sql auth manager init failed");
        return navi::EC_INIT_RESOURCE;
    }
    return navi::EC_NONE;
}

bool SqlAuthManagerR::init(const AuthenticationConfig &config) {
    for (const auto &pair : config.tokenPairs) {
        auto &token = pair.first;
        auto &secret = pair.second;
        if (token.empty()) {
            SQL_LOG(ERROR, "sql authentication manager init failed, config error: token empty");
            return false;
        }
        if (secret.empty()) {
            SQL_LOG(
                ERROR,
                "sql authentication manager init failed, config error: secret empty, token [%s]",
                token.c_str());
            return false;
        }
        SqlAuthItem item(secret);
        _authMap.emplace(token, std::move(item));
    }
    return true;
}

const SqlAuthItem *SqlAuthManagerR::getAuthItem(const std::string &token) const {
    auto iter = _authMap.find(token);
    if (iter == _authMap.end()) {
        return nullptr;
    }
    return &iter->second;
}

bool SqlAuthManagerR::check(const std::string &sqlStr,
                            const std::string &kvPairStr,
                            const std::string &token,
                            const std::string &signature) const {
    if (!_authenticationConfig.enable) {
        return true;
    }
    auto *item = getAuthItem(token);
    if (!item) {
        SQL_LOG(ERROR, "token[%s] not found", token.c_str());
        return false;
    }
    std::string sign = item->sign(sqlStr, kvPairStr);
    if (sign == signature || item->checkSecret(signature)) {
        // TODO: remove backdoor
        return true;
    } else {
        SQL_LOG(ERROR,
                "signature not match, token[%s] expect[%s] actual[%s]",
                token.c_str(),
                sign.c_str(),
                signature.c_str());
        return false;
    }
}

REGISTER_RESOURCE(SqlAuthManagerR);

} // namespace sql
