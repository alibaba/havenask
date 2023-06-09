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
#include "ha3/sql/common/SqlAuthManager.h"

#include "autil/legacy/md5.h"
#include "ha3/sql/config/AuthenticationConfig.h"

using namespace std;
using namespace autil::legacy;

namespace isearch {
namespace sql {
AUTIL_LOG_SETUP(sql, SqlAuthManager);

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

SqlAuthManager::SqlAuthManager() {
}

bool SqlAuthManager::init(const AuthenticationConfig &config) {
    for (const auto &pair : config.tokenPairs) {
        auto &token = pair.first;
        auto &secret = pair.second;
        if (token.empty()) {
            AUTIL_LOG(ERROR, "sql authentication manager init failed, config error: token empty");
            return false;
        }
        if (secret.empty()) {
            AUTIL_LOG(
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

const SqlAuthItem *SqlAuthManager::getAuthItem(const std::string &token) const {
    auto iter = _authMap.find(token);
    if (iter == _authMap.end()) {
        return nullptr;
    }
    return &iter->second;
}

} //end namespace sql
} //end namespace isearch
