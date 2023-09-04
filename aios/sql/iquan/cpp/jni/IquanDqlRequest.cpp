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
#include "iquan/jni/IquanDqlRequest.h"

#include "iquan/common/Utils.h"

namespace iquan {

IquanDqlRequest::IquanDqlRequest()
    : queryHashKey(0) {
    defaultConfig();
}

void IquanDqlRequest::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("sqls", sqls);
    json.Jsonize("sql_params", sqlParams);
    json.Jsonize("default_catalog_name", defaultCatalogName);
    json.Jsonize("default_database_name", defaultDatabaseName);
    if (_enableDynamicParams) {
        json.Jsonize("dynamic_params", dynamicParams._array, dynamicParams._array);
    }
    if (_enableQueryHashKey) {
        json.Jsonize("query_hash_key", queryHashKey, queryHashKey);
    }
}

bool IquanDqlRequest::isValid() const {
    size_t dynamicParamSize = dynamicParams._array.size();
    if (defaultCatalogName.empty() || defaultDatabaseName.empty() || sqls.empty()
        || (dynamicParamSize > 0 && (sqls.size() != dynamicParamSize))) {
        return false;
    }
    return true;
}

Status IquanDqlRequest::toCacheKey(std::string &jsonStr) {
    cacheKeyConfig();
    auto status = Utils::fastToJson(*this, jsonStr, true);
    defaultConfig();
    return status;
}

Status IquanDqlRequest::toWarmupJson(std::string &jsonStr) {
    warmupJsonConfig();
    auto status = Utils::toJson(*this, jsonStr, true);
    defaultConfig();
    return status;
}

Status IquanDqlRequest::fromWarmupJson(const std::string &jsonStr) {
    warmupJsonConfig();
    auto status = Utils::fromJson(*this, jsonStr);
    defaultConfig();
    return status;
}

void IquanDqlRequest::defaultConfig() {
    _enableDynamicParams = true;
    _enableQueryHashKey = false;
}

void IquanDqlRequest::cacheKeyConfig() {
    _enableDynamicParams = false;
    _enableQueryHashKey = false;
}

void IquanDqlRequest::warmupJsonConfig() {
    _enableDynamicParams = false;
    _enableQueryHashKey = true;
}

} // namespace iquan
