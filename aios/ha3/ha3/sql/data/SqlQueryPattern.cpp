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
#include "ha3/sql/data/SqlQueryPattern.h"

namespace isearch {
namespace sql {

SqlQueryPattern::SqlQueryPattern()
    : queryHashKey(0)
{
}

void SqlQueryPattern::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("query_hash_key", queryHashKey, queryHashKey);
    json.Jsonize("sql_str", sqlStr, sqlStr);
    json.Jsonize("kvpair", _kvPair, _kvPair);
}

std::string SqlQueryPattern::toCompactJson() const {
    std::string jsonStr;
    try {
        jsonStr = autil::legacy::ToJsonString(*this, true);
    } catch (const autil::legacy::ExceptionBase &e) {
    }
    return jsonStr;
}

} // namespace sql
} // namespace isearch