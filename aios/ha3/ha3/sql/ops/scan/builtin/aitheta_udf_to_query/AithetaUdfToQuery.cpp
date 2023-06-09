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
#include "ha3/sql/ops/scan/builtin/AithetaUdfToQuery.h"

#include <iosfwd>
#include <map>
#include <string>

#include "alog/Logger.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "ha3/common/Term.h"
#include "ha3/sql/common/KvPairParser.h"
#include "ha3/sql/common/Log.h"
#include "ha3/sql/common/common.h"
#include "ha3/sql/ops/condition/ExprUtil.h"
#include "ha3/sql/ops/scan/Ha3ScanConditionVisitorParam.h"
#include "ha3/sql/ops/scan/builtin/AithetaQueryCreator.h"
#include "rapidjson/document.h"

using namespace std;

using namespace isearch::common;

namespace isearch {
namespace sql {
AUTIL_LOG_SETUP(sql, AithetaUdfToQuery);

Query *AithetaUdfToQuery::toQuery(const autil::SimpleValue &condition,
                                  const Ha3ScanConditionVisitorParam &condParam) {
    if (!ExprUtil::isSameUdf(condition, SQL_UDF_AITHETA_OP)) {
        SQL_LOG(ERROR, "not aitheta op");
        return nullptr;
    }
    const auto &param = condition[SQL_CONDITION_PARAMETER];
    if (!param.IsArray() || param.Size() != 3) {
        SQL_LOG(ERROR, "param size %u != 3", param.Size());
        return nullptr;
    }
    map<string, string> kvPairs;
    KvPairParser::parse(param[2].GetString(), SQL_MODEL_KV_PAIR_SPLIT, SQL_MODEL_KV_SPLIT,
                        kvPairs);
    map<string, string> embedding;
    try {
        autil::legacy::FromJsonString(embedding, param[0].GetString());
    } catch (...) {
        SQL_LOG(ERROR, "parse embedding failed [%s].", string(param[0].GetString()).c_str());
        return nullptr;
    }
    Query *query = AithetaQueryCreator::createQuery(
        param[1].GetString(), kvPairs, embedding, condParam.modelConfigMap);
    if (query == nullptr) {
        SQL_LOG(ERROR, "get aitheta query failed");
        return nullptr;
    }
    return query;
}

} // namespace sql
} // namespace isearch
