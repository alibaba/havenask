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
#include "ha3/sql/ops/scan/builtin/KvExtractMatchUdfToQuery.h"

#include <stdint.h>
#include <iosfwd>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/StringUtil.h"
#include "ha3/isearch.h"
#include "ha3/sql/common/Log.h"
#include "ha3/sql/common/common.h"
#include "ha3/sql/ops/condition/ExprUtil.h"
#include "ha3/sql/ops/condition/SqlJsonUtil.h"
#include "rapidjson/document.h"
#include "ha3/sql/ops/scan/builtin/ContainToQueryImpl.h"

using namespace std;
using namespace autil;

using namespace isearch::common;

namespace isearch {
namespace sql {
AUTIL_LOG_SETUP(sql, KvExtractMatchUdfToQuery);

Query *KvExtractMatchUdfToQuery::toQuery(const SimpleValue &condition,
        const Ha3ScanConditionVisitorParam &condParam) {
    if (!ExprUtil::isSameUdf(condition, SQL_UDF_KV_EXTRACT_MATCH_OP)) {
        SQL_LOG(DEBUG, "not %s op", SQL_UDF_KV_EXTRACT_MATCH_OP);
        return nullptr;
    }
    string op(condition[SQL_CONDITION_OPERATOR].GetString());
    const SimpleValue &param = condition[SQL_CONDITION_PARAMETER];
    if (!param.IsArray() || param.Size() < 3 || param.Size() > 4) {
        SQL_LOG(DEBUG, "param size %u != 3 or 4", param.Size());
        return nullptr;
    }
    if (!SqlJsonUtil::isColumn(param[0])) {
        SQL_LOG(DEBUG, "[%s] not column", string(param[0].GetString()).c_str());
        return nullptr;
    }
    string attributesName = SqlJsonUtil::getColumnName(param[0]);
    string sepStr = param[1].GetString();

    std::string sep("|");
    if (sepStr.size() >= 1) {
        sep = sepStr[0];
    }

    string keyStr = param[2].GetString();
    string valueStr("");
    bool keyMatch = param.Size() == 3;

    if (keyMatch) {
        std::string columnName = attributesName + "_keys";
        vector<string> termVec({keyStr});
        return ContainToQueryImpl::toQuery(condParam, columnName, termVec, true);
    } else {
        string valueStr = param[3].GetString();
        std::string columnName = attributesName + "_" + keyStr;
        vector<std::string> termVec = StringUtil::split(valueStr, sep, true);
        return ContainToQueryImpl::toQuery(condParam, columnName, termVec, true);
    }
    return nullptr;
}

} // namespace sql
} // namespace isearch
