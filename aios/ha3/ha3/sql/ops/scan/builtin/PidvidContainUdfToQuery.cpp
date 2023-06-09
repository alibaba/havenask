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
#include "ha3/sql/ops/scan/builtin/PidvidContainUdfToQuery.h"

#include <iosfwd>
#include <map>
#include <memory>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/StringUtil.h"
#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"
#include "ha3/isearch.h"
#include "ha3/sql/common/Log.h"
#include "ha3/sql/common/common.h"
#include "ha3/sql/ops/condition/ExprUtil.h"
#include "ha3/sql/ops/condition/SqlJsonUtil.h"
#include "ha3/sql/ops/scan/builtin/ContainToQueryImpl.h"

using namespace std;
using namespace autil;

using namespace isearch::common;

namespace isearch {
namespace sql {
AUTIL_LOG_SETUP(sql, PidvidContainUdfToQuery);

Query *PidvidContainUdfToQuery::toQuery(const SimpleValue &condition,
                                  const Ha3ScanConditionVisitorParam &condParam) {
    if (!ExprUtil::isSameUdf(condition, SQL_UDF_PIDVID_CONTAIN_OP)) {
        SQL_LOG(DEBUG, "not pidvid_contain op");
        return nullptr;
    }
    string op(condition[SQL_CONDITION_OPERATOR].GetString());
    const SimpleValue &param = condition[SQL_CONDITION_PARAMETER];
    if (!param.IsArray() || param.Size() != 2) {
        //valid format: pidvid_contain(pidvid, '1:2|3:4')
        SQL_LOG(DEBUG, "param size %u != 2", param.Size());
        return nullptr;
    }
    if (!SqlJsonUtil::isColumn(param[0])) {
        SQL_LOG(DEBUG, "[%s] not column", string(param[0].GetString()).c_str());
        return nullptr;
    }
    string columnName = SqlJsonUtil::getColumnName(param[0]);
    string queryTerms = param[1].GetString();
    const vector<string> &termVec = StringUtil::split(queryTerms, "|", true);
    return ContainToQueryImpl::toQuery(condParam, columnName, termVec);
}

} // namespace sql
} // namespace isearch
