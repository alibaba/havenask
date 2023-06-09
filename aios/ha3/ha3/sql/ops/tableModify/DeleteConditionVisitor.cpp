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
#include "ha3/sql/ops/tableModify/DeleteConditionVisitor.h"

#include "autil/StringUtil.h"

using namespace std;
using namespace autil;

namespace isearch {
namespace sql {
AUTIL_LOG_SETUP(sql, DeleteConditionVisitor);

DeleteConditionVisitor::DeleteConditionVisitor(const std::set<std::string> &fetchFields)
    : UpdateConditionVisitor(fetchFields)
{
}

bool DeleteConditionVisitor::checkFetchValues() {
    std::map<std::string, std::string> fetchFields;
    std::transform(_fetchFields.begin(), _fetchFields.end(),
                   std::inserter(fetchFields, begin(fetchFields)),
                   [] (const std::string &arg) { return std::make_pair(arg, "");});
    for (size_t i = 0; i < _values.size(); ++i) {
        if (!isIncludeFields(_values[i], fetchFields)) {
            SQL_LOG(ERROR, "DELETE: fetch values [%s] not include expected [%s] set",
                    StringUtil::toString(_values).c_str(),
                    StringUtil::toString(_fetchFields).c_str());
            return false;
        }
    }
    return true;
}

} // namespace sql
} // namespace isearch
