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
#include "ha3/sql/ops/tvf/InvocationAttr.h"
#include "ha3/sql/common/Log.h"

using namespace std;

namespace isearch {
namespace sql {
AUTIL_LOG_SETUP(sql, InvocationAttr);
static const string TVF_TABLE_PARAM_PREFIX = "@table#";

void InvocationAttr::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("op", tvfName, tvfName);
    json.Jsonize("type", type, type);
    json.JsonizeAsString("params", tvfParams, "");
}

bool InvocationAttr::parseStrParams(vector<string> &paramVec, vector<string> &inputTables) const {
    try {
        autil::legacy::json::JsonArray paramsArray;
        FromJsonString(paramsArray, tvfParams);
        for (auto &paramAny : paramsArray) {
            if (autil::legacy::json::IsJsonString(paramAny)) {
                string param = *autil::legacy::AnyCast<std::string>(&paramAny);
                if (param.find(TVF_TABLE_PARAM_PREFIX) == 0) {
                    inputTables.push_back(param.substr(TVF_TABLE_PARAM_PREFIX.size()));
                } else {
                    paramVec.push_back(param);
                }
            } else if (autil::legacy::json::IsJsonMap(paramAny)) {
                continue;
            } else {
                return false;
            }
        }
    } catch (const autil::legacy::ExceptionBase &e) {
        SQL_LOG(ERROR, "parse tvf param fail. [%s].", e.what());
        return false;
    } catch (...) { return false; }
    return true;
}

}  // namespace sql
}  // namespace isearch
