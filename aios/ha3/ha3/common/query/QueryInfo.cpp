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
#include "ha3/common/QueryInfo.h"

#include <iosfwd>

#include "autil/legacy/jsonizable.h"

#include "ha3/isearch.h"
#include "autil/Log.h"

using namespace std;
namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, QueryInfo);

QueryInfo::QueryInfo() {
    _defaultIndexName = "default";
    _defaultOP = OP_AND;
    _useMultiTermOptimize = false;
}

QueryInfo::QueryInfo(const string &defaultIndexName, QueryOperator defaultOP,
                     bool flag)
    : _defaultIndexName(defaultIndexName)
    , _defaultOP(defaultOP)
    , _useMultiTermOptimize(flag)
{
}

QueryInfo::~QueryInfo() {
}

void QueryInfo::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
    string opString;
    if (TO_JSON == json.GetMode()) {
        if (_defaultOP == OP_AND) {
            opString = "AND";
        } else if (_defaultOP == OP_OR) {
            opString = "OR";
        }
    }

    json.Jsonize("default_index", _defaultIndexName, _defaultIndexName);
    json.Jsonize("default_operator", opString, string("AND"));

    if (FROM_JSON == json.GetMode()) {
        if (opString == "AND") {
            _defaultOP = OP_AND;
        } else if (opString == "OR") {
            _defaultOP = OP_OR;
        }
    }
    json.Jsonize("multi_term_optimize", _useMultiTermOptimize, _useMultiTermOptimize);
}

} // namespace common
} // namespace isearch
