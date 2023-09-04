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
#include "ha3/queryparser/ParserContext.h"

#include <cstddef>

#include "autil/CommonMacros.h"
#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "ha3/common/Query.h"          // IWYU pragma: keep
#include "ha3/queryparser/QueryExpr.h" // IWYU pragma: keep

using namespace std;
using namespace autil;

namespace isearch {
namespace queryparser {
AUTIL_LOG_SETUP(ha3, ParserContext);

const char *ParserContext::STATUS_MSGS[] = {
    "OK.",
    "Initial status.",
    "Bad index name.",
    "Bad fieldbit name.",
    "Evaluate failed.",
};

ParserContext::ParserContext(QueryParser &queryParser)
    : _queryParser(queryParser) {
    _status = ParserContext::INITIAL_STATUS;
}

ParserContext::~ParserContext() {
    for (size_t i = 0; i < _rootQuerys.size(); ++i) {
        DELETE_AND_SET_NULL(_rootQuerys[i]);
    }
    _rootQuerys.clear();
    for (size_t i = 0; i < _rootQueryExprs.size(); ++i) {
        DELETE_AND_SET_NULL(_rootQueryExprs[i]);
    }
    _rootQueryExprs.clear();
}

const std::string ParserContext::getStatusMsg() const {
    if (_status < ParserContext::SYNTAX_ERROR) {
        return STATUS_MSGS[_status];
    }
    return _statusMsg;
}

void ParserContext::addQuery(common::Query *query) {
    _rootQuerys.push_back(query);
}

vector<common::Query *> ParserContext::stealQuerys() {
    std::vector<common::Query *> rootQuerys;
    rootQuerys.swap(_rootQuerys);
    return rootQuerys;
}

void ParserContext::addQueryExpr(QueryExpr *queryExpr) {
    _rootQueryExprs.push_back(queryExpr);
}

vector<QueryExpr *> ParserContext::stealQueryExprs() {
    vector<QueryExpr *> rootQueryExprs;
    rootQueryExprs.swap(_rootQueryExprs);
    return rootQueryExprs;
}

} // namespace queryparser
} // namespace isearch
