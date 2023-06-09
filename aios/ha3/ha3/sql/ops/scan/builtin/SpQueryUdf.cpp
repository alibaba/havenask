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
#include "ha3/sql/ops/scan/builtin/SpQueryUdf.h"

#include <sstream>
#include <string>

#include "alog/Logger.h"
#include "ha3/common/Term.h"
#include "ha3/isearch.h"
#include "ha3/queryparser/ParserContext.h"
#include "ha3/queryparser/QueryParser.h"
#include "ha3/sql/ops/scan/builtin/SpQueryParser.hh"
#include "ha3/sql/ops/scan/builtin/SpQueryScanner.h"

using namespace std;
using namespace isearch::common;
using namespace isearch::queryparser;

namespace isearch {
namespace sql {

AUTIL_LOG_SETUP(sql, SPQueryUdf);

SPQueryUdf::SPQueryUdf(const char *defaultIndex)
    : _queryParser(defaultIndex, OP_AND, false) {}

SPQueryUdf::~SPQueryUdf() {}

ParserContext *SPQueryUdf::parse(const char *queryText) {
    istringstream iss(queryText);
    ostringstream oss;
    SPQueryScanner scanner(&iss, &oss);
    ParserContext *ctx = new ParserContext(_queryParser);
    AUTIL_LOG(TRACE1, "context %p", ctx);
    isearch_sp::SPQueryParser parser(scanner, *ctx);
    if (parser.parse() != 0) {
        AUTIL_LOG(DEBUG, "%s:|%s|", ctx->getStatusMsg().c_str(), queryText);
        if (oss.str().length() > 0) {
            AUTIL_LOG(WARN, "failed to resolve  %s", oss.str().c_str());
        }
    }
    _queryParser.evaluateQuery(ctx);
    return ctx;
}

} // namespace sql
} // namespace isearch
