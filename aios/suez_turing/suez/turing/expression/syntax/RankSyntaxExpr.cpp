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
#include "suez/turing/expression/syntax/RankSyntaxExpr.h"

#include <stddef.h>
#include <string>

#include "suez/turing/expression/common.h"
#include "suez/turing/expression/syntax/SyntaxExprVisitor.h"

namespace suez {
namespace turing {

AUTIL_LOG_SETUP(expression, RankSyntaxExpr);
static const std::string RANK_SYNTAX_NAME = "RANK";

RankSyntaxExpr::RankSyntaxExpr() {
    setSyntaxExprType(SYNTAX_EXPR_TYPE_RANK);
    setExprResultType(vt_double);
    setExprString(RANK_SYNTAX_NAME);
}

RankSyntaxExpr::~RankSyntaxExpr() {}

void RankSyntaxExpr::accept(SyntaxExprVisitor *visitor) const { visitor->visitRankSyntaxExpr(this); }

bool RankSyntaxExpr::operator==(const SyntaxExpr *expr) const {
    const RankSyntaxExpr *checkExpr = dynamic_cast<const RankSyntaxExpr *>(expr);
    return checkExpr != NULL;
}

} // namespace turing
} // namespace suez
