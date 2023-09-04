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
#include "suez/turing/expression/framework/AttributeExpressionPool.h"

#include <algorithm>
#include <assert.h>
#include <cstddef>
#include <set>

#include "autil/mem_pool/PoolBase.h"
#include "suez/turing/expression/framework/AttributeExpression.h"

using namespace std;

namespace suez {
namespace turing {

AUTIL_LOG_SETUP(expression, AttributeExpressionPool);

AttributeExpressionPool::AttributeExpressionPool() {}

AttributeExpressionPool::~AttributeExpressionPool() {
    assert(assertNoDupExpression());
    for (size_t i = 0; i < _exprVecs.size(); ++i) {
        POOL_DELETE_CLASS(_exprVecs[i]);
    }
    _exprVecs.clear();
}

void AttributeExpressionPool::addPair(const std::string &exprStr, AttributeExpression *attrExpr, bool needDelete) {
    if (attrExpr == nullptr) {
        return;
    }
    if (!attrExpr->isDeterministic()) {
        AUTIL_LOG(DEBUG,
                  "expression[%s] is not deterministic,"
                  "not optimize this expression",
                  exprStr.c_str());
    } else {
        _exprMap[exprStr] = attrExpr;
    }
    if (needDelete) {
        addNeedDeleteExpr(attrExpr);
    }
}

void AttributeExpressionPool::addNeedDeleteExpr(AttributeExpression *attrExpr) {
    if (attrExpr == nullptr) {
        return;
    }
    _exprVecs.push_back(attrExpr);
}

bool AttributeExpressionPool::assertNoDupExpression() {
    set<AttributeExpression *> exprsSet;
    exprsSet.insert(_exprVecs.begin(), _exprVecs.end());
    return exprsSet.size() == _exprVecs.size();
}

} // namespace turing
} // namespace suez
