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
#include "expression/framework/AttributeExpressionPool.h"
#include "expression/framework/AttributeExpression.h"

using namespace std;

#define DEFAULT_EXPRESSION_VEC_INIT_SIZE 128

namespace expression {
AUTIL_LOG_SETUP(expression, AttributeExpressionPool);

AttributeExpressionPool::AttributeExpressionPool() {
    _exprVec.reserve(DEFAULT_EXPRESSION_VEC_INIT_SIZE);
}

AttributeExpressionPool::~AttributeExpressionPool() { 
    for (size_t i = 0; i < _exprVec.size(); i++)
    {
        POOL_DELETE_CLASS(_exprVec[i]);
    }
    _exprVec.clear();
}

void AttributeExpressionPool::addNeedDeleteExpr(AttributeExpression *attrExpr) {
    if (_exprSet.find(attrExpr) == _exprSet.end()) {
        _exprSet.insert(attrExpr);
        _exprVec.push_back(attrExpr);
    } else {
        AUTIL_LOG(DEBUG, "expression[%s] has already exist",
                  attrExpr->getExprString().c_str());
    }
}

}
