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
#include "ha3/search/Filter.h"

#include <stddef.h>
#include <string>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/PoolBase.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/AttributeExpression.h"
#include "suez/turing/expression/framework/AttributeExpressionCreatorBase.h"
#include "suez/turing/expression/syntax/SyntaxExpr.h"

using namespace suez::turing;

namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, Filter);

Filter::Filter(AttributeExpressionTyped<bool> *attributeExpr)
    : _attributeExpr(attributeExpr) {}

Filter::~Filter() {
    _attributeExpr = NULL;
}

void Filter::setAttributeExpression(AttributeExpressionTyped<bool> *attributeExpr) {
    _attributeExpr = attributeExpr;
}

Filter *Filter::createFilter(const suez::turing::SyntaxExpr *filterExpr,
                             AttributeExpressionCreatorBase *attributeExpressionCreator,
                             autil::mem_pool::Pool *pool) {
    if (filterExpr == nullptr) {
        return nullptr;
    }
    AttributeExpression *attrExpr
        = attributeExpressionCreator->createAttributeExpression(filterExpr);
    if (!attrExpr) {
        AUTIL_LOG(WARN, "create filter expression[%s] fail.", filterExpr->getExprString().c_str());
        return nullptr;
    }
    AttributeExpressionTyped<bool> *boolAttrExpr
        = dynamic_cast<AttributeExpressionTyped<bool> *>(attrExpr);
    if (!boolAttrExpr) {
        AUTIL_LOG(WARN,
                  "filter expression[%s] return type should be bool.",
                  filterExpr->getExprString().c_str());
        return nullptr;
    }
    return POOL_NEW_CLASS(pool, Filter, boolAttrExpr);
}

void Filter::updateExprEvaluatedStatus() {
    _attributeExpr->setEvaluated();
}

} // namespace search
} // namespace isearch
