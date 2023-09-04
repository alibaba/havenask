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
#pragma once

#include <assert.h>
#include <stddef.h>
#include <string>

#include "autil/Log.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/PoolBase.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/AttributeExpression.h"
#include "suez/turing/expression/framework/AttributeExpressionCreatorBase.h"
#include "suez/turing/expression/framework/AttributeExpressionPool.h"

namespace matchdoc {
class MatchDocAllocator;
template <typename T>
class Reference;
} // namespace matchdoc

namespace suez {
namespace turing {
class SyntaxExpr;

class SimpleAttributeExpressionCreator : public AttributeExpressionCreatorBase {
public:
    SimpleAttributeExpressionCreator(autil::mem_pool::Pool *pool, matchdoc::MatchDocAllocator *matchDocAllocator);
    ~SimpleAttributeExpressionCreator();

private:
    SimpleAttributeExpressionCreator(const SimpleAttributeExpressionCreator &);
    SimpleAttributeExpressionCreator &operator=(const SimpleAttributeExpressionCreator &);

public:
    AttributeExpression *createAtomicExpr(const std::string &attrName) override { return NULL; }

    AttributeExpression *createAttributeExpression(const SyntaxExpr *syntaxExpr, bool needValidate = false) override;

    AttributeExpression *createAttributeExpression(const std::string &exprString, VariableType vt_type, bool isMulti);

private:
    template <typename T>
    AttributeExpression *createAttributeExpressionTyped(matchdoc::Reference<T> *ref, const std::string &exprString) {
        assert(ref);
        AttributeExpression *attrExpr = _exprPool->tryGetAttributeExpression(exprString);
        if (attrExpr) {
            return dynamic_cast<AttributeExpressionTyped<T> *>(attrExpr);
        }
        auto expr = POOL_NEW_CLASS(_pool, AttributeExpressionTyped<T>);
        expr->setReference(ref);
        expr->setIsSubExpression(ref->isSubDocReference());
        _exprPool->addPair(exprString, expr, true);
        return expr;
    }

private:
    matchdoc::MatchDocAllocator *_allocator;

private:
    AUTIL_LOG_DECLARE();
};

SUEZ_TYPEDEF_PTR(SimpleAttributeExpressionCreator);

} // namespace turing
} // namespace suez
