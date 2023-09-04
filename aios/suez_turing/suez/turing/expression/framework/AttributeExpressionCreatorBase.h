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

#include <set>
#include <string>
#include <vector>

#include "autil/CommonMacros.h"
#include "autil/Log.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/PoolBase.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/AttributeExpression.h"
#include "suez/turing/expression/framework/AttributeExpressionPool.h"
#include "suez/turing/expression/framework/ComboAttributeExpression.h"

namespace suez {
namespace turing {
class SyntaxExpr;

class AttributeExpressionCreatorBase {
public:
    AttributeExpressionCreatorBase(autil::mem_pool::Pool *pool) : _pool(pool), _exprPool(new AttributeExpressionPool) {}
    virtual ~AttributeExpressionCreatorBase();

private:
    AttributeExpressionCreatorBase(const AttributeExpressionCreatorBase &);
    AttributeExpressionCreatorBase &operator=(const AttributeExpressionCreatorBase &);

public:
    virtual AttributeExpression *createAtomicExpr(const std::string &attrName) = 0;

    virtual SyntaxExpr *parseSyntax(const std::string &exprStr);

    AttributeExpression *createAttributeExpression(const std::string &exprStr);

    virtual AttributeExpression *createAttributeExpression(const SyntaxExpr *syntaxExpr, bool needValidate = false) = 0;

    ComboAttributeExpression *createComboAttributeExpression(const std::vector<AttributeExpression *> &attrExprs) {
        ComboAttributeExpression *comboExpr = POOL_NEW_CLASS(_pool, ComboAttributeExpression);
        comboExpr->setExpressionVector(attrExprs);
        _exprPool->addNeedDeleteExpr(comboExpr);
        return comboExpr;
    }

    ComboAttributeExpression *createComboAttributeExpression(const std::set<AttributeExpression *> &attrExprs) {
        ComboAttributeExpression *comboExpr = POOL_NEW_CLASS(_pool, ComboAttributeExpression);
        ExpressionVector tmpAttrVec(attrExprs.begin(), attrExprs.end());
        comboExpr->setExpressionVector(tmpAttrVec);
        _exprPool->addNeedDeleteExpr(comboExpr);
        return comboExpr;
    }

    void addNeedDeleteExpr(AttributeExpression *expr) { _exprPool->addNeedDeleteExpr(expr); }

    void addNeedOptimizeReCalcSyntaxExpr(SyntaxExpr *syntaxExpr) { _syntaxExprVec.emplace_back(syntaxExpr); }

    AttributeExpression *createOptimizeReCalcExpression();

    autil::mem_pool::Pool *getPool() { return _pool; }

    void swapCache(AttributeExpressionPool *&exprPool) { std::swap(exprPool, _exprPool); }

protected:
    autil::mem_pool::Pool *_pool;
    AttributeExpressionPool *_exprPool;
    std::vector<SyntaxExpr *> _syntaxExprVec;

private:
    AUTIL_LOG_DECLARE();
};

SUEZ_TYPEDEF_PTR(AttributeExpressionCreatorBase);

} // namespace turing
} // namespace suez
