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
#include "ha3/rank/ComparatorCreator.h"

#include <assert.h>
#include <stdint.h>
#include <algorithm>
#include <cstddef>
#include <map>
#include <new>

#include "autil/Log.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/PoolBase.h"
#include "ha3/rank/ComboComparator.h"
#include "ha3/rank/ExpressionComparator.h"
#include "ha3/rank/ReferenceComparator.h"
#include "ha3/search/SortExpression.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/AttributeExpression.h"
#include "suez/turing/expression/framework/VariableTypeTraits.h"

namespace isearch {
namespace rank {
class Comparator;
}  // namespace rank
}  // namespace isearch

using namespace std;
using namespace suez::turing;
using namespace isearch::search;

namespace isearch {
namespace rank {
AUTIL_LOG_SETUP(ha3, ComparatorCreator);

ComparatorCreator::ComparatorCreator(
        autil::mem_pool::Pool *pool,
        bool allowLazyEvaluate)
{
    _pool = pool;
    _allowLazyEvaluate = allowLazyEvaluate;
}

ComparatorCreator::~ComparatorCreator() {
}

ComboComparator *ComparatorCreator::createSortComparator(
        const SortExpressionVector &exprs)
{
    _immEvaluateExprs.clear();
    _immEvaluateExprVec.clear();
    _lazyEvaluateExprs.clear();
    // the first expr should alway be evaluated immediately
    _immEvaluateExprs.insert(exprs[0]->getAttributeExpression());
    _immEvaluateExprVec.push_back(exprs[0]->getAttributeExpression());
    return doCreateOneComparator(exprs);
}

vector<ComboComparator*> ComparatorCreator::createRankSortComparators(
        const std::vector<search::SortExpressionVector> &exprs)
{
    _immEvaluateExprs.clear();
    _immEvaluateExprVec.clear();
    _lazyEvaluateExprs.clear();
    for (size_t i = 0; i < exprs.size(); ++i) {
        // all dimentions' first attribute should be evaluated immediately
        _immEvaluateExprs.insert(exprs[i][0]->getAttributeExpression());
        _immEvaluateExprVec.push_back(exprs[i][0]->getAttributeExpression());
    }

    vector<ComboComparator*> comps;
    comps.resize(exprs.size());
    for (size_t i = 0; i < exprs.size(); ++i) {
        comps[i] = doCreateOneComparator(exprs[i]);
    }
    return comps;
}

ComboComparator *ComparatorCreator::doCreateOneComparator(
        const SortExpressionVector &exprs)
{
    vector<ComparatorType> compTypeVec;
    for (size_t i = 0; i < exprs.size(); ++i) {
        AttributeExpression *attrExpr = exprs[i]->getAttributeExpression();
        bool needImmEvaluate =
            _immEvaluateExprs.find(attrExpr) != _immEvaluateExprs.end();
        if (needImmEvaluate
            || attrExpr->getExpressionType() == ET_RANK
            || !_allowLazyEvaluate)
        {
            if (!needImmEvaluate) {
                _immEvaluateExprVec.push_back(attrExpr);
                _immEvaluateExprs.insert(attrExpr);
            }
            compTypeVec.push_back(CT_REFERENCE);
        } else {
            _lazyEvaluateExprs.insert(attrExpr);
            compTypeVec.push_back(CT_EXPRESSION);
        }
    }
    ComboComparator *comboComp = NULL;
    if (compTypeVec.size() == 1) {
        comboComp = createOptimizedComparator(_pool, exprs[0], compTypeVec[0]);
    } else if (compTypeVec.size() == 2) {
        comboComp = createOptimizedComparator(_pool, exprs[0], exprs[1],
                compTypeVec[0], compTypeVec[1]);
    } else if (compTypeVec.size() == 3) {
        comboComp = createOptimizedComparator(_pool, exprs[0], exprs[1], exprs[2],
                compTypeVec[0], compTypeVec[1], compTypeVec[2]);
    }
    if (comboComp == NULL) {
        comboComp = POOL_NEW_CLASS(_pool, ComboComparator);
        for (size_t i = 0; i < exprs.size() && i < compTypeVec.size(); i++) {
            comboComp->addComparator(createComparator(_pool, exprs[i], compTypeVec[i]));
        }
    }
    return comboComp;
}

ComboComparator *ComparatorCreator::createOptimizedComparator(autil::mem_pool::Pool *pool,
        SortExpression *expr, ComparatorType ct)
{
    auto type = expr->getType();
    if (type == vt_int32) {
        return createComboComparatorTyped<int32_t>(pool, expr, ct);
    } else if (type == vt_int64) {
        return createComboComparatorTyped<int64_t>(pool, expr, ct);
    } else if (type == vt_double) {
        return createComboComparatorTyped<double>(pool, expr, ct);
    } else {
        return NULL;
    }
}

ComboComparator *ComparatorCreator::createOptimizedComparator(autil::mem_pool::Pool *pool,
        SortExpression *expr1, SortExpression *expr2,
        ComparatorType ct1, ComparatorType ct2)
{
    auto type1 = expr1->getType();
    auto type2 = expr2->getType();
    if (type1 == vt_int32 && type2 == vt_int32) {
        return createComboComparatorTyped<int32_t, int32_t>(pool, expr1, expr2, ct1, ct2);
    } else if (type1 == vt_int32 && type2 == vt_int64) {
        return createComboComparatorTyped<int32_t, int64_t>(pool, expr1, expr2, ct1, ct2);
    } else if (type1 == vt_int32 && type2 == vt_double) {
        return createComboComparatorTyped<int32_t, double>(pool, expr1, expr2, ct1, ct2);
    } else if (type1 == vt_int64 && type2 == vt_int32) {
        return createComboComparatorTyped<int64_t, int32_t>(pool, expr1, expr2, ct1, ct2);
    } else if (type1 == vt_int64 && type2 == vt_int64) {
        return createComboComparatorTyped<int64_t, int64_t>(pool, expr1, expr2, ct1, ct2);
    } else if (type1 == vt_int64 && type2 == vt_double) {
        return createComboComparatorTyped<int64_t, double>(pool, expr1, expr2, ct1, ct2);
    } else if (type1 == vt_double && type2 == vt_int32) {
        return createComboComparatorTyped<double, int32_t>(pool, expr1, expr2, ct1, ct2);
    } else if (type1 == vt_double && type2 == vt_int64) {
        return createComboComparatorTyped<double, int64_t>(pool, expr1, expr2, ct1, ct2);
    } else if (type1 == vt_double && type2 == vt_double) {
        return createComboComparatorTyped<double, double>(pool, expr1, expr2, ct1, ct2);
    } else {
        return NULL;
    }
}

ComboComparator *ComparatorCreator::createOptimizedComparator(autil::mem_pool::Pool *pool,
        SortExpression *expr1, SortExpression *expr2, SortExpression *expr3,
        ComparatorType ct1, ComparatorType ct2, ComparatorType ct3)
{
    auto type1 = expr1->getType();
    auto type2 = expr2->getType();
    auto type3 = expr3->getType();
    if (type1 == vt_int64 && type2 == vt_double && type3 ==vt_int32) {
        return createComboComparatorTyped<int64_t, double, int32_t>(
                pool, expr1, expr2, expr3, ct1, ct2, ct3);
    } else {
        return NULL;
    }
}

Comparator *ComparatorCreator::createComparator(autil::mem_pool::Pool *pool,
        SortExpression *expr, ComparatorType defaultType)
{
#define COMPARATOR_CREATOR_HELPER(vt_type)                              \
    case vt_type:                                                       \
    {                                                                   \
        Comparator *comp = NULL;                                        \
        bool isMulti = expr->isMultiValue();                            \
        if (isMulti) {                                                  \
            typedef VariableTypeTraits<vt_type, true>::AttrExprType T;  \
            comp = createComparatorTyped<T>(pool,                       \
                    expr->getAttributeExpression(), expr->getSortFlag(), defaultType);       \
        } else {                                                        \
            typedef VariableTypeTraits<vt_type, false>::AttrExprType T; \
            comp = createComparatorTyped<T>(pool,                       \
                    expr->getAttributeExpression(), expr->getSortFlag(), defaultType); \
        }                                                               \
        return comp;                                                    \
    }

    auto type = expr->getType();
    switch(type) {
        NUMERIC_VARIABLE_TYPE_MACRO_HELPER(COMPARATOR_CREATOR_HELPER);
        COMPARATOR_CREATOR_HELPER(vt_string);
    default:
        assert(false);
        return NULL;
    }
}

template <typename T>
ComboComparator *ComparatorCreator::createComboComparatorTyped(
        autil::mem_pool::Pool *pool, SortExpression *expr, ComparatorType exprType)
{
    AttributeExpressionTyped<T> *typedExpr =
        dynamic_cast<AttributeExpressionTyped<T> *>(expr->getAttributeExpression());
    if (exprType == CT_REFERENCE && typedExpr != NULL) {
        return POOL_NEW_CLASS(pool, OneRefComparatorTyped<T>,
                              typedExpr->getReference(), expr->getSortFlag());
    }
    return NULL;
}

template <typename T1, typename T2>
ComboComparator *ComparatorCreator::createComboComparatorTyped(
        autil::mem_pool::Pool *pool, SortExpression *expr1, SortExpression *expr2,
        ComparatorType exprType1, ComparatorType exprType2)
{
    AttributeExpressionTyped<T1> *typedExpr1 =
        dynamic_cast<AttributeExpressionTyped<T1> *>(expr1->getAttributeExpression());
    AttributeExpressionTyped<T2> *typedExpr2 =
        dynamic_cast<AttributeExpressionTyped<T2> *>(expr2->getAttributeExpression());
    if (typedExpr1 == NULL || typedExpr2 == NULL) {
        return NULL;
    }
    if (exprType1 == CT_REFERENCE && exprType2 == CT_REFERENCE){
        return new (pool->allocate(sizeof(TwoRefComparatorTyped<T1, T2>)))TwoRefComparatorTyped<T1, T2>(typedExpr1->getReference(), typedExpr2->getReference(),
                expr1->getSortFlag(), expr2->getSortFlag());
    } else if (exprType1 == CT_REFERENCE && exprType2 == CT_EXPRESSION) {
        return new (pool->allocate(sizeof(RefAndExprComparatorTyped<T1, T2>)))RefAndExprComparatorTyped<T1, T2>(typedExpr1->getReference(), typedExpr2,
                expr1->getSortFlag(), expr2->getSortFlag());
    }
    return NULL;
}

template <typename T1, typename T2, typename T3>
ComboComparator *ComparatorCreator::createComboComparatorTyped(
        autil::mem_pool::Pool *pool, SortExpression *expr1, SortExpression *expr2,
        SortExpression *expr3, ComparatorType exprType1,
        ComparatorType exprType2, ComparatorType exprType3)
{
    AttributeExpressionTyped<T1> *typedExpr1 =
        dynamic_cast<AttributeExpressionTyped<T1> *>(expr1->getAttributeExpression());
    AttributeExpressionTyped<T2> *typedExpr2 =
        dynamic_cast<AttributeExpressionTyped<T2> *>(expr2->getAttributeExpression());
    AttributeExpressionTyped<T3> *typedExpr3 =
        dynamic_cast<AttributeExpressionTyped<T3> *>(expr3->getAttributeExpression());
    if (typedExpr1 == NULL || typedExpr2 == NULL || typedExpr3 == NULL) {
        return NULL;
    }
    if (exprType1 == CT_REFERENCE && exprType2 == CT_REFERENCE && exprType3 == CT_REFERENCE) {
        return new (pool->allocate(sizeof(ThreeRefComparatorTyped<T1, T2, T3>)))ThreeRefComparatorTyped<T1, T2, T3>(
                typedExpr1->getReference(), typedExpr2->getReference(),typedExpr3->getReference(),
                expr1->getSortFlag(), expr2->getSortFlag(), expr3->getSortFlag());
    } else if (exprType1 == CT_REFERENCE && exprType2 == CT_REFERENCE && exprType3 == CT_EXPRESSION) {
        return new (pool->allocate(sizeof(TwoRefAndExprComparatorTyped<T1, T2, T3>)))TwoRefAndExprComparatorTyped<T1, T2,T3>(
                typedExpr1->getReference(), typedExpr2->getReference(), typedExpr3,
                expr1->getSortFlag(), expr2->getSortFlag(), expr3->getSortFlag());
    }
    return NULL;
}

template <typename T>
Comparator *ComparatorCreator::createComparatorTyped(
        autil::mem_pool::Pool *pool, AttributeExpression *expr,
        bool flag, ComparatorType defaultType)
{
    AttributeExpressionTyped<T> *typedExpr =
        dynamic_cast<AttributeExpressionTyped<T> *>(expr);

    if (defaultType == CT_REFERENCE) {
        return POOL_NEW_CLASS(pool, ReferenceComparator<T>,
                              typedExpr->getReference(), flag);
    } else if (defaultType == CT_EXPRESSION) {
        return POOL_NEW_CLASS(pool, ExpressionComparator<T>, typedExpr, flag);
    } else {
        assert(false);
        return NULL;
    }
}

} // namespace rank
} // namespace isearch
