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

#include <memory>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/search/SortExpression.h"
#include "suez/turing/expression/framework/AttributeExpression.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil
namespace isearch {
namespace rank {
class ComboComparator;
class Comparator;
}  // namespace rank
}  // namespace isearch

namespace isearch {
namespace rank {

class ComparatorCreator
{
public:
    enum ComparatorType {
        CT_REFERENCE,
        CT_EXPRESSION,
    };
public:
    ComparatorCreator(autil::mem_pool::Pool *pool,
                      bool allowLazyEvaluate = false);
    ~ComparatorCreator();
private:
    ComparatorCreator(const ComparatorCreator &);
    ComparatorCreator& operator=(const ComparatorCreator &);
public:
    ComboComparator *createSortComparator(
            const search::SortExpressionVector &exprs);

    std::vector<ComboComparator*> createRankSortComparators(
            const std::vector<search::SortExpressionVector> &exprs);    

    const std::vector<suez::turing::AttributeExpression*> &getImmEvaluateExpressions() const {
        return _immEvaluateExprVec;
    }
    
    const suez::turing::ExpressionSet &getLazyEvaluateExpressions() const {
        return _lazyEvaluateExprs;
    }
public:
    static ComboComparator *createOptimizedComparator(autil::mem_pool::Pool *pool,
            search::SortExpression *expr, ComparatorType ct);
    static ComboComparator *createOptimizedComparator(autil::mem_pool::Pool *pool,
            search::SortExpression *expr1, search::SortExpression *expr2,
            ComparatorType ct1, ComparatorType ct2);
    static ComboComparator *createOptimizedComparator(autil::mem_pool::Pool *pool,
            search::SortExpression *expr1, search::SortExpression *expr2,
            search::SortExpression *expr3, ComparatorType ct1, ComparatorType ct2,
            ComparatorType ct3);

    // public for test
    static Comparator *createComparator(autil::mem_pool::Pool *pool,
            search::SortExpression *expr,
            ComparatorType defaultType = CT_REFERENCE);
private:
    ComboComparator *doCreateOneComparator(
            const search::SortExpressionVector &exprs);
    
    template <typename T>
    static Comparator *createComparatorTyped(autil::mem_pool::Pool *pool, 
            suez::turing::AttributeExpression *expr, bool flag, ComparatorType defaultType);
    template <typename T>
    static ComboComparator *createComboComparatorTyped(
            autil::mem_pool::Pool *pool, search::SortExpression *expr, ComparatorType exprType);
    template <typename T1, typename T2>
    static ComboComparator *createComboComparatorTyped(
            autil::mem_pool::Pool *pool, search::SortExpression *expr1,
            search::SortExpression *expr2,
            ComparatorType exprType1, ComparatorType exprType2);
    template <typename T1, typename T2, typename T3>
    static ComboComparator *createComboComparatorTyped(
            autil::mem_pool::Pool *pool, search::SortExpression *expr1,
            search::SortExpression *expr2, search::SortExpression *expr3,
            ComparatorType exprType1, ComparatorType exprType2, ComparatorType exprType3);


private:
    autil::mem_pool::Pool *_pool;
    bool _allowLazyEvaluate;
    std::vector<suez::turing::AttributeExpression*> _immEvaluateExprVec;
    suez::turing::ExpressionSet _immEvaluateExprs;
    suez::turing::ExpressionSet _lazyEvaluateExprs;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<ComparatorCreator> ComparatorCreatorPtr;

} // namespace rank
} // namespace isearch

