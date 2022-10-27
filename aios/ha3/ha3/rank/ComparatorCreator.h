#ifndef ISEARCH_COMPARATORCREATOR_H
#define ISEARCH_COMPARATORCREATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <suez/turing/expression/framework/AttributeExpression.h>
#include <ha3/search/SortExpression.h>
#include <ha3/rank/ComboComparator.h>
#include <suez/turing/expression/framework/AttributeExpressionPool.h>
#include <ha3/search/SortExpressionCreator.h>
#include <autil/mem_pool/Pool.h>

BEGIN_HA3_NAMESPACE(rank);

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
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(ComparatorCreator);

END_HA3_NAMESPACE(rank);

#endif //ISEARCH_COMPARATORCREATOR_H
