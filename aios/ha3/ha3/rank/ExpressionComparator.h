#ifndef ISEARCH_EXPRESSIONCOMPARATOR_H
#define ISEARCH_EXPRESSIONCOMPARATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <suez/turing/expression/framework/AttributeExpression.h>
#include <ha3/rank/Comparator.h>

BEGIN_HA3_NAMESPACE(rank);

template <typename T>
class ExpressionComparator : public Comparator
{
public:
    ExpressionComparator(suez::turing::AttributeExpressionTyped<T> *expr, bool flag = false) {
        _expr = expr;
        _flag = flag;
    }
    ~ExpressionComparator() {}
private:
    ExpressionComparator(const ExpressionComparator &);
    ExpressionComparator& operator=(const ExpressionComparator &);
public:
    bool compare(matchdoc::MatchDoc a, matchdoc::MatchDoc b) const  override {
        if (_flag) {
            return _expr->evaluateAndReturn(b) < _expr->evaluateAndReturn(a);
        } else {
            return _expr->evaluateAndReturn(a) < _expr->evaluateAndReturn(b);
        }
    }
    std::string getType() const  override { return "expression"; }

    void setReverseFlag(bool flag) { _flag = flag; }
    bool getReverseFlag() const { return _flag; }

private:
    suez::turing::AttributeExpressionTyped<T> *_expr;
    bool _flag;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(rank);

#endif //ISEARCH_EXPRESSIONCOMPARATOR_H
