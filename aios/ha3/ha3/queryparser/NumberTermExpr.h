#ifndef ISEARCH_NUMBERTERMEXPR_H
#define ISEARCH_NUMBERTERMEXPR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/queryparser/AtomicQueryExpr.h>
#include <ha3/common/NumberTerm.h>
#include <limits.h>

BEGIN_HA3_NAMESPACE(queryparser);

class NumberTermExpr : public AtomicQueryExpr
{
public:
    NumberTermExpr(int64_t leftNumber, bool leftInclusive,
                   int64_t rightNumber, bool rightInclusive);
    ~NumberTermExpr();
public:
    int64_t getLeftNumber() const {return _leftNumber;}
    int64_t getRightNumber() const {return _rightNumber;}
    int64_t getLeftInclusive() const {return _leftInclusive;}
    int64_t getRightInclusive() const {return _rightInclusive;}

    void setLeftNumber(int64_t leftNumber, bool leftInclusive);
    void setRightNumber(int64_t rightNumber, bool rightInclusive);

    void evaluate(QueryExprEvaluator *qee);
    common::TermPtr constructSearchTerm();
private:
    int64_t _leftNumber;
    int64_t _rightNumber;
    bool _leftInclusive;
    bool _rightInclusive;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(queryparser);

#endif //ISEARCH_NUMBERTERMEXPR_H
