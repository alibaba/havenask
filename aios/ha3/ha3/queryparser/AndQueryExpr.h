#ifndef ISEARCH_ANDQUERYEXPR_H
#define ISEARCH_ANDQUERYEXPR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/queryparser/BinaryQueryExpr.h>

BEGIN_HA3_NAMESPACE(queryparser);

class AndQueryExpr : public BinaryQueryExpr
{
public:
    AndQueryExpr(QueryExpr *a, QueryExpr *b) : BinaryQueryExpr(a, b) {}
    ~AndQueryExpr() {}
public:
    void evaluate(QueryExprEvaluator *qee);
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(queryparser);

#endif //ISEARCH_ANDQUERYEXPR_H
