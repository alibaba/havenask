#ifndef ISEARCH_ORQUERYEXPR_H
#define ISEARCH_ORQUERYEXPR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/queryparser/BinaryQueryExpr.h>

BEGIN_HA3_NAMESPACE(queryparser);

class OrQueryExpr : public BinaryQueryExpr
{
public:
    OrQueryExpr(QueryExpr *a, QueryExpr *b) : BinaryQueryExpr(a, b) {}
    ~OrQueryExpr() {}
public:
    void evaluate(QueryExprEvaluator *qee);
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(queryparser);

#endif //ISEARCH_ORQUERYEXPR_H
