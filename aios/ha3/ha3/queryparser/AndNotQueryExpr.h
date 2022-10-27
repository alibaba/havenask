#ifndef ISEARCH_ANDNOTQUERYEXPR_H
#define ISEARCH_ANDNOTQUERYEXPR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/queryparser/BinaryQueryExpr.h>

BEGIN_HA3_NAMESPACE(queryparser);

class AndNotQueryExpr : public BinaryQueryExpr
{
public:
    AndNotQueryExpr(QueryExpr *a, QueryExpr *b) : BinaryQueryExpr(a, b) {}
    ~AndNotQueryExpr() {}
public:
    void evaluate(QueryExprEvaluator *qee);
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(queryparser);

#endif //ISEARCH_ANDNOTQUERYEXPR_H
