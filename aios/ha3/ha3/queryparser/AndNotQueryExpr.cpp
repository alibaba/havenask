#include <ha3/queryparser/AndNotQueryExpr.h>
#include <ha3/queryparser/QueryExprEvaluator.h>

BEGIN_HA3_NAMESPACE(queryparser);
HA3_LOG_SETUP(queryparser, AndNotQueryExpr);

void AndNotQueryExpr::evaluate(QueryExprEvaluator *qee) {
    qee->evaluateAndNotExpr(this);
}

END_HA3_NAMESPACE(queryparser);

