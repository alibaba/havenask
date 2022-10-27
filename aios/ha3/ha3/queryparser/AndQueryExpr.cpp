#include <ha3/queryparser/AndQueryExpr.h>
#include <ha3/queryparser/QueryExprEvaluator.h>

BEGIN_HA3_NAMESPACE(queryparser);
HA3_LOG_SETUP(queryparser, AndQueryExpr);

void AndQueryExpr::evaluate(QueryExprEvaluator *qee) {
    qee->evaluateAndExpr(this);
}

END_HA3_NAMESPACE(queryparser);

