#include <ha3/queryparser/OrQueryExpr.h>
#include <ha3/queryparser/QueryExprEvaluator.h>

BEGIN_HA3_NAMESPACE(queryparser);
HA3_LOG_SETUP(queryparser, OrQueryExpr);

void OrQueryExpr::evaluate(QueryExprEvaluator *qee) {
    qee->evaluateOrExpr(this);
}

END_HA3_NAMESPACE(queryparser);

