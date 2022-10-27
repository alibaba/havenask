#include <ha3/queryparser/RankQueryExpr.h>
#include <ha3/queryparser/QueryExprEvaluator.h>

BEGIN_HA3_NAMESPACE(queryparser);
HA3_LOG_SETUP(queryparser, RankQueryExpr);

void RankQueryExpr::evaluate(QueryExprEvaluator *qee) {
    qee->evaluateRankExpr(this);
}


END_HA3_NAMESPACE(queryparser);

