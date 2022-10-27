#include <ha3/queryparser/OrTermQueryExpr.h>
#include <ha3/queryparser/QueryExprEvaluator.h>

BEGIN_HA3_NAMESPACE(queryparser);
HA3_LOG_SETUP(queryparser, OrTermQueryExpr);

OrTermQueryExpr::OrTermQueryExpr(AtomicQueryExpr *a, AtomicQueryExpr *b)
    :_or(a,b)
{ 
}

OrTermQueryExpr::~OrTermQueryExpr() { 
}

void OrTermQueryExpr::setIndexName(const std::string &indexName) {
    AtomicQueryExpr *a = static_cast<AtomicQueryExpr *>(_or.getLeftExpr());
    AtomicQueryExpr *b = static_cast<AtomicQueryExpr *>(_or.getRightExpr());
    assert(a);
    assert(b);
    a->setIndexName(indexName);
    b->setIndexName(indexName);
}

void OrTermQueryExpr::setRequiredFields(
        const common::RequiredFields &requiredFields)
{
    AtomicQueryExpr *a = static_cast<AtomicQueryExpr *>(_or.getLeftExpr());
    AtomicQueryExpr *b = static_cast<AtomicQueryExpr *>(_or.getRightExpr());
    assert(a);
    assert(b);
    a->setRequiredFields(requiredFields);
    b->setRequiredFields(requiredFields);
}

void OrTermQueryExpr::evaluate(QueryExprEvaluator *qee) {
    qee->evaluateOrExpr(&_or);
}

END_HA3_NAMESPACE(queryparser);

