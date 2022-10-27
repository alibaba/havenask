#include <ha3/queryparser/AndTermQueryExpr.h>
#include <ha3/queryparser/QueryExprEvaluator.h>

BEGIN_HA3_NAMESPACE(queryparser);
HA3_LOG_SETUP(queryparser, AndTermQueryExpr);


AndTermQueryExpr::AndTermQueryExpr(AtomicQueryExpr *a, AtomicQueryExpr *b)
    :_and(a,b)
{ 
}

AndTermQueryExpr::~AndTermQueryExpr() { 
}

void AndTermQueryExpr::setIndexName(const std::string &indexName) {
    AtomicQueryExpr *a = static_cast<AtomicQueryExpr *>(_and.getLeftExpr());
    AtomicQueryExpr *b = static_cast<AtomicQueryExpr *>(_and.getRightExpr());
    assert(a);
    assert(b);
    a->setIndexName(indexName);
    b->setIndexName(indexName);
}

void AndTermQueryExpr::setRequiredFields(
        const common::RequiredFields &requiredFields)
{
    AtomicQueryExpr *a = static_cast<AtomicQueryExpr *>(_and.getLeftExpr());
    AtomicQueryExpr *b = static_cast<AtomicQueryExpr *>(_and.getRightExpr());
    assert(a);
    assert(b);
    a->setRequiredFields(requiredFields);
    b->setRequiredFields(requiredFields);
}

void AndTermQueryExpr::evaluate(QueryExprEvaluator *qee) {
    qee->evaluateAndExpr(&_and);
}


END_HA3_NAMESPACE(queryparser);

