#include <ha3/queryparser/MultiTermQueryExpr.h>
#include <ha3/queryparser/OrQueryExpr.h>
#include <ha3/queryparser/AndQueryExpr.h>
#include <ha3/queryparser/QueryExprEvaluator.h>

using namespace std;
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(common);


BEGIN_HA3_NAMESPACE(queryparser);
HA3_LOG_SETUP(queryparser, MultiTermQueryExpr);

MultiTermQueryExpr::MultiTermQueryExpr(QueryOperator opExpr)
    : _defaultOP(opExpr)
    , _minShoudMatch(1)
{ 
}

MultiTermQueryExpr::~MultiTermQueryExpr() { 
    for (TermExprArray::const_iterator it = _termQueryExprs.begin(); 
         it != _termQueryExprs.end(); it++)
    {
        delete(*it);
    }
}

void MultiTermQueryExpr::addTermQueryExpr(AtomicQueryExpr* expr) {
    _termQueryExprs.push_back(expr);
}

void MultiTermQueryExpr::setIndexName(const std::string &indexName) {
    for (TermExprArray::const_iterator it = _termQueryExprs.begin(); 
         it != _termQueryExprs.end(); it++)
    {
        (*it)->setIndexName(indexName);
    }
}
 
void MultiTermQueryExpr::setRequiredFields(const common::RequiredFields &requiredFields) {
    for (TermExprArray::const_iterator it = _termQueryExprs.begin(); 
         it != _termQueryExprs.end(); it++)
    {
        (*it)->setRequiredFields(requiredFields);
    }
}

void MultiTermQueryExpr::evaluate(QueryExprEvaluator *qee) {
    qee->evaluateMultiTermExpr(this);
}

END_HA3_NAMESPACE(queryparser);

