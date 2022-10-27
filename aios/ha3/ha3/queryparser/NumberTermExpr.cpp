#include <ha3/queryparser/NumberTermExpr.h>
#include <ha3/queryparser/QueryExprEvaluator.h>
#include <sstream>

using namespace std;
BEGIN_HA3_NAMESPACE(queryparser);
HA3_LOG_SETUP(queryparser, NumberTermExpr);

NumberTermExpr::NumberTermExpr(int64_t leftNumber, bool leftInclusive,
                   int64_t rightNumber, bool rightInclusive) 
{
    _leftNumber = leftNumber;
    _rightNumber = rightNumber;
    _leftInclusive = leftInclusive;
    _rightInclusive = rightInclusive;
}

NumberTermExpr::~NumberTermExpr() { 
}

void NumberTermExpr::setLeftNumber(int64_t leftNumber, bool leftInclusive) {
    _leftNumber = leftNumber;
    _leftInclusive = leftInclusive;
}

void NumberTermExpr::setRightNumber(int64_t rightNumber, bool rightInclusive) {
    _rightNumber = rightNumber;
    _rightInclusive = rightInclusive;    
}

void NumberTermExpr::evaluate(QueryExprEvaluator *qee) {
    qee->evaluateNumberExpr(this);
}

common::TermPtr NumberTermExpr::constructSearchTerm() {
    common::NumberTermPtr numberTermPtr(new common::NumberTerm(
                    _leftNumber, _leftInclusive, _rightNumber,
                    _rightInclusive, _indexName.c_str(),
                    _requiredFields, _boost, _secondaryChain));
    return dynamic_pointer_cast<common::Term>(numberTermPtr);
}

END_HA3_NAMESPACE(queryparser);

