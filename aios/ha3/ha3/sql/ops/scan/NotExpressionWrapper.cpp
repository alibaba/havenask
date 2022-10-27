#include <ha3/sql/ops/scan/NotExpressionWrapper.h>

using namespace std;

BEGIN_HA3_NAMESPACE(sql);

NotExpressionWrapper::NotExpressionWrapper(AttributeExpressionTyped<bool> *expression)
    : _expression(expression)
{}

NotExpressionWrapper::~NotExpressionWrapper() { 
    _expression = NULL;
}

bool NotExpressionWrapper::evaluateAndReturn(matchdoc::MatchDoc matchDoc) {
    assert(_expression);
    return !_expression->evaluateAndReturn(matchDoc);
}

END_HA3_NAMESPACE(sql);

