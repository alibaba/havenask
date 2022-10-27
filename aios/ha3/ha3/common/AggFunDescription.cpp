#include <ha3/common/AggFunDescription.h>
#include <ha3/queryparser/RequestSymbolDefine.h>

using namespace std;
using namespace suez::turing;

BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, AggFunDescription);

AggFunDescription::AggFunDescription() { 
    _syntaxExpr = NULL;
}

AggFunDescription::AggFunDescription(const string &functionName, 
                                     SyntaxExpr *syntaxExpr)
{
    _functionName = functionName;
    _syntaxExpr = syntaxExpr;
}

AggFunDescription::~AggFunDescription() { 
    delete _syntaxExpr;
}

void AggFunDescription::setFunctionName(const std::string& functionName) {
    _functionName = functionName;
}

const std::string& AggFunDescription::getFunctionName() const {
    return _functionName;
}

void AggFunDescription::setSyntaxExpr(SyntaxExpr *syntaxExpr) {
    if (_syntaxExpr) {
        delete _syntaxExpr;
    }
    _syntaxExpr = syntaxExpr;
}

SyntaxExpr* AggFunDescription::getSyntaxExpr() const {
    return _syntaxExpr;
}

#define AGGFUNDESCRIPTION_MEMBERS(X_X)          \
    X_X(_functionName);                         \
    X_X(_syntaxExpr);

HA3_SERIALIZE_IMPL(AggFunDescription, AGGFUNDESCRIPTION_MEMBERS);

std::string AggFunDescription::toString() const {
    return _functionName + AGGREGATE_DESCRIPTION_OPEN_PAREN
        + (_syntaxExpr ? _syntaxExpr->getExprString() : "")
        + AGGREGATE_DESCRIPTION_CLOSE_PAREN;
}

END_HA3_NAMESPACE(common);

