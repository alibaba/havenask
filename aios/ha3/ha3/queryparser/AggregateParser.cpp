#include <ha3/queryparser/AggregateParser.h>
#include <sstream>
#include <ha3/common/AggregateDescription.h>
#include <ha3/queryparser/RequestSymbolDefine.h>

using namespace std;
using namespace suez::turing;

USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(queryparser);

HA3_LOG_SETUP(queryparser, AggregateParser);

AggregateParser::AggregateParser() {
}

AggregateParser::~AggregateParser() { 
}

AggregateDescription* AggregateParser::createAggDesc() {
    return new AggregateDescription;
}

vector<string>* AggregateParser::createRangeExpr() {
    return new vector<string>;
}

vector<AggFunDescription*>* AggregateParser::createFuncDescs() {
    return new vector<AggFunDescription*>;
}

AggFunDescription* AggregateParser::createAggMaxFunc(SyntaxExpr *expr) {
    assert(expr);
    return new AggFunDescription(AGGREGATE_FUNCTION_MAX, expr);
}

AggFunDescription* AggregateParser::createAggMinFunc(SyntaxExpr *expr) {
    assert(expr);
    return new AggFunDescription(AGGREGATE_FUNCTION_MIN, expr);
}

AggFunDescription* AggregateParser::createAggCountFunc() {
    return new AggFunDescription(AGGREGATE_FUNCTION_COUNT, NULL);
}

AggFunDescription* AggregateParser::createAggSumFunc(SyntaxExpr *expr) {
    assert(expr);
    return new AggFunDescription(AGGREGATE_FUNCTION_SUM, expr);
}

AggFunDescription* AggregateParser::createAggDistinctCountFunc(SyntaxExpr *expr) {
    assert(expr);
    return new AggFunDescription(AGGREGATE_FUNCTION_DISTINCT_COUNT, expr);
}
 
FilterClause* AggregateParser::createFilterClause(SyntaxExpr *expr) {
    FilterClause* filterClause = new FilterClause(expr);
    if (expr) {
        filterClause->setOriginalString(expr->getExprString());
    }
    return filterClause;
}


END_HA3_NAMESPACE(queryparser);

