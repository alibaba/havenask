#ifndef ISEARCH_AGGREGATEPRPARSER_H
#define ISEARCH_AGGREGATEPRPARSER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <string>
#include <vector>
#include <ha3/common/AggregateDescription.h>
#include <ha3/queryparser/SyntaxExprParser.h>

BEGIN_HA3_NAMESPACE(queryparser);

class AggregateParser
{
public:
    AggregateParser();
    ~AggregateParser();
public:
    common::AggregateDescription *createAggDesc();
    std::vector<std::string> *createRangeExpr();
    std::vector<common::AggFunDescription*> *createFuncDescs();
    common::AggFunDescription* createAggMaxFunc(suez::turing::SyntaxExpr *expr);
    common::AggFunDescription* createAggMinFunc(suez::turing::SyntaxExpr *expr);
    common::AggFunDescription* createAggCountFunc();
    common::AggFunDescription* createAggSumFunc(suez::turing::SyntaxExpr *expr);
    common::AggFunDescription *createAggDistinctCountFunc(suez::turing::SyntaxExpr *expr);
    common::FilterClause* createFilterClause(suez::turing::SyntaxExpr *expr);
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(queryparser);

#endif //ISEARCH_AGGREGATEPRPARSER_H
