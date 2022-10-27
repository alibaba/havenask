#ifndef ISEARCH_QUERYEXECUTOREXPRESSIONWRAPPER_H
#define ISEARCH_QUERYEXECUTOREXPRESSIONWRAPPER_H

#include <ha3/sql/common/common.h>
#include <ha3/search/QueryExecutor.h>
#include <suez/turing/expression/framework/AttributeExpression.h>

BEGIN_HA3_NAMESPACE(sql);

class QueryExecutorExpressionWrapper : public suez::turing::AttributeExpressionTyped<bool>
{
public:
    QueryExecutorExpressionWrapper(search::QueryExecutor* queryExecutor);
    ~QueryExecutorExpressionWrapper();
private:
    QueryExecutorExpressionWrapper(const QueryExecutorExpressionWrapper &);
    QueryExecutorExpressionWrapper& operator=(const QueryExecutorExpressionWrapper &);
public:
    bool evaluateAndReturn(matchdoc::MatchDoc matchDoc) override;

private:
    search::QueryExecutor* _queryExecutor;
};

HA3_TYPEDEF_PTR(QueryExecutorExpressionWrapper);

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_QUERYEXECUTOREXPRESSIONWRAPPER_H
