#ifndef ISEARCH_MULTITERMQUERYEXPR_H
#define ISEARCH_MULTITERMQUERYEXPR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/queryparser/AtomicQueryExpr.h>
#include <ha3/queryparser/OrQueryExpr.h>
#include <ha3/queryparser/IndexIdentifier.h>
#include <ha3/queryparser/ParserContext.h>
#include <ha3/config/QueryInfo.h>

BEGIN_HA3_NAMESPACE(queryparser);

class MultiTermQueryExpr : public AtomicQueryExpr
{
public:
    typedef std::vector<AtomicQueryExpr*> TermExprArray;
public:
    MultiTermQueryExpr(QueryOperator opExpr);
    ~MultiTermQueryExpr();
public:
    void setIndexName(const std::string &indexName);
    void setRequiredFields(const common::RequiredFields &requiredFields);
    void evaluate(QueryExprEvaluator *qee);
public:
    const QueryOperator getOpExpr() const {return _defaultOP;}
    TermExprArray& getTermExprs() {return _termQueryExprs;}

    void addTermQueryExpr(AtomicQueryExpr* expr);
    void setMinShouldMatch(uint32_t i) { _minShoudMatch = i; }
    const uint32_t getMinShouldMatch() const { return _minShoudMatch; }
private:
    TermExprArray _termQueryExprs;
    QueryOperator _defaultOP;
    uint32_t _minShoudMatch;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(MultiTermQueryExpr);

END_HA3_NAMESPACE(queryparser);

#endif //ISEARCH_MULTITERMQUERYEXPR_H
