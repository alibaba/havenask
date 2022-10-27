#ifndef ISEARCH_ORTERMQUERYEXPR_H
#define ISEARCH_ORTERMQUERYEXPR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/queryparser/AtomicQueryExpr.h>
#include <ha3/queryparser/OrQueryExpr.h>
#include <ha3/queryparser/IndexIdentifier.h>
#include <ha3/queryparser/ParserContext.h>

BEGIN_HA3_NAMESPACE(queryparser);

class OrTermQueryExpr : public AtomicQueryExpr
{
public:
    OrTermQueryExpr(AtomicQueryExpr *a, AtomicQueryExpr *b);
    ~OrTermQueryExpr();
public:
    void setIndexName(const std::string &indexName) override;
    void evaluate(QueryExprEvaluator *qee) override;
    void setRequiredFields(const common::RequiredFields &requiredFields) override;
    void setLabel(const std::string &label) override {
        _or.setLabel(label);
    }
    std::string getLabel() const override {
        return _or.getLabel();
    }
private:
    OrQueryExpr _or;
private:
    HA3_LOG_DECLARE();
};

//TYPEDEF_PTR(OrTermQueryExpr);

END_HA3_NAMESPACE(queryparser);

#endif //ISEARCH_ORTERMQUERYEXPR_H
