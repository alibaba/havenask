#ifndef ISEARCH_ANDTERMQUERYEXPR_H
#define ISEARCH_ANDTERMQUERYEXPR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/queryparser/AtomicQueryExpr.h>
#include <ha3/queryparser/AndQueryExpr.h>
#include <ha3/queryparser/IndexIdentifier.h>
#include <ha3/queryparser/ParserContext.h>

BEGIN_HA3_NAMESPACE(queryparser);

class AndTermQueryExpr : public AtomicQueryExpr
{
public:
    AndTermQueryExpr(AtomicQueryExpr *a, AtomicQueryExpr *b);
    ~AndTermQueryExpr();
public:
    void setIndexName(const std::string &indexName) override;
    void evaluate(QueryExprEvaluator *qee) override;
    void setRequiredFields(const common::RequiredFields &requiredFields) override;
    void setLabel(const std::string &label) override {
        _and.setLabel(label);
    }
    std::string getLabel() const override {
        return _and.getLabel();
    }
private:
    AndQueryExpr _and;
private:
    HA3_LOG_DECLARE();
};

//TYPEDEF_PTR(AndTermQueryExpr);

END_HA3_NAMESPACE(queryparser);

#endif //ISEARCH_ANDTERMQUERYEXPR_H
