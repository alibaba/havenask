
#ifndef ISEARCH_BINARYQUERYEXPR_H
#define ISEARCH_BINARYQUERYEXPR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/queryparser/QueryExpr.h>

BEGIN_HA3_NAMESPACE(queryparser);

class BinaryQueryExpr : public QueryExpr
{
public:
    BinaryQueryExpr(QueryExpr *a, QueryExpr *b);
    ~BinaryQueryExpr();
public:
    QueryExpr *getLeftExpr() {return _exprA;}
    QueryExpr *getRightExpr() {return _exprB;}
    virtual void setLeafIndexName(const std::string &indexName) override {
        _exprA->setLeafIndexName(indexName);
        _exprB->setLeafIndexName(indexName);
    }
private:
    QueryExpr *_exprA;
    QueryExpr *_exprB;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(queryparser);

#endif //ISEARCH_BINARYQUERYEXPR_H
