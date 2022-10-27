#ifndef ISEARCH_QUERYEXPR_H
#define ISEARCH_QUERYEXPR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/Query.h>
#include <assert.h>

BEGIN_HA3_NAMESPACE(queryparser);

class QueryExprEvaluator;
class QueryExpr
{
public:
    QueryExpr();
    virtual ~QueryExpr();
public:
    virtual void evaluate(QueryExprEvaluator *qee) = 0;
    virtual void setLeafIndexName(const std::string &indexName) = 0;
    virtual void setLabel(const std::string &label) {
        _label = label;
    }
    virtual std::string getLabel() const {
        return _label;
    }
private:
    std::string _label;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(queryparser);

#endif //ISEARCH_QUERYEXPR_H
