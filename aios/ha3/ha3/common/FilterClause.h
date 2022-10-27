#ifndef ISEARCH_FILTERCLAUSE_H
#define ISEARCH_FILTERCLAUSE_H

#include <string>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/DataBuffer.h>
#include <ha3/common/ClauseBase.h>
#include <suez/turing/expression/syntax/SyntaxExpr.h>

BEGIN_HA3_NAMESPACE(common);

class FilterClause : public ClauseBase
{
public:
    FilterClause();
    FilterClause(suez::turing::SyntaxExpr *filterExpr);
    ~FilterClause();
public:
    void serialize(autil::DataBuffer &dataBuffer) const override;
    void deserialize(autil::DataBuffer &dataBuffer) override;
    std::string toString() const override;
public:
    void setRootSyntaxExpr(suez::turing::SyntaxExpr *filterExpr);
    const suez::turing::SyntaxExpr* getRootSyntaxExpr() const;  
private:
    suez::turing::SyntaxExpr *_filterExpr;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(FilterClause);

typedef FilterClause AuxFilterClause;
HA3_TYPEDEF_PTR(AuxFilterClause);

END_HA3_NAMESPACE(common);

#endif //ISEARCH_FILTERCLAUSE_H
