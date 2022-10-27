#ifndef ISEARCH_ANDNOTQUERY_H
#define ISEARCH_ANDNOTQUERY_H

#include <string>
#include <vector>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/Query.h>

BEGIN_HA3_NAMESPACE(common);

class AndNotQuery : public Query
{
public:
    AndNotQuery(const std::string &label);
    ~AndNotQuery();
public:
    bool operator == (const Query& query) const override;
    void accept(QueryVisitor *visitor) const override;
    void accept(ModifyQueryVisitor *visitor) override;
    Query *clone() const override;
    std::string getQueryName() const override {
        return "AndNotQuery";
    }
    QueryType getType() const override {
        return ANDNOT_QUERY;
    }
    void setQueryLabelWithDefaultLevel(const std::string &label) override {
        setQueryLabelBinary(label);
    }
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(common);

#endif //ISEARCH_ANDNOTQUERY_H
