#ifndef ISEARCH_ANDQUERY_H
#define ISEARCH_ANDQUERY_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/Query.h>

BEGIN_HA3_NAMESPACE(common);

class AndQuery : public Query
{
public:
    AndQuery(const std::string &label);
    AndQuery(const AndQuery &other);
    ~AndQuery();
public:
    bool operator == (const Query& query) const override;
    void accept(QueryVisitor *visitor) const override;
    void accept(ModifyQueryVisitor *visitor) override;
    Query *clone() const override;
    std::string getQueryName() const override {
        return "AndQuery";
    }

    QueryType getType() const override {
        return AND_QUERY;
    }
    void setQueryLabelWithDefaultLevel(const std::string &label) override {
        setQueryLabelBinary(label);
    }
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(common);

#endif //ISEARCH_ANDQUERY_H
