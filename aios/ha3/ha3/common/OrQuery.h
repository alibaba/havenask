#ifndef ISEARCH_ORQUERY_H
#define ISEARCH_ORQUERY_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/Query.h>
#include <string>
#include <vector>

BEGIN_HA3_NAMESPACE(common);

class OrQuery : public Query
{
public:
    OrQuery(const std::string &label);
    OrQuery(const OrQuery &other);
    ~OrQuery();
public:
    bool operator == (const Query& query) const override;
    void accept(QueryVisitor *visitor) const override;
    void accept(ModifyQueryVisitor *visitor) override;
    Query *clone() const override;
    std::string getQueryName() const override {
        return "OrQuery";
    }

    QueryType getType() const override {
        return OR_QUERY;
    }
    void setQueryLabelWithDefaultLevel(const std::string &label) override {
        setQueryLabelBinary(label);
    }
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(common);

#endif //ISEARCH_ORQUERY_H
