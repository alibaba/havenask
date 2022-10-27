#ifndef ISEARCH_NUMBERQUERY_H
#define ISEARCH_NUMBERQUERY_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/NumberTerm.h>
#include <ha3/common/Query.h>

BEGIN_HA3_NAMESPACE(common);

class NumberQuery : public Query
{
public:
    NumberQuery(int64_t leftNumber, bool leftInclusive, int64_t rightNumber,
                bool rightInclusive, const char *indexName,
                const RequiredFields &requiredFields,
                const std::string &label,
                int64_t boost = DEFAULT_BOOST_VALUE,
                const std::string &truncateName = "");
    NumberQuery(int64_t num, const char *indexName,
                const RequiredFields &requiredFields,
                const std::string &label,
                int64_t boost = DEFAULT_BOOST_VALUE,
                const std::string &truncateName = "");
    NumberQuery(const NumberTerm& term, const std::string &label);
    NumberQuery(const NumberQuery &other);
    ~NumberQuery();
public:
    void setTerm(const NumberTerm& term);
    bool operator == (const Query& query) const override;
    void accept(QueryVisitor *visitor) const override;
    void accept(ModifyQueryVisitor *visitor) override;
    Query *clone() const override;
    void addQuery(QueryPtr query) override {
        assert(false);
    }
    bool removeQuery(const Query *query);
    Query *rewriteQuery();//optimize query
    const NumberTerm& getTerm() const;
    NumberTerm& getTerm();
    std::string getQueryName() const override { return "NumberQuery"; }
    std::string toString() const override;

    void serialize(autil::DataBuffer &dataBuffer) const override {
        dataBuffer.write(_term);
        serializeMDLandQL(dataBuffer);
    }
    void deserialize(autil::DataBuffer &dataBuffer) override {
        dataBuffer.read(_term);
        deserializeMDLandQL(dataBuffer);
    }
    QueryType getType() const override {
        return NUMBER_QUERY;
    }
    void setQueryLabelWithDefaultLevel(const std::string &label) override {
        setQueryLabelTerm(label);
    }
private:
    NumberTerm _term;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(common);

#endif //ISEARCH_NUMBERQUERY_H
