#ifndef ISEARCH_TERMQUERY_H
#define ISEARCH_TERMQUERY_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/DataBuffer.h>
#include <ha3/common/Query.h>
#include <ha3/common/Term.h>

BEGIN_HA3_NAMESPACE(common);

class TermQuery : public Query
{
public:
    TermQuery(const Term& term, const std::string &label);
    TermQuery(const std::string &word, const std::string &indexName,
              const RequiredFields &requiredFields,
              const std::string &label,
              int32_t boost = DEFAULT_BOOST_VALUE,
              const std::string &truncateName = "")
        : _term(word, indexName, requiredFields, boost, truncateName) {
        setQueryLabelTerm(label);
    }    

    TermQuery(const build_service::analyzer::Token& token, const char *indexName,
              const RequiredFields &requiredFields,
              const std::string &label,
              int32_t boost = DEFAULT_BOOST_VALUE,
              const std::string &truncateName = "")
        : _term(token, indexName, requiredFields, boost, truncateName) {
        setQueryLabelTerm(label);
    }
    TermQuery(const TermQuery& other);
    ~TermQuery();
public:
    bool operator == (const Query &query) const override;
    void accept(QueryVisitor *visitor) const override;
    void accept(ModifyQueryVisitor *visitor) override;
    Query *clone() const override;
    void setTerm(const Term& term);
    const Term& getTerm() const;
    Term& getTerm();
    std::string getQueryName() const override;
    std::string toString() const override;
    void serialize(autil::DataBuffer &dataBuffer) const override;
    void deserialize(autil::DataBuffer &dataBuffer) override;
    QueryType getType() const override {
        return TERM_QUERY;
    }
    void setQueryLabelWithDefaultLevel(const std::string &label) override {
        setQueryLabelTerm(label);
    }
private:
    Term _term;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(TermQuery);

END_HA3_NAMESPACE(common);

#endif //ISEARCH_TERMQUERY_H
