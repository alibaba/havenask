#ifndef ISEARCH_PHRASEQUERY_H
#define ISEARCH_PHRASEQUERY_H

#include <vector>
#include <ha3/isearch.h>
#include <ha3/common.h>
#include <ha3/util/Log.h>
#include <autil/DataBuffer.h>
#include <ha3/common/Term.h>
#include <ha3/common/Query.h>

BEGIN_HA3_NAMESPACE(common);

class PhraseQuery : public Query
{
public:
    typedef std::vector<TermPtr> TermArray;
public:
    PhraseQuery(const std::string &label);
    PhraseQuery(const PhraseQuery &other);
    ~PhraseQuery();
public:
    void addTerm(const TermPtr& term);
    bool operator == (const Query& query) const override;
    void accept(QueryVisitor *visitor) const override;
    void accept(ModifyQueryVisitor *visitor) override;
    Query *clone() const override;
    std::string getQueryName() const override {
        return "PhraseQuery";
    }
    std::string toString() const override;
    const TermArray& getTermArray() const;
    TermArray& getTermArray() {
        return _terms;
    }
    void serialize(autil::DataBuffer &dataBuffer) const override;
    void deserialize(autil::DataBuffer &dataBuffer) override;
    QueryType getType() const override {
        return PHRASE_QUERY;
    }
    void setQueryLabelWithDefaultLevel(const std::string &label) override {
        setQueryLabelTerm(label);
    }
private:
    TermArray _terms;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(PhraseQuery);

END_HA3_NAMESPACE(common);

#endif //ISEARCH_PHRASEQUERY_H
