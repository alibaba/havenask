#ifndef ISEARCH_MULTITERMQUERY_H
#define ISEARCH_MULTITERMQUERY_H

#include <ha3/common/Query.h>
#include <autil/DataBuffer.h>
#include <ha3/common/Term.h>

BEGIN_HA3_NAMESPACE(common);

class MultiTermQuery : public Query
{
public:
    typedef std::vector<TermPtr> TermArray;
public:

    MultiTermQuery(const std::string &label, QueryOperator op = OP_AND);
    MultiTermQuery(const MultiTermQuery &);
    ~MultiTermQuery();
public:
    bool operator == (const Query& query) const override;
    void accept(QueryVisitor *visitor) const override;
    void accept(ModifyQueryVisitor *visitor) override;
    Query *clone() const override;

    std::string getQueryName() const override {
        return "MultiTermQuery";
    }
    std::string toString() const override;
    const TermArray& getTermArray() const;
    TermArray& getTermArray() {
        return _terms;
    }
    void addTerm(const TermPtr& term);

    void setOPExpr(QueryOperator opExpr) {_opExpr = opExpr;}
    const QueryOperator getOpExpr() const {return _opExpr;}
    void setMinShouldMatch(uint32_t i) { _minShoudMatch = std::min(i, (uint32_t)_terms.size()); }
    const uint32_t getMinShouldMatch() const { return _minShoudMatch; }

    void serialize(autil::DataBuffer &dataBuffer) const override;
    void deserialize(autil::DataBuffer &dataBuffer) override;

    QueryType getType() const override {
        return MULTI_TERM_QUERY;
    }
    void setQueryLabelWithDefaultLevel(const std::string &label) override {
        setQueryLabelTerm(label);
    }
private:
    TermArray _terms;
    QueryOperator _opExpr;
    uint32_t _minShoudMatch;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(MultiTermQuery);

END_HA3_NAMESPACE(common);

#endif //ISEARCH_MULTITERMQUERY_H
