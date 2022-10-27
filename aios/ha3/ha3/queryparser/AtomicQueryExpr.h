#ifndef ISEARCH_ATOMICQUERYEXPR_H
#define ISEARCH_ATOMICQUERYEXPR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/Term.h>
#include <ha3/queryparser/QueryExpr.h>
#include <ha3/queryparser/IndexIdentifier.h>
#include <ha3/queryparser/ParserContext.h>

BEGIN_HA3_NAMESPACE(queryparser);

class AtomicQueryExpr : public QueryExpr
{
public:
    AtomicQueryExpr();
    virtual ~AtomicQueryExpr();
public:
    virtual void setIndexName(const std::string &indexName) {_indexName = indexName;}
    virtual void setLeafIndexName(const std::string &indexName) override {
        setIndexName(indexName);
    }
    const std::string &getIndexName() const { return _indexName;}
    virtual void setRequiredFields(const common::RequiredFields &requiredFields)
    {
        _requiredFields.fields.assign(requiredFields.fields.begin(),
                requiredFields.fields.end());
        _requiredFields.isRequiredAnd = requiredFields.isRequiredAnd;
        sort(_requiredFields.fields.begin(), _requiredFields.fields.end());
    }
    const common::RequiredFields& getRequiredFields() const {
        return _requiredFields;}

    void setBoost(int32_t boost) {_boost = boost;}
    int32_t getBoost() const {return _boost;}

    void setSecondaryChain(const std::string &secondaryChain) {
        _secondaryChain = secondaryChain;
    }
    std::string getSecondaryChain() const {return _secondaryChain;}
    const std::string& getText() const {return _text;}
    void setText(const std::string &text) {_text = text;}

    virtual common::TermPtr constructSearchTerm() {
        return common::TermPtr(new common::Term(_text.c_str(), _indexName.c_str(),
                        _requiredFields, _boost, _secondaryChain));
    }

protected:
    std::string _indexName;
    std::string _secondaryChain;
    common::RequiredFields _requiredFields;
    int32_t _boost;
    std::string _text;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(queryparser);

#endif //ISEARCH_ATOMICQUERYEXPR_H
