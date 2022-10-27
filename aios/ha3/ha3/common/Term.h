#ifndef ISEARCH_TERM_H
#define ISEARCH_TERM_H

#include <string>
#include <sstream>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/DataBuffer.h>
#include <build_service/analyzer/Token.h>

BEGIN_HA3_NAMESPACE(common);

struct RequiredFields {
    RequiredFields() {
        isRequiredAnd = true;
    }

    bool operator == (const RequiredFields& other) const;
    bool operator != (const RequiredFields& other) const {
        return !(*this == other);
    }

    std::vector<std::string> fields;
    bool isRequiredAnd;
};

class Term
{
public:
    Term() {
        _boost = DEFAULT_BOOST_VALUE;
        _truncateName = "";
    }
    Term(const std::string &word, const std::string &indexName, 
         const RequiredFields &requiredFields, 
         int32_t boost = DEFAULT_BOOST_VALUE,
         const std::string &truncateName = "")
        : _token(word, 0), 
          _indexName(indexName),
          _boost(boost),
          _truncateName(truncateName)
    {
        copyRequiredFields(requiredFields);
    }

    Term(const build_service::analyzer::Token& token, const char *indexName, 
         const RequiredFields &requiredFields, 
         int32_t boost = DEFAULT_BOOST_VALUE,
         const std::string &truncateName = "")
        : _token(token)
        , _indexName(indexName)
        , _boost(boost)
        , _truncateName(truncateName)
    {
        copyRequiredFields(requiredFields);
    }

    Term(const Term& t) {
        if (&t != this) {  
            _token = t._token;
            _indexName = t._indexName;
            copyRequiredFields(t._requiredFields);
            _boost = t._boost;
            _truncateName = t._truncateName;
        }
    }

    virtual Term *clone() {
        return new Term(*this);
    }

    virtual ~Term() {
    }

public:
    const std::string &getWord() const {
        return _token.getNormalizedText();
    }

    void setWord(const char *word) {
        _token.setText(word);
        _token.setNormalizedText(word);
    }
 
    const build_service::analyzer::Token& getToken() const {
        return _token;
    }
    build_service::analyzer::Token& getToken() {
        return _token;
    }

    const std::string &getIndexName() const {
        return _indexName;
    }
    void setIndexName(const char *indexName) {
        _indexName = indexName;
    }

    const RequiredFields &getRequiredFields() const {
        return _requiredFields;
    }

    int32_t getBoost() const {return _boost;}
    void setBoost(int32_t boost) {_boost = boost;}

    const std::string &getTruncateName() const {return _truncateName;}
    
    void setTruncateName(const std::string &truncateName) {
        _truncateName = truncateName;
    }
    bool hasTruncateName() const {
        return !_truncateName.empty();
    }

    void copyRequiredFields(const RequiredFields &requiredFields) {
        _requiredFields.fields.assign(requiredFields.fields.begin(),
                requiredFields.fields.end());
        _requiredFields.isRequiredAnd = requiredFields.isRequiredAnd;
        sort(_requiredFields.fields.begin(), _requiredFields.fields.end());
    }
    virtual TermType getTermType() const {return TT_STRING;}

    virtual std::string getTermName() const {return "Term";}

    virtual bool operator == (const Term& term) const;
    bool operator != (const Term& term) const {return !(*this == term);}
    virtual std::string toString() const;
    void formatString(std::stringstream &ss) const;

    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer);
protected:
    build_service::analyzer::Token _token;
    std::string _indexName;
    RequiredFields _requiredFields;
    int32_t _boost;
    std::string _truncateName;
private:
    friend class TermTest;
private:
    HA3_LOG_DECLARE();
};

inline bool operator < (const Term &lhs, const Term &rhs) {
    if (lhs.getWord() < rhs.getWord()) {
        return true;
    } else if (lhs.getWord() > rhs.getWord()) {
        return false;
    }
    if (lhs.getIndexName() < rhs.getIndexName()) {
        return true;
    } else if (lhs.getIndexName() > rhs.getIndexName()) {
        return false;
    }
    return lhs.getTruncateName() < rhs.getTruncateName();
}

inline bool operator > (const Term &lhs, const Term &rhs) {
    return rhs < lhs;
}

HA3_TYPEDEF_PTR(Term);
std::ostream& operator << (std::ostream &os, const Term& term);

END_HA3_NAMESPACE(common);

#endif //ISEARCH_TERM_H
