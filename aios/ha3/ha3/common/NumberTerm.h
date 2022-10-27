#ifndef ISEARCH_NUMBERTERM_H
#define ISEARCH_NUMBERTERM_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/DataBuffer.h>
#include <ha3/common/Term.h>

BEGIN_HA3_NAMESPACE(common);

class NumberTerm : public Term
{
public:
    NumberTerm(int64_t num, const char *indexName, 
               const RequiredFields &requiredFields,
               int64_t boost = DEFAULT_BOOST_VALUE,
               const std::string &truncateName = "");
    NumberTerm(int64_t leftNum, bool leftInclusive, int64_t rightNum,
               bool rightInclusive, const char *indexName,
               const RequiredFields &requiredFields,
	       int64_t boost = DEFAULT_BOOST_VALUE,
               const std::string &truncateName = "");
    NumberTerm(const NumberTerm &other);
    ~NumberTerm();
public:
    int64_t getLeftNumber() const {return _leftNum;}
    int64_t getRightNumber() const {return _rightNum;}
    virtual TermType getTermType() const {return TT_NUMBER;}
    virtual std::string getTermName() const {return "NumberTerm";}
    virtual Term *clone() {
        return new NumberTerm(*this);
    }
    bool operator == (const Term& term) const;
    std::string toString() const;

    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer);

private:
    int64_t _leftNum;
    int64_t _rightNum;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(NumberTerm);

END_HA3_NAMESPACE(common);

#endif //ISEARCH_NUMBERTERM_H
