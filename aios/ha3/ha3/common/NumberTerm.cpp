#include <ha3/common/NumberTerm.h>
#include <sstream>
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;

BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, NumberTerm);

NumberTerm::NumberTerm(int64_t num, const char *indexName, 
                       const RequiredFields &requiredFields, int64_t boost,
                       const string &truncateName) 
    : Term("", indexName, requiredFields, boost, truncateName)
{
    _leftNum = _rightNum = num;
    std::string tokenStr = StringUtil::toString(_leftNum);
    setWord(tokenStr.c_str());    
}

NumberTerm::NumberTerm(int64_t leftNum, bool leftInclusive, int64_t rightNum,
                       bool rightInclusive, const char *indexName, 
                       const RequiredFields &requiredFields, int64_t boost,
                       const string &truncateName)
    : common::Term("", indexName, requiredFields, boost, truncateName)
{
    _leftNum = leftNum;
    _rightNum = rightNum;
    if (!leftInclusive) {
        _leftNum++;
    }
    if (!rightInclusive) {
        _rightNum--;
    }
    std::string tokenStr;
    if (_leftNum == _rightNum) {
        tokenStr = StringUtil::toString(_leftNum);
    } else {
        tokenStr = "[" + StringUtil::toString(_leftNum) + "," + StringUtil::toString(_rightNum) + "]";
    }
    setWord(tokenStr.c_str());
}

NumberTerm::~NumberTerm() { 
}

NumberTerm::NumberTerm(const NumberTerm &other)
    : Term(other)
{
    _leftNum = other._leftNum;
    _rightNum = other._rightNum;
    std::string tokenStr;
    if (_leftNum == _rightNum) {
        tokenStr = StringUtil::toString(_leftNum);
    } else {
        tokenStr = "[" + StringUtil::toString(_leftNum) + "," + StringUtil::toString(_rightNum) + "]";
    }
    setWord(tokenStr.c_str());
}

bool NumberTerm::operator == (const Term& term) const {
    if (&term == this) {
        return true;
    }
    if (term.getTermName() != getTermName()) {
        return false;
    }   
    const NumberTerm &term2 = dynamic_cast<const NumberTerm&>(term);
    if ((_requiredFields.fields.size() != term2._requiredFields.fields.size())
        ||(_requiredFields.isRequiredAnd != term2._requiredFields.isRequiredAnd))
    {
        return false;
    }
    for (size_t i = 0; i < _requiredFields.fields.size(); ++i)
    {
        if (_requiredFields.fields[i] != term2._requiredFields.fields[i]) {
            return false;
        }
    }

    return _leftNum == term2._leftNum 
        && _rightNum == term2._rightNum 
        && _indexName == term2._indexName
        && _token.getText() == term2._token.getText()
        && _boost == term2._boost
        && _truncateName == term2._truncateName;
}

std::string NumberTerm::toString() const {
    std::stringstream ss;
    ss << "NumberTerm: [" << _indexName << ",";
    ss << "[" << getLeftNumber() << ", "
       << getRightNumber() << "|";
    formatString(ss);
    ss << "]";
    return ss.str();
}

void NumberTerm::serialize(autil::DataBuffer &dataBuffer) const
{
    Term::serialize(dataBuffer);
    dataBuffer.write(_leftNum);
    dataBuffer.write(_rightNum);
}

void NumberTerm::deserialize(autil::DataBuffer &dataBuffer)
{
    Term::deserialize(dataBuffer);
    dataBuffer.read(_leftNum);
    dataBuffer.read(_rightNum);
}

END_HA3_NAMESPACE(common);

