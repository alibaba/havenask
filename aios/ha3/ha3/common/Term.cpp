#include <ha3/common/Term.h>

using namespace std;

BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, Term);

bool RequiredFields::operator==(const RequiredFields &other) const {
    if ((fields.size() != other.fields.size())
        ||(isRequiredAnd != other.isRequiredAnd))
    {
        return false;
    }
    for (size_t i = 0; i < fields.size(); ++i)
    {
        if (fields[i] != other.fields[i]) {
            return false;
        }
    }
    return true;
}

bool Term::operator == (const Term& term) const {
    return  _token == term._token
        && _indexName == term._indexName
        && _boost == term._boost
        && _truncateName == term._truncateName
        && _requiredFields == term._requiredFields;
}

std::string Term::toString() const {
    std::stringstream ss;
    ss << "Term:[" << _indexName << "|";
    formatString(ss);
    ss << "]";
    return ss.str();
}

void Term::formatString(std::stringstream &ss) const {
    bool isFirstField = true;
    string delimiter = _requiredFields.isRequiredAnd ? " and " : " or ";
    for (vector<string>::const_iterator it = _requiredFields.fields.begin();
         it != _requiredFields.fields.end(); ++it)
    {
        if (isFirstField) {
            ss << *it;
            isFirstField = false;
        } else {
            ss << delimiter << *it;
        }
    }
    ss << "|";

    ss << _token.getNormalizedText().c_str() << "|" << _boost << "|" <<
        _truncateName;
}

std::ostream& operator <<(std::ostream &os, const Term& term) {
    return os << term.toString();
}

void Term::serialize(autil::DataBuffer &dataBuffer) const
{
    dataBuffer.write(_token);
    dataBuffer.write(_indexName);
    dataBuffer.write(_requiredFields.fields);
    dataBuffer.write(_requiredFields.isRequiredAnd);
    dataBuffer.write(_boost);
    dataBuffer.write(_truncateName);
}

void Term::deserialize(autil::DataBuffer &dataBuffer)
{
    dataBuffer.read(_token);
    dataBuffer.read(_indexName);
    dataBuffer.read(_requiredFields.fields);
    dataBuffer.read(_requiredFields.isRequiredAnd);
    dataBuffer.read(_boost);
    dataBuffer.read(_truncateName);
}

END_HA3_NAMESPACE(common);
