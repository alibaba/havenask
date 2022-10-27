#include <sstream>
#include <ha3/common/TermQuery.h>
#include <ha3/common/QueryVisitor.h>
#include <ha3/common/ModifyQueryVisitor.h>

using namespace std;

BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, TermQuery);

TermQuery::TermQuery(const Term& term, const std::string &label) : _term(term) {
    setQueryLabelTerm(label);
}

TermQuery::TermQuery(const TermQuery& other)
    : Query(other)
    , _term(other._term)
{
}

TermQuery::~TermQuery() {
}

bool TermQuery::operator == (const Query& query) const {
    if (&query == this) {
        return true;
    }
    if (query.getQueryName() != getQueryName()) {
        return false;
    }
    return (_term == dynamic_cast<const TermQuery&>(query)._term);
}

void TermQuery::accept(QueryVisitor *visitor) const {
    visitor->visitTermQuery(this);
}

void TermQuery::accept(ModifyQueryVisitor *visitor) {
    visitor->visitTermQuery(this);
}

Query *TermQuery::clone() const {
    return new TermQuery(*this);
}

std::string TermQuery::getQueryName() const {
    return "TermQuery";
}

std::string TermQuery::toString() const {
    std::stringstream ss;
    ss << "TermQuery";
    if (!_queryLabel.empty()) {
        ss << "@" << _queryLabel;
    }
    ss << ":[" << _term.toString() << "]";
    return ss.str();
}

void TermQuery::setTerm(const Term& term) {
    _term = term;
}

const Term& TermQuery::getTerm() const {
    return _term;
}

Term& TermQuery::getTerm() {
    return _term;
}

void TermQuery::serialize(autil::DataBuffer &dataBuffer) const
{
    dataBuffer.write(_term);
    serializeMDLandQL(dataBuffer);
}

void TermQuery::deserialize(autil::DataBuffer &dataBuffer)
{
    dataBuffer.read(_term);
    deserializeMDLandQL(dataBuffer);
}

END_HA3_NAMESPACE(common);

