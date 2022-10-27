#include <ha3/common/NumberQuery.h>
#include <sstream>
#include <ha3/common/QueryVisitor.h>
#include <ha3/common/ModifyQueryVisitor.h>

using namespace std;
BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, NumberQuery);

NumberQuery::NumberQuery(int64_t leftNumber, bool leftInclusive,
                         int64_t rightNumber, bool rightInclusive,
                         const char *indexName, const RequiredFields &requiredFields,
                         const std::string &label,
                         int64_t boost, const std::string &truncateName)
    : _term(leftNumber, leftInclusive, rightNumber,
            rightInclusive, indexName, requiredFields,
            boost, truncateName)
{
    setQueryLabelTerm(label);
}

NumberQuery::NumberQuery(int64_t num, const char *indexName,
                         const RequiredFields &requiredFields,
                         const std::string &label,
                         int64_t boost, const std::string &truncateName)
    : _term(num, indexName, requiredFields, boost, truncateName)
{
    setQueryLabelTerm(label);
}

NumberQuery::NumberQuery(const NumberTerm& term, const std::string &label)
    : _term(term)
{
    setQueryLabelTerm(label);
}

NumberQuery::NumberQuery(const NumberQuery &other)
    : Query(other)
    , _term(other._term)
{
}

NumberQuery::~NumberQuery() {
}

void NumberQuery::setTerm(const NumberTerm& term) {
    _term = term;
}

bool NumberQuery::operator == (const Query& query) const {
    if (&query == this) {
        return true;
    }
    if (query.getQueryName() != getQueryName()) {
        return false;
    }
    return (_term == dynamic_cast<const NumberQuery&>(query)._term);
}

std::string NumberQuery::toString() const {
    std::stringstream ss;
    ss << "NumberQuery";
    if (!_queryLabel.empty()) {
        ss << "@" << _queryLabel;
    }
    ss << ":[" << _term.toString() << "]";
    return ss.str();
}

void NumberQuery::accept(QueryVisitor *visitor) const {
    visitor->visitNumberQuery(this);
}

void NumberQuery::accept(ModifyQueryVisitor *visitor) {
    visitor->visitNumberQuery(this);
}

Query *NumberQuery::clone() const {
    return new NumberQuery(*this);
}

bool NumberQuery::removeQuery(const Query *query) {
    assert(false);
    return false;
}

Query* NumberQuery::rewriteQuery() {
    assert(false);
    return NULL;
}

const NumberTerm& NumberQuery::getTerm() const {
    return _term;
}

NumberTerm& NumberQuery::getTerm() {
    return _term;
}

END_HA3_NAMESPACE(common);

