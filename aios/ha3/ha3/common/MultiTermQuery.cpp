#include <ha3/common/MultiTermQuery.h>
#include <ha3/common/QueryVisitor.h>
#include <ha3/common/ModifyQueryVisitor.h>

BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, MultiTermQuery);

MultiTermQuery::MultiTermQuery(const std::string &label, QueryOperator op)
    : _opExpr(op)
    , _minShoudMatch(1)
{
    setQueryLabelTerm(label);
}

MultiTermQuery::MultiTermQuery(const MultiTermQuery &other)
    : Query(other)
    , _opExpr(other._opExpr)
    , _minShoudMatch(other._minShoudMatch)
{
    for (TermArray::const_iterator it = other._terms.begin();
         it != other._terms.end(); ++it)
    {
        _terms.push_back(TermPtr((*it)->clone()));
    }
    setQueryLabelTerm(other._queryLabel);
}

MultiTermQuery::~MultiTermQuery() {
}

void MultiTermQuery::addTerm(const TermPtr& term) {
    _terms.push_back(term);
}

bool MultiTermQuery::operator == (const Query& query) const {
    if (&query == this) {
        return true;
    }
    if (query.getQueryName() != getQueryName()) {
        return false;
    }
    auto p = dynamic_cast<const MultiTermQuery*>(&query);
    if (!p) {
        return false;
    }
    const auto &multiTermQuery = *p;
    if (getQueryLabel() != multiTermQuery.getQueryLabel()) {
        return false;
    }
    const TermArray &terms2 = multiTermQuery._terms;
    if (_minShoudMatch != multiTermQuery.getMinShouldMatch()) {
        return false;
    }
    if (_opExpr != multiTermQuery.getOpExpr()) {
        return false;
    }
    if (_terms.size() != terms2.size()) {
        return false;
    }
    TermArray::const_iterator it1 = _terms.begin();
    TermArray::const_iterator it2 = terms2.begin();
    for (; it1 != _terms.end(); it1++, it2++)
    {
        if (!( (**it1) == (**it2) )) {
            return false;
        }
    }
    return true;
}

void MultiTermQuery::accept(QueryVisitor *visitor) const {
    visitor->visitMultiTermQuery(this);
}

void MultiTermQuery::accept(ModifyQueryVisitor *visitor) {
    visitor->visitMultiTermQuery(this);
}

Query *MultiTermQuery::clone() const {
    return new MultiTermQuery(*this);
}

std::string MultiTermQuery::toString() const {
    std::stringstream ss;
    ss << "MultiTermQuery";
    if (!_queryLabel.empty()) {
        ss << "@" << _queryLabel;
    }
    ss << ":[" ;
    for (TermArray::const_iterator it = _terms.begin();
         it != _terms.end(); it++)
    {
        ss << (*it)->toString() << ", ";
    }
    ss << "]";
    return ss.str();
}

const MultiTermQuery::TermArray& MultiTermQuery::getTermArray() const {
    return _terms;
}

void MultiTermQuery::serialize(autil::DataBuffer &dataBuffer) const
{
    dataBuffer.write(_terms);
    dataBuffer.write((int8_t)_opExpr);
    dataBuffer.write(_minShoudMatch);
    serializeMDLandQL(dataBuffer);
}

void MultiTermQuery::deserialize(autil::DataBuffer &dataBuffer)
{
    dataBuffer.read(_terms);
    int8_t opExpr = 0;
    dataBuffer.read(opExpr);
    _opExpr = (QueryOperator)opExpr;
    dataBuffer.read(_minShoudMatch);
    deserializeMDLandQL(dataBuffer);
}

END_HA3_NAMESPACE(common);
