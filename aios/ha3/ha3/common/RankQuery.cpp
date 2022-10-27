#include <sstream>
#include <ha3/common/RankQuery.h>
#include <ha3/common/QueryVisitor.h>
#include <ha3/common/ModifyQueryVisitor.h>

using namespace std;

BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, RankQuery);


RankQuery::RankQuery(const std::string &label) {
    setQueryLabelBinary(label);
}

RankQuery::RankQuery(const RankQuery &other)
    : Query(other)
    , _rankBoosts(other._rankBoosts)
{
}

RankQuery::~RankQuery() {
}

bool RankQuery::operator == (const Query& query) const {
    if (&query == this) {
        return true;
    }
    if (query.getQueryName() != getQueryName()) {
        return false;
    }
    const QueryVector &children2 = dynamic_cast<const RankQuery&>(query)._children;

    if (_children.size() != children2.size()) {
        return false;
    }
    QueryVector::const_iterator it1 = _children.begin();
    QueryVector::const_iterator it2 = children2.begin();
    for (; it1 != _children.end(); it1++, it2++)
    {
        if (!( *(*it1) == *(*it2) )) {
            return false;
        }
    }
    return true;
}

void RankQuery::addQuery(QueryPtr queryPtr) {
    _children.push_back(queryPtr);
    _rankBoosts.push_back(0);
}

void RankQuery::addQuery(QueryPtr queryPtr, uint32_t rankBoost) {
    _children.push_back(queryPtr);
    _rankBoosts.push_back(rankBoost);
}

void RankQuery::accept(QueryVisitor *visitor) const {
    visitor->visitRankQuery(this);
}

void RankQuery::accept(ModifyQueryVisitor *visitor) {
    visitor->visitRankQuery(this);
}

Query *RankQuery::clone() const {
    return new RankQuery(*this);
}

void RankQuery::setRankBoost(uint32_t rankBoost, uint32_t pos) {
    assert(pos < _rankBoosts.size());
    _rankBoosts[pos] = rankBoost;
}

uint32_t RankQuery::getRankBoost(uint32_t pos) const {
    assert(pos < _rankBoosts.size());
    return _rankBoosts[pos];
}

void RankQuery::serialize(autil::DataBuffer &dataBuffer) const
{
    Query::serialize(dataBuffer);
    dataBuffer.write(_rankBoosts);
}

void RankQuery::deserialize(autil::DataBuffer &dataBuffer)
{
    Query::deserialize(dataBuffer);
    dataBuffer.read(_rankBoosts);
}

END_HA3_NAMESPACE(common);
