#include <ha3/common/AggregateClause.h>
#include <assert.h>

using namespace std;

BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, AggregateClause);

AggregateClause::AggregateClause() { 
}

AggregateClause::~AggregateClause() { 
    for (vector<AggregateDescription*>::iterator it = _aggDescriptions.begin();
         it != _aggDescriptions.end(); it++)
    {
        delete *it;
    }
    _aggDescriptions.clear();
}

const std::vector<AggregateDescription*>& AggregateClause::getAggDescriptions() const {
    return _aggDescriptions;
}

void AggregateClause::addAggDescription(AggregateDescription *aggDescription) {
    if (aggDescription) {
        _aggDescriptions.push_back(aggDescription);
    }
}

bool AggregateClause::removeAggDescription(AggregateDescription *aggDescription) {
    assert(false);
    return false;
}

void AggregateClause::clearAggDescriptions() {
    _aggDescriptions.clear();
}

void AggregateClause::serialize(autil::DataBuffer &dataBuffer) const
{
    dataBuffer.write(_originalString);
    dataBuffer.write(_aggDescriptions);
}

void AggregateClause::deserialize(autil::DataBuffer &dataBuffer)
{
    dataBuffer.read(_originalString);
    dataBuffer.read(_aggDescriptions);
}

string AggregateClause::toString() const {
    string aggClauseStr;
    size_t descCount = _aggDescriptions.size();
    for (size_t i = 0; i < descCount; i++) {
        assert(_aggDescriptions[i]);
        aggClauseStr.append("[");
        aggClauseStr.append(_aggDescriptions[i]->toString());
        aggClauseStr.append("]");
    }
    return aggClauseStr;
}

END_HA3_NAMESPACE(common);
