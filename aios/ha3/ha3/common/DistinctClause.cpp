#include <ha3/common/DistinctClause.h>

using namespace std;

BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, DistinctClause);

DistinctClause::DistinctClause() { 
}

DistinctClause::~DistinctClause() {
    clearDistinctDescriptions();
}

const vector<DistinctDescription*>& 
DistinctClause::getDistinctDescriptions() const {
    return _distDescriptions;
}

const DistinctDescription* 
DistinctClause::getDistinctDescription(uint32_t pos) const {
    if (pos >= (uint32_t)_distDescriptions.size()) {
        return NULL;
    }
    return _distDescriptions[pos];
}

DistinctDescription* 
DistinctClause::getDistinctDescription(uint32_t pos) {
    if (pos >= (uint32_t)_distDescriptions.size()) {
        return NULL;
    }
    return _distDescriptions[pos];
}

void DistinctClause::addDistinctDescription(
        DistinctDescription *distDescription) 
{
    _distDescriptions.push_back(distDescription);
}

void DistinctClause::clearDistinctDescriptions() {
    for (vector<DistinctDescription*>::iterator it = _distDescriptions.begin();
         it != _distDescriptions.end(); ++it)
    {
        delete (*it);
    }
    _distDescriptions.clear();
}

uint32_t DistinctClause::getDistinctDescriptionsCount() const {
    return _distDescriptions.size();
}

void DistinctClause::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(_originalString);
    dataBuffer.write(_distDescriptions);
}

void DistinctClause::deserialize(autil::DataBuffer &dataBuffer) {
    dataBuffer.read(_originalString);
    dataBuffer.read(_distDescriptions);
}

string DistinctClause::toString() const {
    string distinctClauseStr;
    for(size_t i = 0; i < _distDescriptions.size(); i++) {
        distinctClauseStr.append("[");
        if (_distDescriptions[i]) {
            distinctClauseStr.append(_distDescriptions[i]->toString());
        } else {
            distinctClauseStr.append("none_dist");
        }
        distinctClauseStr.append("]");
    }
    return distinctClauseStr;
}

END_HA3_NAMESPACE(common);

