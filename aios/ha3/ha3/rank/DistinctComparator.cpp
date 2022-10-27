#include <ha3/rank/DistinctComparator.h>

BEGIN_HA3_NAMESPACE(rank);
HA3_LOG_SETUP(rank, DistinctComparator);

DistinctComparator::DistinctComparator(
        matchdoc::Reference<DistinctInfo> *distInfoRef, bool flag)
{
    _distInfoRef = distInfoRef;
    _flag = flag;
}

DistinctComparator::~DistinctComparator() {
}

bool DistinctComparator::compare(matchdoc::MatchDoc a, matchdoc::MatchDoc b) const {
    if (_flag) {
        return _distInfoRef->getPointer(b)->getDistinctBoost()
            < _distInfoRef->getPointer(a)->getDistinctBoost();
    } else {
        return _distInfoRef->getPointer(a)->getDistinctBoost()
            < _distInfoRef->getPointer(b)->getDistinctBoost();
    }
}

END_HA3_NAMESPACE(rank);
