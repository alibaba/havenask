#include <ha3/rank/GradeComparator.h>

BEGIN_HA3_NAMESPACE(rank);
HA3_LOG_SETUP(rank, GradeComparator);

GradeComparator::GradeComparator(
        matchdoc::Reference<DistinctInfo> *distInfoRef, bool flag)
{
    _distInfoRef = distInfoRef;
    _flag = flag;
}

GradeComparator::~GradeComparator() {
}
bool GradeComparator::compare(matchdoc::MatchDoc a, matchdoc::MatchDoc b) const {
    if (_flag) {
        return _distInfoRef->getReference(b).getGradeBoost()
            < _distInfoRef->getReference(a).getGradeBoost();
    } else {
        return _distInfoRef->getReference(a).getGradeBoost()
            < _distInfoRef->getReference(b).getGradeBoost();
    }
}

END_HA3_NAMESPACE(rank);
