#include <ha3/sql/rank/ComboComparator.h>

BEGIN_HA3_NAMESPACE(sql);

ComboComparator::ComboComparator()
{
}

ComboComparator::~ComboComparator() {
    for (ComparatorVector::iterator it = _cmpVector.begin();
         it != _cmpVector.end(); it++)
    {
        POOL_DELETE_CLASS(*it);
    }
    _cmpVector.clear();
}

void ComboComparator::addComparator(const Comparator *cmp) {
    assert(cmp);
    _cmpVector.push_back(cmp);
}

bool ComboComparator::compare(Row a, Row b) const {
    for (size_t i = 0; i < _cmpVector.size(); ++i) {
        if (_cmpVector[i]->compare(a, b)) {
            return true;
        } else if (_cmpVector[i]->compare(b, a)) {
            return false;
        }
    }
    return false;
}

END_HA3_NAMESPACE(sql);
