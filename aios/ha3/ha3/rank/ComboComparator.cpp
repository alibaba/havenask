#include <ha3/rank/ComboComparator.h>
#include <assert.h>

USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(rank);
HA3_LOG_SETUP(rank, ComboComparator);

ComboComparator::ComboComparator() {
    _extrDocIdCmp = NULL;
    _extrHashIdCmp = NULL;
    _extrClusterIdCmp = NULL;
}

ComboComparator::~ComboComparator() {
    for (ComparatorVector::iterator it = _cmpVector.begin();
         it != _cmpVector.end(); it++)
    {
        POOL_DELETE_CLASS(*it);
    }
    _cmpVector.clear();

    POOL_DELETE_CLASS(_extrDocIdCmp);
    POOL_DELETE_CLASS(_extrHashIdCmp);
    POOL_DELETE_CLASS(_extrClusterIdCmp);
}

void ComboComparator::addComparator(const Comparator *cmp) {
    assert(cmp);
    _cmpVector.push_back(cmp);
}

bool ComboComparator::compare(matchdoc::MatchDoc a, matchdoc::MatchDoc b) const {
    for (ComparatorVector::const_iterator it = _cmpVector.begin();
         it != _cmpVector.end(); it++)
    {
        const Comparator *cmp = *it;
        if (cmp->compare(a, b)) {
            return true;
        } else if (cmp->compare(b, a)) {
            return false;
        }
    }
    return compareDocInfo(a, b);
}

bool ComboComparator::compareDocInfo(matchdoc::MatchDoc a, matchdoc::MatchDoc b) const {
    auto idA = a.getDocId();
    auto idB = b.getDocId();
    if (idA > idB) {
        return true;
    } else if (idA < idB) {
        return false;
    }
    if (_extrClusterIdCmp) {
        if (_extrClusterIdCmp->compare(b, a)) {
            return true;
        } else if (_extrClusterIdCmp->compare(a, b)) {
            return false;
        }
    }
    if (_extrHashIdCmp) {
        if (_extrHashIdCmp->compare(b, a)) {
            return true;
        } else if (_extrHashIdCmp->compare(a, b)) {
            return false;
        }
    }
    if (_extrDocIdCmp) {
        return _extrDocIdCmp->compare(b, a);
    }
    return false;

}

void ComboComparator::setExtrDocIdComparator(Comparator *cmp) {
    if (_extrDocIdCmp != NULL) {
        POOL_DELETE_CLASS(_extrDocIdCmp);
    }
    _extrDocIdCmp = cmp;
}
void ComboComparator::setExtrHashIdComparator(Comparator *cmp) {
    if (_extrHashIdCmp != NULL) {
        POOL_DELETE_CLASS(_extrHashIdCmp);
    }
    _extrHashIdCmp = cmp;
}
void ComboComparator::setExtrClusterIdComparator(Comparator *cmp) {
    if (_extrClusterIdCmp != NULL) {
        POOL_DELETE_CLASS(_extrClusterIdCmp);
    }
    _extrClusterIdCmp = cmp;
}


END_HA3_NAMESPACE(rank);
