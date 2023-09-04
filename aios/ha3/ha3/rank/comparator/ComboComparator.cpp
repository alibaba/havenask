/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "ha3/rank/ComboComparator.h"

#include <assert.h>
#include <stddef.h>

#include "autil/Log.h"
#include "autil/mem_pool/PoolBase.h"
#include "ha3/rank/Comparator.h"
#include "matchdoc/MatchDoc.h"

namespace isearch {
namespace rank {
AUTIL_LOG_SETUP(ha3, ComboComparator);

ComboComparator::ComboComparator() {
    _extrDocIdCmp = NULL;
    _extrHashIdCmp = NULL;
    _extrClusterIdCmp = NULL;
}

ComboComparator::~ComboComparator() {
    for (ComparatorVector::iterator it = _cmpVector.begin(); it != _cmpVector.end(); it++) {
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
    for (ComparatorVector::const_iterator it = _cmpVector.begin(); it != _cmpVector.end(); it++) {
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

} // namespace rank
} // namespace isearch
