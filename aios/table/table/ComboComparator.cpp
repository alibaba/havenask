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
#include "table/ComboComparator.h"

#include <assert.h>
#include <stddef.h>

#include "autil/mem_pool/PoolBase.h"

#include "table/Comparator.h"

namespace table {

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

}
