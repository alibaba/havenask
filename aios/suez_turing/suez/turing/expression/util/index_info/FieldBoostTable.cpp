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
#include "suez/turing/expression/util/FieldBoostTable.h"

#include <ext/alloc_traits.h>
#include <iosfwd>

#include "alog/Logger.h"

using namespace std;

namespace suez {
namespace turing {

AUTIL_LOG_SETUP(expression, FieldBoostTable);

FieldBoostTable::FieldBoostTable() {}

FieldBoostTable::~FieldBoostTable() {}

fieldboost_t FieldBoostTable::getFieldBoost(indexid_t indexid, int32_t fieldPosition) const {
    if (indexid >= _boostTable.size() || fieldPosition < 0 || fieldPosition >= FieldBoost::FIELDS_LIMIT) {
        AUTIL_LOG(WARN, "Out of range error. indexid[%d], fieldPosition[%d]", indexid, fieldPosition);
        return (fieldboost_t)0;
    }
    return _boostTable[indexid][fieldPosition];
}

fieldboost_t FieldBoostTable::getFieldBoostSum(indexid_t indexid, fieldbitmap_t bitmap) const {
    fieldbitmap_t mask = (fieldbitmap_t)1;
    fieldbitmap_t bmRemain = bitmap;
    fieldboost_t scoreSum = 0;
    int idx = 0;
    const FieldBoost &fbb = _boostTable[indexid];
    while (bmRemain) {
        if (bmRemain & mask) {
            scoreSum += fbb[idx];
        }
        bmRemain >>= 1;
        idx++;
    }
    return scoreSum;
}

fieldboost_t FieldBoostTable::getFieldBoostMax(indexid_t indexid, fieldbitmap_t bitmap) const {
    fieldbitmap_t mask = (fieldbitmap_t)1;
    fieldbitmap_t bmRemain = bitmap;
    fieldboost_t scoreMax = 0;
    int idx = 0;
    const FieldBoost &fbb = _boostTable[indexid];
    while (bmRemain) {
        if ((bmRemain & mask) && (fbb[idx]) > scoreMax) {
            scoreMax = fbb[idx];
        }
        bmRemain >>= 1;
        idx++;
    }
    return scoreMax;
}

void FieldBoostTable::setFieldBoost(indexid_t indexid, const FieldBoost &fieldbitboost) {
    if (indexid >= _boostTable.size()) {
        _boostTable.resize(indexid + 1);
    }
    _boostTable[indexid] = fieldbitboost;
}

void FieldBoostTable::setFieldBoostValue(indexid_t indexid, int32_t fieldPosition, fieldboost_t boostValue) {
    if (indexid >= _boostTable.size()) {
        _boostTable.resize(indexid + 1);
    }
    if (fieldPosition < 0 || fieldPosition >= FieldBoost::FIELDS_LIMIT) {
        AUTIL_LOG(WARN, "fieldPosition is out of FieldBoost::FIELDS_LIMIT");
        return;
    }
    _boostTable[indexid][fieldPosition] = boostValue;
}

void FieldBoostTable::clear() { _boostTable.clear(); }

} // namespace turing
} // namespace suez
