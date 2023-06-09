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
#pragma once

#include "indexlib/index/common/field_format/spatial/shape/Point.h"
#include "indexlib/util/Comparable.h"

namespace indexlib::index {
class SweepLine;
class SLseg : public indexlib::util::Comparable<SLseg*>
{
    SLseg() : indexlib::util::Comparable<SLseg*>(this) {}
    ~SLseg() = default;

    int key() { return _edge; }
    // return true if 'this' is below 'a'
    bool operator<(const SLseg& a)
    {
        if (_lP->GetX() <= a._lP->GetX()) {
            double s = IsLeft(_lP, _rP, a._lP);
            if (s != 0)
                return s > 0;
            else {
                if (_lP->GetX() == _rP->GetX()) // special case of vertical line.
                    return _lP->GetY() < a._lP->GetY();
                else {
                    double ret = IsLeft(_lP, _rP, a._rP);
                    if (ret != 0) {
                        return IsLeft(_lP, _rP, a._rP) > 0;
                    }
                    return _edge < a._edge;
                }
            }
        } else {
            double s = IsLeft(a._lP, a._rP, _lP);
            if (s != 0)
                return s < 0;
            else {
                double ret = IsLeft(a._lP, a._rP, _rP);
                if (ret != 0) {
                    return IsLeft(a._lP, a._rP, _rP) < 0;
                }
                return _edge < a._edge;
            }
        }
    }

    bool operator==(const SLseg& a) { return this->_edge == a._edge; }

    indexlib::util::cmp_t Compare(SLseg* key) const override
    {
        return (*key == *this) ? indexlib::util::EQ_CMP
                               : ((*key < *this) ? indexlib::util::MIN_CMP : indexlib::util::MAX_CMP);
    }

public:
    static double IsLeft(const Point* P0, const Point* P1, const Point* P2);

public:
    friend class SweepLine;

private:
    int _edge;        // polygon edge i is V[i] to V[i+1]
    const Point* _lP; // leftmost vertex point
    const Point* _rP; // rightmost vertex point
    SLseg* _above;    // segment above this one
    SLseg* _below;    // segment below this one
};

inline double SLseg::IsLeft(const Point* P0, const Point* P1, const Point* P2)
{
    return (P1->GetX() - P0->GetX()) * (P2->GetY() - P0->GetY()) -
           (P2->GetX() - P0->GetX()) * (P1->GetY() - P0->GetY());
}
} // namespace indexlib::index
