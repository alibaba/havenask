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

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/index/common/field_format/spatial/shape/Point.h"

namespace indexlib::index {
class SLseg;
class EventQueue;
class SweepLine;

enum class SEG_SIDE { LEFT, RIGHT };

class Event : private autil::NoCopyable
{
    Event() = default;
    ~Event() = default;

private:
    static int XYOrder(const Point* p1, const Point* p2);

private:
    int _edge;        // polygon edge i is V[i] to V[i+1]
    SEG_SIDE _type;   // event type: LEFT or RIGHT vertex
    Point* _point;    // event vertex
    SLseg* _slseg;    // segment in tree
    Event* _otherEnd; // segment is [this, otherEnd]

    friend class EventQueue;
    friend class SweepLine;

private:
    AUTIL_LOG_DECLARE();
};

// xyorder(): determines the xy lexicographical order of two points
//      returns: (+1) if p1 > p2; (-1) if p1 < p2; and 0 if equal
inline int Event::XYOrder(const Point* p1, const Point* p2)
{
    // test the x-coord first
    if (p1->GetX() > p2->GetX())
        return 1;
    if (p1->GetX() < p2->GetX())
        return (-1);
    // and test the y-coord second
    if (p1->GetY() > p2->GetY())
        return 1;
    if (p1->GetY() < p2->GetY())
        return (-1);
    // when you exclude all other possibilities, what remains is...
    return 0; // they are the same point
}

} // namespace indexlib::index
