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
#include "indexlib/index/common/field_format/spatial/shape/Event.h"
#include "indexlib/index/common/field_format/spatial/shape/Point.h"

namespace indexlib::index {

class EventQueue : private autil::NoCopyable
{
public:
    explicit EventQueue(const std::vector<Point>& P);

    ~EventQueue(void)
    {
        delete[] _sortedEvents;
        delete[] _events;
    }

    Event* Next();

private:
    // qsort compare two events
    static int Compare(const void* v1, const void* v2);

private:
    const size_t _eventCount; // total number of events in array
    size_t _nextIdx;          // index of Next event on queue
    Event* _events;           // array of all events
    Event** _sortedEvents;    // sorted list of event pointers

private:
    AUTIL_LOG_DECLARE();
};

// EventQueue Routines
EventQueue::EventQueue(const std::vector<Point>& P) : _eventCount(2 * P.size()) // 2 vertex events for each edge
{
    _nextIdx = 0;
    _events = (Event*)new Event[_eventCount];
    _sortedEvents = (Event**)new Event*[_eventCount];
    for (size_t i = 0; i < _eventCount; i++) // init _sortedEvents array pointers
        _sortedEvents[i] = &_events[i];

    // Initialize event queue with edge segment endpoints
    for (size_t i = 0; i < P.size(); i++) { // init data for edge i
        _sortedEvents[2 * i]->_edge = i;
        _sortedEvents[2 * i + 1]->_edge = i;
        _sortedEvents[2 * i]->_point = const_cast<Point*>(&(P[i]));
        _sortedEvents[2 * i]->_otherEnd = _sortedEvents[2 * i + 1];
        _sortedEvents[2 * i + 1]->_otherEnd = _sortedEvents[2 * i];
        _sortedEvents[2 * i]->_slseg = _sortedEvents[2 * i + 1]->_slseg = 0;

        const Point* pi1 = (i + 1 < P.size()) ? &(P[i + 1]) : &(P[0]);
        _sortedEvents[2 * i + 1]->_point = const_cast<Point*>(pi1);
        if (Event::XYOrder(&P[i], pi1) < 0) { // determine type
            _sortedEvents[2 * i]->_type = SEG_SIDE::LEFT;
            _sortedEvents[2 * i + 1]->_type = SEG_SIDE::RIGHT;
        } else {
            _sortedEvents[2 * i]->_type = SEG_SIDE::RIGHT;
            _sortedEvents[2 * i + 1]->_type = SEG_SIDE::LEFT;
        }
    }

    // Sort _sortedEvents[] by increasing x and y
    ::qsort(_sortedEvents, _eventCount, sizeof(Event*), Compare);
}

inline Event* EventQueue::Next()
{
    if (_nextIdx >= _eventCount)
        return (Event*)0;
    else
        return _sortedEvents[_nextIdx++];
}

// qsort compare two events
int EventQueue::Compare(const void* v1, const void* v2)
{
    Event** pe1 = (Event**)v1;
    Event** pe2 = (Event**)v2;

    int r = Event::XYOrder((*pe1)->_point, (*pe2)->_point);
    if (r == 0) {
        if ((*pe1)->_type == (*pe2)->_type)
            return 0;
        if ((*pe1)->_type == SEG_SIDE::LEFT)
            return -1;
        else
            return 1;
    } else
        return r;
}
} // namespace indexlib::index
