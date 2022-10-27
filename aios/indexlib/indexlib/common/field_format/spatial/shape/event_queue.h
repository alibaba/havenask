#ifndef __INDEXLIB_EVENT_QUEUE_H
#define __INDEXLIB_EVENT_QUEUE_H

#include <vector>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/common/field_format/spatial/shape/point.h"
#include "indexlib/common/field_format/spatial/shape/event.h"

IE_NAMESPACE_BEGIN(common);

// the EventQueue is a presorted array (no insertions needed)
class EventQueue {
private:
    const size_t ne;        // total number of events in array
    size_t       ix;        // index of Next event on queue
    Event*       Edata;     // array of all events
    Event**      Eq;        // sorted list of event pointers

public:
    EventQueue(const std::vector<Point> &P);    // constructor

    ~EventQueue(void)                // destructor
    {
        delete[] Eq;
        delete[] Edata;
    }

    Event*   Next();                    // Next event on queue

private:
    // qsort compare two events
    static int E_Compare(const void *v1, const void *v2);
private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index, EventQueue);

// EventQueue Routines
EventQueue::EventQueue( const std::vector<Point> &P )
    : ne(2 * P.size())  // 2 vertex events for each edge
{
    ix = 0;
    Edata = (Event*)new Event[ne];
    Eq = (Event**)new Event*[ne];
    for (size_t i=0; i < ne; i++)          // init Eq array pointers
        Eq[i] = &Edata[i];

    // Initialize event queue with edge segment endpoints
    for (size_t i=0; i < P.size(); i++) {       // init data for edge i
        Eq[2*i]->edge = i;
        Eq[2*i+1]->edge = i;
        Eq[2*i]->eV   = const_cast<Point*>(&(P[i]));
        Eq[2*i]->otherEnd = Eq[2*i+1];
        Eq[2*i+1]->otherEnd = Eq[2*i];
        Eq[2*i]->seg = Eq[2*i+1]->seg = 0;

        const Point *pi1 = (i+1 < P.size()) ? &(P[i+1]):&(P[0]);
        Eq[2*i+1]->eV = const_cast<Point*>(pi1);
        if (Event::xyorder( &P[i], pi1) < 0) { // determine type
            Eq[2*i]->type   = LEFT;
            Eq[2*i+1]->type = RIGHT;
        }
        else {
            Eq[2*i]->type   = RIGHT;
            Eq[2*i+1]->type = LEFT;
        }
    }

    // Sort Eq[] by increasing x and y
    ::qsort(Eq, ne, sizeof(Event *), E_Compare);
}

inline Event* EventQueue::Next()
{
    if (ix >= ne)
        return (Event*)0;
    else
        return Eq[ix++];
}

// qsort compare two events
int EventQueue::E_Compare(const void *v1, const void *v2)
{
    Event**    pe1 = (Event**)v1;
    Event**    pe2 = (Event**)v2;

    int r = Event::xyorder( (*pe1)->eV, (*pe2)->eV );
    if (r == 0) {
        if ((*pe1)->type == (*pe2)->type) return 0;
        if ((*pe1)->type == LEFT) return -1;
        else return 1;
    } else
        return r;
}

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_EVENT_QUEUE_H
