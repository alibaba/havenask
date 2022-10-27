#ifndef __INDEXLIB_EVENT_H
#define __INDEXLIB_EVENT_H

#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/common/field_format/spatial/shape/point.h"

IE_NAMESPACE_BEGIN(common);

class SLseg;
class EventQueue;
class SweepLine;

enum SEG_SIDE { LEFT, RIGHT };

class Event;
// Event element data struct
class Event {
public:
    friend class EventQueue;
    friend class SweepLine;

private:
    int      edge;         // polygon edge i is V[i] to V[i+1]
    enum SEG_SIDE type;    // event type: LEFT or RIGHT vertex
    Point*   eV;           // event vertex
    SLseg* seg;   // segment in tree
    Event* otherEnd;       // segment is [this, otherEnd]

private:
    static int xyorder( const Point* p1, const Point* p2 );
};

// xyorder(): determines the xy lexicographical order of two points
//      returns: (+1) if p1 > p2; (-1) if p1 < p2; and 0 if equal
inline int Event::xyorder( const Point* p1, const Point* p2 )
{
    // test the x-coord first
    if (p1->GetX() > p2->GetX()) return 1;
    if (p1->GetX() < p2->GetX()) return (-1);
    // and test the y-coord second
    if (p1->GetY() > p2->GetY()) return 1;
    if (p1->GetY() < p2->GetY()) return (-1);
    // when you exclude all other possibilities, what remains is...
    return 0;  // they are the same point
}

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_EVENT_H
