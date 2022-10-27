#include "indexlib/common/field_format/spatial/shape/point.h"
#include "indexlib/common/field_format/spatial/shape/event.h"
#include "indexlib/common/field_format/spatial/shape/event_queue.h"
#include "indexlib/common/field_format/spatial/shape/slseg.h"
#include "indexlib/common/field_format/spatial/shape/sweep_line.h"

using namespace std;

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, SweepLine);

SLseg* SweepLine::Add( Event* E )
{
    // fill in SLseg element data
    SLseg* s = new SLseg;
    s->edge  = E->edge;
    E->seg = s;

    // if it is being added, then it must be a LEFT edge event
    // but need to determine which endpoint is the left one
    const Point* v1 = &(Pn[s->edge]);
    const Point* eN = (s->edge+1 < (int)Pn.size() ? &(Pn[s->edge+1]):&(Pn[0]));
    const Point* v2 = eN;
    if (Event::xyorder( v1, v2) < 0) { // determine which is leftmost
        s->lP = v1;
        s->rP = v2;
    }
    else {
        s->rP = v1;
        s->lP = v2;
    }
    s->above = (SLseg*)0;
    s->below = (SLseg*)0;

    // add a node to the balanced binary tree
    Tnode* nd = Tree.Insert(s);
    Tnode* nx = Tree.Next(nd);
    Tnode* np = Tree.Prev(nd);

    if (nx != (Tnode*)0) {
        s->above = (SLseg*)nx->Data();
        s->above->below = s;
    }
    if (np != (Tnode*)0) {
        s->below = (SLseg*)np->Data();
        s->below->above = s;
    }
    //Tree.DumpTree();
    return s;
}

void SweepLine::Remove( SLseg* s )
{
    // remove the node from the balanced binary tree
    //Tree.DumpTree();
    Tnode* nd = Tree.Search(s);
    if (nd == (Tnode*)0) {
        IE_LOG(ERROR, "IsSimplePolygon internal error:"
               "  attempt to remove segment [%lf %lf, %lf %lf]not in tree",
               s->lP->GetX(), s->lP->GetY(), s->rP->GetX(), s->rP->GetY());
        return;
    }

    // get the above and below segments pointing to each other
    Tnode* nx = Tree.Next(nd);
    if (nx != (Tnode*)0) {
        SLseg* sx = (SLseg*)(nx->Data());
        sx->below = s->below;
    }
    Tnode* np = Tree.Prev(nd);
    if (np != (Tnode*)0) {
        SLseg* sp = (SLseg*)(np->Data());
        sp->above = s->above;
    }
    Tree.Delete(nd->Key());       // now can safely remove it
    delete s;  // note:  s == nd->Data()
}

// test intersect of 2 segments and return: 0=none, 1=intersect
bool SweepLine::Intersect( SLseg* s1, SLseg* s2)
{
    if (s1 == (SLseg*)0 || s2 == (SLseg*)0)
        return false;      // no intersect if either segment doesn't exist

    // check for consecutive edges in polygon
    int e1 = s1->edge;
    int e2 = s2->edge;
    if (((int)((e1+1) % nv) == e2) || (e1 == (int)((e2+1) % nv)))
        return false;      // no non-simple intersect since consecutive

    // test for existence of an intersect point
    double lsign, rsign;
    lsign = SLseg::IsLeft(s1->lP, s1->rP, s2->lP);    // s2 left point sign
    rsign = SLseg::IsLeft(s1->lP, s1->rP, s2->rP);    // s2 right point sign
    if (lsign * rsign > 0) // s2 endpoints have same sign relative to s1
        return false;      // => on same side => no intersect is possible

    lsign = SLseg::IsLeft(s2->lP, s2->rP, s1->lP);    // s1 left point sign
    rsign = SLseg::IsLeft(s2->lP, s2->rP, s1->rP);    // s1 right point sign
    if (lsign * rsign > 0) // s1 endpoints have same sign relative to s2
        return false;      // => on same side => no intersect is possible

    // the segments s1 and s2 straddle each other
    return true;           // => an intersect exists
}

bool SweepLine::IsSimplePolygon()
{
    EventQueue  Eq(Pn);
    SweepLine   SL(Pn);
    Event*      e;                 // the current event
    SLseg*      s;                 // the current SL segment

    // This loop processes all events in the sorted queue
    // Events are only left or right vertices since
    // No new events will be added (an intersect => Done)
    while ((e = Eq.Next())) {      // while there are events
        if (e->type == LEFT) {     // process a left vertex
            //IE_LOG(ERROR, "add edge:[%d]", e->edge);
            s = SL.Add(e);         // add it to the sweep line
            if (SL.Intersect( s, s->above))
                return false;          // Pn is NOT simple
            if (SL.Intersect( s, s->below))
                return false;          // Pn is NOT simple
        }
        else {                     // process a right vertex
            s = e->otherEnd->seg;
            if (SL.Intersect( s->above, s->below))
                return true;          // Pn is NOT simple
            // IE_LOG(ERROR, "remove edge:[%d]", s->edge);
            // IE_LOG(ERROR, "event:[edge:%d, type:%d, (%lf, %lf)]",
            //               e->otherEnd->edge, e->otherEnd->type, e->otherEnd->eV->GetX(),
            //       e->otherEnd->eV->GetY());

            SL.Remove(s);     // remove it from the sweep line
        }
    }
    return true;      // Pn is simple
}

IE_NAMESPACE_END(common);

