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
#include "indexlib/index/common/field_format/spatial/shape/SweepLine.h"

#include "indexlib/index/common/field_format/spatial/shape/Event.h"
#include "indexlib/index/common/field_format/spatial/shape/EventQueue.h"
#include "indexlib/index/common/field_format/spatial/shape/Point.h"
#include "indexlib/index/common/field_format/spatial/shape/SLseg.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, SweepLine);

SLseg* SweepLine::Add(Event* E)
{
    // fill in SLseg element data
    SLseg* s = new SLseg;
    s->_edge = E->_edge;
    E->_slseg = s;

    // if it is being added, then it must be a LEFT edge event
    // but need to determine which endpoint is the left one
    const Point* v1 = &(_pointVec[s->_edge]);
    const Point* eN = (s->_edge + 1 < (int)_pointVec.size() ? &(_pointVec[s->_edge + 1]) : &(_pointVec[0]));
    const Point* v2 = eN;
    if (Event::XYOrder(v1, v2) < 0) { // determine which is leftmost
        s->_lP = v1;
        s->_rP = v2;
    } else {
        s->_rP = v1;
        s->_lP = v2;
    }
    s->_above = (SLseg*)0;
    s->_below = (SLseg*)0;

    // add a node to the balanced binary tree
    Tnode* nd = _tree.Insert(s);
    Tnode* nx = _tree.Next(nd);
    Tnode* np = _tree.Prev(nd);

    if (nx != (Tnode*)0) {
        s->_above = (SLseg*)nx->Data();
        s->_above->_below = s;
    }
    if (np != (Tnode*)0) {
        s->_below = (SLseg*)np->Data();
        s->_below->_above = s;
    }
    // Tree.DumpTree();
    return s;
}

void SweepLine::Remove(SLseg* s)
{
    // remove the node from the balanced binary tree
    // Tree.DumpTree();
    Tnode* nd = _tree.Search(s);
    if (nd == (Tnode*)0) {
        AUTIL_LOG(ERROR,
                  "IsSimplePolygon internal error:"
                  "  attempt to remove segment [%lf %lf, %lf %lf]not in tree",
                  s->_lP->GetX(), s->_lP->GetY(), s->_rP->GetX(), s->_rP->GetY());
        return;
    }

    // get the above and below segments pointing to each other
    Tnode* nx = _tree.Next(nd);
    if (nx != (Tnode*)0) {
        SLseg* sx = (SLseg*)(nx->Data());
        sx->_below = s->_below;
    }
    Tnode* np = _tree.Prev(nd);
    if (np != (Tnode*)0) {
        SLseg* sp = (SLseg*)(np->Data());
        sp->_above = s->_above;
    }
    _tree.Delete(nd->Key()); // now can safely remove it
    delete s;                // note:  s == nd->Data()
}

// test intersect of 2 segments and return: 0=none, 1=intersect
bool SweepLine::Intersect(SLseg* s1, SLseg* s2)
{
    if (s1 == (SLseg*)0 || s2 == (SLseg*)0)
        return false; // no intersect if either segment doesn't exist

    // check for consecutive edges in polygon
    int e1 = s1->_edge;
    int e2 = s2->_edge;
    if (((int)((e1 + 1) % _verticeCnt) == e2) || (e1 == (int)((e2 + 1) % _verticeCnt)))
        return false; // no non-simple intersect since consecutive

    // test for existence of an intersect point
    double lsign, rsign;
    lsign = SLseg::IsLeft(s1->_lP, s1->_rP, s2->_lP); // s2 left point sign
    rsign = SLseg::IsLeft(s1->_lP, s1->_rP, s2->_rP); // s2 right point sign
    if (lsign * rsign > 0)                            // s2 endpoints have same sign relative to s1
        return false;                                 // => on same side => no intersect is possible

    lsign = SLseg::IsLeft(s2->_lP, s2->_rP, s1->_lP); // s1 left point sign
    rsign = SLseg::IsLeft(s2->_lP, s2->_rP, s1->_rP); // s1 right point sign
    if (lsign * rsign > 0)                            // s1 endpoints have same sign relative to s2
        return false;                                 // => on same side => no intersect is possible

    // the segments s1 and s2 straddle each other
    return true; // => an intersect exists
}

bool SweepLine::IsSimplePolygon()
{
    EventQueue Eq(_pointVec);
    SweepLine SL(_pointVec);
    Event* e; // the current event
    SLseg* s; // the current SL segment

    // This loop processes all events in the sorted queue
    // Events are only left or right vertices since
    // No new events will be added (an intersect => Done)
    while ((e = Eq.Next())) {             // while there are events
        if (e->_type == SEG_SIDE::LEFT) { // process a left vertex
            // AUTIL_LOG(ERROR, "add edge:[%d]", e->edge);
            s = SL.Add(e); // add it to the sweep line
            if (SL.Intersect(s, s->_above))
                return false; // _pointVec is NOT simple
            if (SL.Intersect(s, s->_below))
                return false; // _pointVec is NOT simple
        } else {              // process a right vertex
            s = e->_otherEnd->_slseg;
            if (SL.Intersect(s->_above, s->_below))
                return true; // _pointVec is NOT simple
            // AUTIL_LOG(ERROR, "remove edge:[%d]", s->edge);
            // AUTIL_LOG(ERROR, "event:[edge:%d, type:%d, (%lf, %lf)]",
            //               e->otherEnd->edge, e->otherEnd->type, e->otherEnd->eV->GetX(),
            //       e->otherEnd->eV->GetY());

            SL.Remove(s); // remove it from the sweep line
        }
    }
    return true; // _pointVec is simple
}
} // namespace indexlib::index
