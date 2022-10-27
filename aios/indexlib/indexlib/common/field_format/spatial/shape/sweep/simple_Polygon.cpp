// ===================================================================
// simple_Polygon.cpp - Check if a polygon is simple
// Written by Dan Sunday (2001) (http://geomalgorithms.com/a09-_intersect-3.html)
// Modified by Glenn Burkhardt (2014) to integrate it with the AVL balanced tree
// Modified by Glenn Burkhardt (2015) to improve the sweepline segment comparison test
 

// Copyright 2001, softSurfer (www.softsurfer.com)
// This code may be freely used and modified for any purpose
// providing that this copyright notice is included with it.
// SoftSurfer makes no warranty for this code, and cannot be held
// liable for any real or imagined damage resulting from its use.
// Users of this code must verify correctness for their application.

// http://geomalgorithms.com/a09-_intersect-3.html#Simple-Polygons

#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <stdexcept>
#include "Avl.h"
#include "simple_polygon.h"

namespace SIMPLEPOLYGON {

// Assume that classes are already given for the objects:
//    Point with 2D coordinates {float x, y;}
//    Polygon with n vertices {int n; Point *V;} with V[n]=V[0]
//    Tnode is a node element structure for a BBT
//    BBT is a class for a Balanced Binary Tree
//        such as an AVL, a 2-3, or a red-black tree
//===================================================================

enum SEG_SIDE { LEFT, RIGHT };

// xyorder(): determines the xy lexicographical order of two points
//      returns: (+1) if p1 > p2; (-1) if p1 < p2; and 0 if equal
int xyorder( Point* p1, Point* p2 )
{
    // test the x-coord first
    if (p1->x > p2->x) return 1;
    if (p1->x < p2->x) return (-1);
    // and test the y-coord second
    if (p1->y > p2->y) return 1;
    if (p1->y < p2->y) return (-1);
    // when you exclude all other possibilities, what remains is...
    return 0;  // they are the same point
}

class SLseg;

// isLeft(): tests if point P2 is Left|On|Right of the line P0 to P1.
//      returns: >0 for left, 0 for on, and <0 for right of the line.
//      (see the January 2001 Algorithm on Area of Triangles)
inline double
isLeft( const Point* P0, const Point* P1, const Point* P2 )
{
    return (P1->x - P0->x)*(P2->y - P0->y) - (P2->x - P0->x)*(P1->y - P0->y);
}
//===================================================================
 
// EventQueue Class and friends

// Event element data struct
typedef struct _event Event;
struct _event {
    int      edge;         // polygon edge i is V[i] to V[i+1]
    enum SEG_SIDE type;    // event type: LEFT or RIGHT vertex
    Point*   eV;           // event vertex
    SLseg* seg;   // segment in tree
    Event* otherEnd;       // segment is [this, otherEnd]
};

int E_compare( const void* v1, const void* v2 ) // qsort compare two events
{
    Event**    pe1 = (Event**)v1;
    Event**    pe2 = (Event**)v2;

    int r = xyorder( (*pe1)->eV, (*pe2)->eV );
    if (r == 0) {
    if ((*pe1)->type == (*pe2)->type) return 0;
    if ((*pe1)->type == LEFT) return -1;
    else return 1;
    } else
    return r;
}

// the EventQueue is a presorted array (no insertions needed)
class EventQueue {
    int      ne;               // total number of events in array
    int      ix;               // index of next event on queue
    Event*   Edata;            // array of all events
    Event**  Eq;               // sorted list of event pointers
public:
    EventQueue(const Polygon &P);    // constructor
    ~EventQueue(void)                // destructor
                 { delete[] Eq; 
                   delete[] Edata;
                 }

    Event*   next();                    // next event on queue
};

// EventQueue Routines
EventQueue::EventQueue( const Polygon &P )
{
    ix = 0;
    ne = 2 * P.n;          // 2 vertex events for each edge
    Edata = (Event*)new Event[ne];
    Eq = (Event**)new Event*[ne];
    for (int i=0; i < ne; i++)          // init Eq array pointers
        Eq[i] = &Edata[i];

    // Initialize event queue with edge segment endpoints
    for (int i=0; i < P.n; i++) {       // init data for edge i
        //fill Edata
        Eq[2*i]->edge = i;
        Eq[2*i+1]->edge = i;
        Eq[2*i]->eV   = &(P.V[i]);
        Eq[2*i]->otherEnd = Eq[2*i+1];
        Eq[2*i+1]->otherEnd = Eq[2*i];
        Eq[2*i]->seg = Eq[2*i+1]->seg = 0;
        //fill second point
        Point *pi1 = (i+1 < P.n) ? &(P.V[i+1]):&(P.V[0]);
        Eq[2*i+1]->eV = pi1;

        //set left or right
        if (xyorder( &P.V[i], pi1) < 0) { // determine type
            Eq[2*i]->type   = LEFT;
            Eq[2*i+1]->type = RIGHT;
        }
        else {
            Eq[2*i]->type   = RIGHT;
            Eq[2*i+1]->type = LEFT;
        }
    }

    // Sort Eq[] by increasing x and y
    ::qsort( Eq, ne, sizeof(Event*), E_compare );
}

Event* EventQueue::next()
{
    if (ix >= ne)
        return (Event*)0;
    else
        return Eq[ix++];
}
//===================================================================
 

// SweepLine Class

// SweepLine segment data struct
class SLseg : public Comparable<SLseg*> {
public:
    int      edge;         // polygon edge i is V[i] to V[i+1]
    const Point* lP;       // leftmost vertex point
    const Point* rP;       // rightmost vertex point
    SLseg*   above;        // segment above this one
    SLseg*   below;        // segment below this one

    SLseg() : Comparable<SLseg*>(this) {}
    ~SLseg() {}

    // return true if 'this' is below 'a'
    bool operator< (const SLseg& a) {
        //left point x smaller or eq a.leftPoint
        if (lP->x <= a.lP->x) {
            //a.lp is on left of edge
            double s = isLeft(lP, rP, a.lP);
            if (s != 0)
                return s > 0;  //a.lp on left return true(a bigger)
            else {             //a.lp.x on edge
                if (lP->x == rP->x)     // special case of vertical line.
                    return lP->y < a.lP->y;//a.lp.y > lp.y(a bigger)
                else
                    return isLeft(lP, rP, a.rP) > 0;//a.rp on left
            }
        } else {
            double s = isLeft(a.lP, a.rP, lP);
            if (s != 0)
                return s < 0;
            else
                return isLeft(a.lP, a.rP, rP) < 0;
        }
    }

    bool operator== (const SLseg& a) {
        return this->edge == a.edge;
    }

    cmp_t Compare(SLseg* key) const {
        return (*key == *this) ? EQ_CMP
                                : ((*key < *this) ? MIN_CMP : MAX_CMP);
    }

    // debug print functions declared virtual so they're not removed as dead code
    // and are available to the debugger
    virtual ostream &
    Print(ostream & os) const { 
       os << "(" << lP->x << ", " << lP->y << "; " << rP->x << ", " << rP->y << ")";
       return  os;
    }
    virtual void Print() {
        this->Print(cout) << "\n";
    }
};

ostream & operator<<(ostream & os, SLseg* item) {
    return item->Print(os);
}

typedef AvlNode<SLseg*> Tnode;

// the Sweep Line itself
class SweepLine {
    int      nv;           // number of vertices in polygon
    const Polygon* Pn;     // initial Polygon
    AvlTree<SLseg*> Tree;  // balanced binary tree
public:
    SweepLine(const Polygon &P)    // constructor
                 { nv = P.n; Pn = &P; }
    ~SweepLine(void)               // destructor
                 { cleanTree(Tree.myRoot); }

    void cleanTree(Tnode *p) {
        if (!p) return;
        delete p->Data();
        cleanTree(p->Subtree(AvlNode<SLseg*>::LEFT));
        cleanTree(p->Subtree(AvlNode<SLseg*>::RIGHT));
    }

    SLseg*   add( Event* );
    SLseg*   find( Event* );
    bool     intersect( SLseg*, SLseg* );
    void     remove( SLseg* );
};

SLseg* SweepLine::add( Event* E )
{
    // fill in SLseg element data
    SLseg* s = new SLseg;
    s->edge  = E->edge;
    E->seg = s;

    // if it is being added, then it must be a LEFT edge event
    // but need to determine which endpoint is the left one
    Point* v1 = &(Pn->V[s->edge]);
    Point* eN = (s->edge+1 < Pn->n ? &(Pn->V[s->edge+1]):&(Pn->V[0]));
    Point* v2 = eN;
    if (xyorder( v1, v2) < 0) { // determine which is leftmost
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
    return s;
}

void SweepLine::remove( SLseg* s )
{
    // remove the node from the balanced binary tree
    Tnode* nd = Tree.Search(s);
    if (nd == (Tnode*)0) {
        const char* m = "simple_Polygon internal error:  attempt to remove segment not in tree";
        fprintf(stderr, "%s\n", m);
        fprintf(stderr, "segment: (%g, %g) --> (%g, %g)\n",
                s->lP->x, s->lP->y, s->rP->x, s->rP->y);
        Tree.DumpTree();
        throw runtime_error(m);
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
bool SweepLine::intersect( SLseg* s1, SLseg* s2)
{
    if (s1 == (SLseg*)0 || s2 == (SLseg*)0)
        return false;      // no intersect if either segment doesn't exist

    // check for consecutive edges in polygon
    int e1 = s1->edge;
    int e2 = s2->edge;
    if ((((e1+1) % nv) == e2) || (e1 == ((e2+1) % nv)))
        return false;      // no non-simple intersect since consecutive

    // test for existence of an intersect point
    double lsign, rsign;
    lsign = isLeft(s1->lP, s1->rP, s2->lP);    // s2 left point sign
    rsign = isLeft(s1->lP, s1->rP, s2->rP);    // s2 right point sign
    if (lsign * rsign > 0) // s2 endpoints have same sign relative to s1
        return false;      // => on same side => no intersect is possible

    lsign = isLeft(s2->lP, s2->rP, s1->lP);    // s1 left point sign
    rsign = isLeft(s2->lP, s2->rP, s1->rP);    // s1 right point sign
    if (lsign * rsign > 0) // s1 endpoints have same sign relative to s2
        return false;      // => on same side => no intersect is possible

    // the segments s1 and s2 straddle each other
    return true;           // => an intersect exists
}
//===================================================================
 

// simple_Polygon(): test if a Polygon P is simple or not
//     Input:  Pn = a polygon with n vertices V[]
//     Return: false = is NOT simple
//             true  = IS simple

bool simple_Polygon( const Polygon &Pn )
{
    EventQueue  Eq(Pn);
    SweepLine   SL(Pn);
    Event*      e;                 // the current event
    SLseg*      s;                 // the current SL segment

    // This loop processes all events in the sorted queue
    // Events are only left or right vertices since
    // No new events will be added (an intersect => Done)
    while ((e = Eq.next())) {      // while there are events
        if (e->type == LEFT) {     // process a left vertex
            s = SL.add(e);         // add it to the sweep line
            if (SL.intersect( s, s->above))
                return false;          // Pn is NOT simple
            if (SL.intersect( s, s->below))
                return false;          // Pn is NOT simple
        }
        else {                     // process a right vertex
            s = e->otherEnd->seg;
            if (SL.intersect( s->above, s->below))
                return true;          // Pn is NOT simple
            SL.remove(s);     // remove it from the sweep line
        }
    }
    return true;      // Pn is simple
}
//===================================================================
}
