#ifndef __INDEXLIB_SWEEP_LINE_H
#define __INDEXLIB_SWEEP_LINE_H

#include <tr1/memory>
#include <vector>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/util/avl.h"

IE_NAMESPACE_BEGIN(common);

class Point;
class Event;
class SLseg;

typedef util::AvlNode<SLseg*> Tnode;

// the Sweep Line itself
class SweepLine {
private:
    const size_t              nv;    // number of vertices in polygon
    const std::vector<Point>& Pn;    // initial Polygon
    util::AvlTree<SLseg*>           Tree;  // balanced binary tree

public:
    SweepLine(const std::vector<Point> &P)    // constructor
        : nv(P.size())
        , Pn(P)
    {
    }

    ~SweepLine(void)               // destructor
    {
        CleanTree(Tree.myRoot);
    }

    bool IsSelftIntersectedPolygon()
    {
        return !IsSimplePolygon();
    }

private:
    SLseg*   Add( Event* );
    // SLseg*   find( Event* );
    void     Remove( SLseg* );
    bool     Intersect( SLseg*, SLseg* );
    bool     IsSimplePolygon();

private:
    void CleanTree(Tnode *p) {
        if (!p) return;
        delete p->Data();
        CleanTree(p->Subtree(util::AvlNode<SLseg *>::LEFT));
        CleanTree(p->Subtree(util::AvlNode<SLseg *>::RIGHT));
    }

private:
    IE_LOG_DECLARE();

};

DEFINE_SHARED_PTR(SweepLine);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_SWEEP_LINE_H
