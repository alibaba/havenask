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
#include "indexlib/util/Avl.h"

namespace indexlib::index {
class Point;
class Event;
class SLseg;

using Tnode = indexlib::util::AvlNode<SLseg*>;

class SweepLine : private autil::NoCopyable
{
public:
    SweepLine(const std::vector<Point>& pointVec) // constructor
        : _verticeCnt(pointVec.size())
        , _pointVec(pointVec)
    {
    }

    ~SweepLine(void) // destructor
    {
        CleanTree(_tree.myRoot);
    }

    bool IsSelftIntersectedPolygon() { return !IsSimplePolygon(); }

private:
    SLseg* Add(Event*);
    void Remove(SLseg*);
    bool Intersect(SLseg*, SLseg*);
    bool IsSimplePolygon();
    void CleanTree(Tnode* p)
    {
        if (!p)
            return;
        delete p->Data();
        CleanTree(p->Subtree(indexlib::util::AvlNode<SLseg*>::LEFT));
        CleanTree(p->Subtree(indexlib::util::AvlNode<SLseg*>::RIGHT));
    }

private:
    const size_t _verticeCnt;              // number of vertices in polygon
    const std::vector<Point>& _pointVec;   // initial Polygon
    indexlib::util::AvlTree<SLseg*> _tree; // balanced binary tree

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
