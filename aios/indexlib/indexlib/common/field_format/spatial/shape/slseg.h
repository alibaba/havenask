#ifndef __INDEXLIB_SLSEG_H
#define __INDEXLIB_SLSEG_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/common/field_format/spatial/shape/point.h"
#include "indexlib/util/comparable.h"

IE_NAMESPACE_BEGIN(common);

class SLseg;
class SweepLine;

// SweepLine segment data struct
class SLseg : public util::Comparable<SLseg*> {
private:
    int      edge;         // polygon edge i is V[i] to V[i+1]
    const Point* lP;       // leftmost vertex point
    const Point* rP;       // rightmost vertex point
    SLseg*   above;        // segment above this one
    SLseg*   below;        // segment below this one

public:
    friend class SweepLine;

public:
    SLseg() : Comparable<SLseg*>(this) {}
    ~SLseg() {}
    int key() { return edge; }
    // return true if 'this' is below 'a'
    bool operator< (const SLseg& a) {

        if (lP->GetX() <= a.lP->GetX()) {
            double s = IsLeft(lP, rP, a.lP);
            if (s != 0)
                return s > 0;
            else {
                if (lP->GetX() == rP->GetX())  // special case of vertical line.
                    return lP->GetY() < a.lP->GetY();
                else
                {
                    double ret = IsLeft(lP, rP, a.rP);
                    if (ret != 0)
                    {
                        return IsLeft(lP, rP, a.rP) > 0;
                    }
                    return edge < a.edge;
                }
            }
        } else {
            double s = IsLeft(a.lP, a.rP, lP);
            if (s != 0)
                return s < 0;
            else
            {
                double ret = IsLeft(a.lP, a.rP, rP);
                if (ret != 0)
                {
                    return IsLeft(a.lP, a.rP, rP) < 0;
                }
                return edge < a.edge;
            }

        }
    }

    bool operator== (const SLseg& a) {
        return this->edge == a.edge;
    }

    util::cmp_t Compare(SLseg* key) const {
        return (*key == *this) ? util::EQ_CMP
            : ((*key < *this) ? util::MIN_CMP : util::MAX_CMP);
    }

public:
    static double IsLeft(const Point *P0, const Point *P1, const Point *P2);
};

inline double SLseg::IsLeft(const Point *P0, const Point *P1, const Point *P2)
{
    return (P1->GetX() - P0->GetX())*(P2->GetY() - P0->GetY()) -
        (P2->GetX() - P0->GetX())*(P1->GetY() - P0->GetY());
}

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_SLSEG_H
