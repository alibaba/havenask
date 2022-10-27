#ifndef __INDEXLIB_LINE_INTERSECT_OPERATOR_H
#define __INDEXLIB_LINE_INTERSECT_OPERATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/common/field_format/spatial/shape/point.h"

IE_NAMESPACE_BEGIN(common);

class LineIntersectOperator
{
public:
    LineIntersectOperator();
    ~LineIntersectOperator();

public:
    enum IntersectType
    {
        NO_INTERSECTION,
        POINT_INTERSECTION,
        COLLINEAR_INTERSECTION,
        CROSS_INTERSECTION
    };

     // used to tag shared vertics for rectangle or two lines
    struct IntersectVertics
    {
        IntersectVertics()
            : p1(false)
            , p2(false)
            , q1(false)
            , q2(false)
        {}
        void Reset()
            {
                p1 = false;
                p2 = false;
                q1 = false;
                q2 = false;
            }
        uint8_t p1:1;
        uint8_t p2:1;
        uint8_t q1:1;
        uint8_t q2:1;
    };
    
public:
    static IntersectType ComputeIntersect(const Point& p1, const Point& p2,
                                          const Point& q1, const Point& q2,
                                          IntersectVertics& intersectVertics);

private:
    static IntersectType ComputeCollinearIntersect(
        const Point& p1, const Point& p2,
        const Point& q1, const Point& q2,
        IntersectVertics& intersectVertics);

    static int SignOfDet2x2(double x1, double y1, double x2, double y2);

    static bool IntersectBoudingBox(const Point& p1, const Point& p2,
                                    const Point& q1, const Point& q2);

    /**
     * Returns the index of the direction of the point <code>q</code>
     * relative to a vector specified by <code>p1-p2</code>.
     *
     * @param p1 the origin point of the vector
     * @param p2 the final point of the vector
     * @param q the point to compute the direction to
     *
     * @return 1 if q is counter-clockwise (left) from p1-p2
     * @return -1 if q is clockwise (right) from p1-p2
     * @return 0 if q is collinear with p1-p2
     */
    static int OrientationIndex(const Point& p1, const Point& p2,
                                const Point& q);

    static bool IntersectEnvelope(const Point& p1, const Point& p2,
                                  const Point& q);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(LineIntersectOperator);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_LINE_INTERSECT_OPERATOR_H
