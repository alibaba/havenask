/*
 * Copyright (c) 2016 Vivid Solutions, and others.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License 2.0
 * and Eclipse Distribution License v. 1.0 which accompanies this distribution.
 * The Eclipse Public License is available at http://www.eclipse.org/legal/epl-v20.html
 * and the Eclipse Distribution License is available at
 *
 * http://www.eclipse.org/org/documents/edl-v10.php.
 */

/*
 * Eclipse Distribution License - v 1.0
 *
 * Copyright (c) 2007, Eclipse Foundation, Inc. and its licensors.
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   Redistributions of source code must retain the above copyright notice, this
 *   list of conditions and the following disclaimer.
 *
 *   Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 *
 *   Neither the name of the Eclipse Foundation, Inc. nor the names of its
 *   contributors may be used to endorse or promote products derived from this
 *   software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
 * ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include "indexlib/index/common/field_format/spatial/shape/LineIntersectOperator.h"

#include <cmath>

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, LineIntersectOperator);
LineIntersectOperator::IntersectType LineIntersectOperator::ComputeIntersect(const Point& p1, const Point& p2,
                                                                             const Point& q1, const Point& q2,
                                                                             IntersectVertics& intersectVertics)
{
    /**
     * reference to jts::algorithm::LineIntersector.computeIntersection(...)
     */

    // first try a fast test to see if the envelopes of the lines intersect
    if (!IntersectBoudingBox(p1, p2, q1, q2)) {
        return IntersectType::NO_INTERSECTION;
    }

    // for each endpoint, compute which side of the other segment it lies
    // if both endpoints lie on the same side of the other segment,
    // the segments do not intersect
    int Pq1 = OrientationIndex(p1, p2, q1);
    int Pq2 = OrientationIndex(p1, p2, q2);

    if ((Pq1 > 0 && Pq2 > 0) || (Pq1 < 0 && Pq2 < 0)) {
        return IntersectType::NO_INTERSECTION;
    }

    int Qp1 = OrientationIndex(q1, q2, p1);
    int Qp2 = OrientationIndex(q1, q2, p2);

    if ((Qp1 > 0 && Qp2 > 0) || (Qp1 < 0 && Qp2 < 0)) {
        return IntersectType::NO_INTERSECTION;
    }

    bool collinear = (Pq1 == 0 && Pq2 == 0 && Qp1 == 0 && Qp2 == 0);
    if (collinear) {
        return ComputeCollinearIntersect(p1, p2, q1, q2, intersectVertics);
    }

    if (Pq1 != 0 && Pq2 != 0 && Qp1 != 0 && Qp2 != 0) {
        return IntersectType::CROSS_INTERSECTION;
    }

    if (Pq1 == 0) {
        intersectVertics.q1 = true;
    }

    if (Pq2 == 0) {
        intersectVertics.q2 = true;
    }

    if (Qp1 == 0) {
        intersectVertics.p1 = true;
    }

    if (Qp2 == 0) {
        intersectVertics.p2 = true;
    }

    return IntersectType::POINT_INTERSECTION;
}

LineIntersectOperator::IntersectType
LineIntersectOperator::ComputeCollinearIntersect(const Point& p1, const Point& p2, const Point& q1, const Point& q2,
                                                 IntersectVertics& intersectVertics)
{
    bool p1q1p2 = IntersectEnvelope(p1, p2, q1);
    bool p1q2p2 = IntersectEnvelope(p1, p2, q2);
    bool q1p1q2 = IntersectEnvelope(q1, q2, p1);
    bool q1p2q2 = IntersectEnvelope(q1, q2, p2);

    intersectVertics.p1 = q1p1q2 ? true : false;
    intersectVertics.p2 = q1p2q2 ? true : false;
    intersectVertics.q1 = p1q1p2 ? true : false;
    intersectVertics.q2 = p1q2p2 ? true : false;

    if (p1q1p2 && p1q2p2) {
        return IntersectType::COLLINEAR_INTERSECTION;
    }
    if (q1p1q2 && q1p2q2) {
        return IntersectType::COLLINEAR_INTERSECTION;
    }
    if (p1q1p2 && q1p1q2) {
        return q1 == p1 ? IntersectType::POINT_INTERSECTION : IntersectType::COLLINEAR_INTERSECTION;
    }
    if (p1q1p2 && q1p2q2) {
        return q1 == p2 ? IntersectType::POINT_INTERSECTION : IntersectType::COLLINEAR_INTERSECTION;
    }
    if (p1q2p2 && q1p1q2) {
        return q2 == p1 ? IntersectType::POINT_INTERSECTION : IntersectType::COLLINEAR_INTERSECTION;
    }
    if (p1q2p2 && q1p2q2) {
        return q2 == p2 ? IntersectType::POINT_INTERSECTION : IntersectType::COLLINEAR_INTERSECTION;
    }
    return IntersectType::NO_INTERSECTION;
}

int LineIntersectOperator::SignOfDet2x2(double x1, double y1, double x2, double y2)
{
    /**
     * reference to jts::algorithm::RobustDeterminant.signOfDet2x2(...)
     */

    // returns -1 if the determinant is negative,
    // returns  1 if the determinant is positive,
    // retunrs  0 if the determinant is null.
    int sign = 1;
    double swap;
    double k;

    /*
     * testing null entries
     */
    if ((x1 == 0.0) || (y2 == 0.0)) {
        if ((y1 == 0.0) || (x2 == 0.0)) {
            return 0;
        } else if (y1 > 0) {
            if (x2 > 0) {
                return -sign;
            } else {
                return sign;
            }
        } else {
            if (x2 > 0) {
                return sign;
            } else {
                return -sign;
            }
        }
    }
    if ((y1 == 0.0) || (x2 == 0.0)) {
        if (y2 > 0) {
            if (x1 > 0) {
                return sign;
            } else {
                return -sign;
            }
        } else {
            if (x1 > 0) {
                return -sign;
            } else {
                return sign;
            }
        }
    }

    /*
     *  making y coordinates positive and permuting the entries
     *  so that y2 is the biggest one
     */
    if (0.0 < y1) {
        if (0.0 < y2) {
            if (y1 <= y2) {
                ;
            } else {
                sign = -sign;
                swap = x1;
                x1 = x2;
                x2 = swap;
                swap = y1;
                y1 = y2;
                y2 = swap;
            }
        } else {
            if (y1 <= -y2) {
                sign = -sign;
                x2 = -x2;
                y2 = -y2;
            } else {
                swap = x1;
                x1 = -x2;
                x2 = swap;
                swap = y1;
                y1 = -y2;
                y2 = swap;
            }
        }
    } else {
        if (0.0 < y2) {
            if (-y1 <= y2) {
                sign = -sign;
                x1 = -x1;
                y1 = -y1;
            } else {
                swap = -x1;
                x1 = x2;
                x2 = swap;
                swap = -y1;
                y1 = y2;
                y2 = swap;
            }
        } else {
            if (y1 >= y2) {
                x1 = -x1;
                y1 = -y1;
                x2 = -x2;
                y2 = -y2;
            } else {
                sign = -sign;
                swap = -x1;
                x1 = -x2;
                x2 = swap;
                swap = -y1;
                y1 = -y2;
                y2 = swap;
            }
        }
    }

    /*
     * making x coordinates positive
     */
    /*
     * if |x2|<|x1| one can conclude
     */
    if (0.0 < x1) {
        if (0.0 < x2) {
            if (x1 <= x2) {
                ;
            } else {
                return sign;
            }
        } else {
            return sign;
        }
    } else {
        if (0.0 < x2) {
            return -sign;
        } else {
            if (x1 >= x2) {
                sign = -sign;
                x1 = -x1;
                x2 = -x2;
            } else {
                return -sign;
            }
        }
    }

    /*
     * all entries strictly positive   x1 <= x2 and y1 <= y2
     */
    while (true) {
        k = std::floor(x2 / x1);
        x2 = x2 - k * x1;
        y2 = y2 - k * y1;

        /*
         * testing if R (new U2) is in U1 rectangle
         */
        if (y2 < 0.0) {
            return -sign;
        }
        if (y2 > y1) {
            return sign;
        }

        /*
         * finding R'
         */
        if (x1 > x2 + x2) {
            if (y1 < y2 + y2) {
                return sign;
            }
        } else {
            if (y1 > y2 + y2) {
                return -sign;
            } else {
                x2 = x1 - x2;
                y2 = y1 - y2;
                sign = -sign;
            }
        }
        if (y2 == 0.0) {
            if (x2 == 0.0) {
                return 0;
            } else {
                return -sign;
            }
        }
        if (x2 == 0.0) {
            return sign;
        }

        /*
         * exchange 1 and 2 role.
         */
        k = std::floor(x1 / x2);
        x1 = x1 - k * x2;
        y1 = y1 - k * y2;

        /*
         * testing if R (new U1) is in U2 rectangle
         */
        if (y1 < 0.0) {
            return sign;
        }
        if (y1 > y2) {
            return -sign;
        }

        /*
         * finding R'
         */
        if (x2 > x1 + x1) {
            if (y2 < y1 + y1) {
                return -sign;
            }
        } else {
            if (y2 > y1 + y1) {
                return sign;
            } else {
                x1 = x2 - x1;
                y1 = y2 - y1;
                sign = -sign;
            }
        }
        if (y1 == 0.0) {
            if (x1 == 0.0) {
                return 0;
            } else {
                return sign;
            }
        }
        if (x1 == 0.0) {
            return -sign;
        }
    }
}

bool LineIntersectOperator::IntersectBoudingBox(const Point& p1, const Point& p2, const Point& q1, const Point& q2)
{
    double minq = std::min(q1.GetX(), q2.GetX());
    double maxq = std::max(q1.GetX(), q2.GetX());
    double minp = std::min(p1.GetX(), p2.GetX());
    double maxp = std::max(p1.GetX(), p2.GetX());

    if (minp > maxq || maxp < minq) {
        return false;
    }

    minq = std::min(q1.GetY(), q2.GetY());
    maxq = std::max(q1.GetY(), q2.GetY());
    minp = std::min(p1.GetY(), p2.GetY());
    maxp = std::max(p1.GetY(), p2.GetY());

    if (minp > maxq || maxp < minq) {
        return false;
    }

    return true;
}

int LineIntersectOperator::OrientationIndex(const Point& p1, const Point& p2, const Point& q)
{
    // travelling along p1->p2, turn counter clockwise to get to q return 1,
    // travelling along p1->p2, turn clockwise to get to q return -1,
    // p1, p2 and q are colinear return 0.
    double dx1 = p2.GetX() - p1.GetX();
    double dy1 = p2.GetY() - p1.GetY();
    double dx2 = q.GetX() - p2.GetX();
    double dy2 = q.GetY() - p2.GetY();
    return SignOfDet2x2(dx1, dy1, dx2, dy2);
}

bool LineIntersectOperator::IntersectEnvelope(const Point& p1, const Point& p2, const Point& q)
{
    // OptimizeIt shows that Math#min and Math#max here are a bottleneck.
    // Replace with direct comparisons. [Jon Aquino]

    double qX = q.GetX(), qY = q.GetY();
    double p1X = p1.GetX(), p1Y = p1.GetY();
    double p2X = p2.GetX(), p2Y = p2.GetY();

    if (((qX >= (p1X < p2X ? p1X : p2X)) && (qX <= (p1X > p2X ? p1X : p2X))) &&
        ((qY >= (p1Y < p2Y ? p1Y : p2Y)) && (qY <= (p1Y > p2Y ? p1Y : p2Y)))) {
        return true;
    }
    return false;
}
} // namespace indexlib::index
