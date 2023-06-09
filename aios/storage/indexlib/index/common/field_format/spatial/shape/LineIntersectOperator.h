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

#pragma once

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/index/common/field_format/spatial/shape/Point.h"

namespace indexlib::index {

class LineIntersectOperator : private autil::NoCopyable
{
public:
    LineIntersectOperator() = default;
    ~LineIntersectOperator() = default;

public:
    enum class IntersectType { NO_INTERSECTION, POINT_INTERSECTION, COLLINEAR_INTERSECTION, CROSS_INTERSECTION };

    // used to tag shared vertics for rectangle or two lines
    struct IntersectVertics {
        IntersectVertics() : p1(false), p2(false), q1(false), q2(false) {}
        void Reset()
        {
            p1 = false;
            p2 = false;
            q1 = false;
            q2 = false;
        }
        uint8_t p1 : 1;
        uint8_t p2 : 1;
        uint8_t q1 : 1;
        uint8_t q2 : 1;
    };

public:
    static IntersectType ComputeIntersect(const Point& p1, const Point& p2, const Point& q1, const Point& q2,
                                          IntersectVertics& intersectVertics);

private:
    static IntersectType ComputeCollinearIntersect(const Point& p1, const Point& p2, const Point& q1, const Point& q2,
                                                   IntersectVertics& intersectVertics);

    static int SignOfDet2x2(double x1, double y1, double x2, double y2);

    static bool IntersectBoudingBox(const Point& p1, const Point& p2, const Point& q1, const Point& q2);

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
    static int OrientationIndex(const Point& p1, const Point& p2, const Point& q);

    static bool IntersectEnvelope(const Point& p1, const Point& p2, const Point& q);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
