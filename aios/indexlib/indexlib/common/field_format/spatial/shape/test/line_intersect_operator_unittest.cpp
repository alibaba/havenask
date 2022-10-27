#include "indexlib/common/field_format/spatial/shape/test/line_intersect_operator_unittest.h"

using namespace std;

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, LineIntersectOperatorTest);

LineIntersectOperatorTest::LineIntersectOperatorTest()
{
}

LineIntersectOperatorTest::~LineIntersectOperatorTest()
{
}

void LineIntersectOperatorTest::CaseSetUp()
{
}

void LineIntersectOperatorTest::CaseTearDown()
{
}

void LineIntersectOperatorTest::TestNoIntersection()
{
    // case1: two mirror line based on y=0
    {
        LineIntersectOperator::IntersectVertics intersectVertics;
        Point p1(-1.500, -1.500);
        Point p2(-1.000, 1.000);
        Point q1(1.500, -1.500);
        Point q2(1.000, 1.000);
        LineIntersectOperator::IntersectType intersectType =
            LineIntersectOperator::ComputeIntersect(p1, p2,
                    q1, q2, intersectVertics);
        ASSERT_EQ(LineIntersectOperator::NO_INTERSECTION, intersectType);
        ASSERT_FALSE(intersectVertics.p1);
        ASSERT_FALSE(intersectVertics.p2);
        ASSERT_FALSE(intersectVertics.q1);
        ASSERT_FALSE(intersectVertics.q2);
    }
    // case 2: share the same "real line"
    {
        LineIntersectOperator::IntersectVertics intersectVertics;
        Point p1(-1.500, -1.500);
        Point p2(-0.500, -0.500);
        Point q1(1.500, 1.500);
        Point q2(0.500, 0.500);
        LineIntersectOperator::IntersectType intersectType =
            LineIntersectOperator::ComputeIntersect(p1, p2,
                    q1, q2, intersectVertics);
        ASSERT_EQ(LineIntersectOperator::NO_INTERSECTION, intersectType);
        ASSERT_FALSE(intersectVertics.p1);
        ASSERT_FALSE(intersectVertics.p2);
        ASSERT_FALSE(intersectVertics.q1);
        ASSERT_FALSE(intersectVertics.q2);
    }
}

void LineIntersectOperatorTest::TestPointIntersection()
{
    // case1: p1 is the same with q1
    {
        LineIntersectOperator::IntersectVertics intersectVertics;
        Point p1(-1.500, -1.500);
        Point p2(-1.000, 1.000);
        Point q1(-1.500, -1.500);
        Point q2(1.000, 1.000);
        LineIntersectOperator::IntersectType intersectType =
            LineIntersectOperator::ComputeIntersect(p1, p2,
                    q1, q2, intersectVertics);
        ASSERT_EQ(LineIntersectOperator::POINT_INTERSECTION, intersectType);
        ASSERT_TRUE(intersectVertics.p1);
        ASSERT_FALSE(intersectVertics.p2);
        ASSERT_TRUE(intersectVertics.q1);
        ASSERT_FALSE(intersectVertics.q2);
    }
    // case2: q1 lies between p1 and p2
    {
        LineIntersectOperator::IntersectVertics intersectVertics;
        Point p1(-1.500, -1.500);
        Point p2(-1.000, 1.000);
        Point q1(-1.250, -0.250);
        Point q2(1.000, 1.000);
        LineIntersectOperator::IntersectType intersectType =
            LineIntersectOperator::ComputeIntersect(p1, p2,
                    q1, q2, intersectVertics);
        ASSERT_EQ(LineIntersectOperator::POINT_INTERSECTION, intersectType);
        ASSERT_FALSE(intersectVertics.p1);
        ASSERT_FALSE(intersectVertics.p2);
        ASSERT_TRUE(intersectVertics.q1);
        ASSERT_FALSE(intersectVertics.q2);
    }
    // case 3: p1 is the same with q1 and share the same "real line"
    {
        LineIntersectOperator::IntersectVertics intersectVertics;
        Point p1(-1.500, -1.500);
        Point p2(-0.500, -0.500);
        Point q1(-1.500, -1.500);
        Point q2(-2.500, -2.500);
        LineIntersectOperator::IntersectType intersectType =
            LineIntersectOperator::ComputeIntersect(p1, p2,
                    q1, q2, intersectVertics);
        ASSERT_EQ(LineIntersectOperator::POINT_INTERSECTION, intersectType);
        ASSERT_TRUE(intersectVertics.p1);
        ASSERT_FALSE(intersectVertics.p2);
        ASSERT_TRUE(intersectVertics.q1);
        ASSERT_FALSE(intersectVertics.q2);
    }
    // case 4: p1 is the same with q2 and share the same "real line"
    {
        LineIntersectOperator::IntersectVertics intersectVertics;
        Point p1(-1.500, -1.500);
        Point p2(-0.500, -0.500);
        Point q1(-2.500, -2.500);
        Point q2(-1.500, -1.500);
        LineIntersectOperator::IntersectType intersectType =
            LineIntersectOperator::ComputeIntersect(p1, p2,
                    q1, q2, intersectVertics);
        ASSERT_EQ(LineIntersectOperator::POINT_INTERSECTION, intersectType);
        ASSERT_TRUE(intersectVertics.p1);
        ASSERT_FALSE(intersectVertics.p2);
        ASSERT_FALSE(intersectVertics.q1);
        ASSERT_TRUE(intersectVertics.q2);
    }
}

void LineIntersectOperatorTest::TestCrossIntersection()
{
    LineIntersectOperator::IntersectVertics intersectVertics;
    Point p1(-1.500, -1.500);
    Point p2(1.500, 1.500);
    Point q1(-1.500, 1.500);
    Point q2(1.500, -1.500);
    LineIntersectOperator::IntersectType intersectType =
        LineIntersectOperator::ComputeIntersect(p1, p2,
                q1, q2, intersectVertics);
    ASSERT_EQ(LineIntersectOperator::CROSS_INTERSECTION, intersectType);
    ASSERT_FALSE(intersectVertics.p1);
    ASSERT_FALSE(intersectVertics.p2);
    ASSERT_FALSE(intersectVertics.q1);
    ASSERT_FALSE(intersectVertics.q2);
}

void LineIntersectOperatorTest::TestColineIntersection_Q1OnLeftOfP1()
{
    // case1: q1 on left of p1, q2 on left of p2
    {
        LineIntersectOperator::IntersectVertics intersectVertics;
        Point p1(-1.500, -1.500);
        Point p2(1.500, 1.500);
        Point q1(-1.600, -1.600);
        Point q2(1.000, 1.000);
        LineIntersectOperator::IntersectType intersectType =
            LineIntersectOperator::ComputeIntersect(p1, p2,
                    q1, q2, intersectVertics);
        ASSERT_EQ(LineIntersectOperator::COLLINEAR_INTERSECTION, intersectType);
        ASSERT_TRUE(intersectVertics.p1);
        ASSERT_FALSE(intersectVertics.p2);
        ASSERT_FALSE(intersectVertics.q1);
        ASSERT_TRUE(intersectVertics.q2);
    }
    // case2: q1 on left of p1, q2 is the same with p2
    {
        LineIntersectOperator::IntersectVertics intersectVertics;
        Point p1(-1.500, -1.500);
        Point p2(1.500, 1.500);
        Point q1(-1.600, -1.600);
        Point q2(1.500, 1.500);
        LineIntersectOperator::IntersectType intersectType =
            LineIntersectOperator::ComputeIntersect(p1, p2,
                    q1, q2, intersectVertics);
        ASSERT_EQ(LineIntersectOperator::COLLINEAR_INTERSECTION, intersectType);
        ASSERT_TRUE(intersectVertics.p1);
        ASSERT_TRUE(intersectVertics.p2);
        ASSERT_FALSE(intersectVertics.q1);
        ASSERT_TRUE(intersectVertics.q2);
    }
    // case3: q1 on left of p1, q2 on right of p2
    {
        LineIntersectOperator::IntersectVertics intersectVertics;
        Point p1(-1.500, -1.500);
        Point p2(1.500, 1.500);
        Point q1(-1.600, -1.600);
        Point q2(2.500, 2.500);
        LineIntersectOperator::IntersectType intersectType =
            LineIntersectOperator::ComputeIntersect(p1, p2,
                    q1, q2, intersectVertics);
        ASSERT_EQ(LineIntersectOperator::COLLINEAR_INTERSECTION, intersectType);
        ASSERT_TRUE(intersectVertics.p1);
        ASSERT_TRUE(intersectVertics.p2);
        ASSERT_FALSE(intersectVertics.q1);
        ASSERT_FALSE(intersectVertics.q2);
    }
}

void LineIntersectOperatorTest::TestColineIntersection_Q1IsSameWithP1()
{
    // case1: q1 is the same with p1, q2 on left of p2
    {
        LineIntersectOperator::IntersectVertics intersectVertics;
        Point p1(-1.500, -1.500);
        Point p2(1.500, 1.500);
        Point q1(-1.500, -1.500);
        Point q2(1.000, 1.000);
        LineIntersectOperator::IntersectType intersectType =
            LineIntersectOperator::ComputeIntersect(p1, p2,
                    q1, q2, intersectVertics);
        ASSERT_EQ(LineIntersectOperator::COLLINEAR_INTERSECTION, intersectType);
        ASSERT_TRUE(intersectVertics.p1);
        ASSERT_FALSE(intersectVertics.p2);
        ASSERT_TRUE(intersectVertics.q1);
        ASSERT_TRUE(intersectVertics.q2);
    }
    // case2: q1 is the same with p1, q2 is the same with p2
    {
        LineIntersectOperator::IntersectVertics intersectVertics;
        Point p1(-1.500, -1.500);
        Point p2(1.500, 1.500);
        Point q1(-1.500, -1.500);
        Point q2(1.500, 1.500);
        LineIntersectOperator::IntersectType intersectType =
            LineIntersectOperator::ComputeIntersect(p1, p2,
                    q1, q2, intersectVertics);
        ASSERT_EQ(LineIntersectOperator::COLLINEAR_INTERSECTION, intersectType);
        ASSERT_TRUE(intersectVertics.p1);
        ASSERT_TRUE(intersectVertics.p2);
        ASSERT_TRUE(intersectVertics.q1);
        ASSERT_TRUE(intersectVertics.q2);
    }
    // case3: q1 is the same with p1, q2 on right of p2
    {
        LineIntersectOperator::IntersectVertics intersectVertics;
        Point p1(-1.500, -1.500);
        Point p2(1.500, 1.500);
        Point q1(-1.500, -1.500);
        Point q2(2.500, 2.500);
        LineIntersectOperator::IntersectType intersectType =
            LineIntersectOperator::ComputeIntersect(p1, p2,
                    q1, q2, intersectVertics);
        ASSERT_EQ(LineIntersectOperator::COLLINEAR_INTERSECTION, intersectType);
        ASSERT_TRUE(intersectVertics.p1);
        ASSERT_TRUE(intersectVertics.p2);
        ASSERT_TRUE(intersectVertics.q1);
        ASSERT_FALSE(intersectVertics.q2);
    }
}

void LineIntersectOperatorTest::TestColineIntersection_Q1BetweenP1P2()
{
    // case1: q1 is between p1 and p2, q2 on left of p1
    {
        LineIntersectOperator::IntersectVertics intersectVertics;
        Point p1(-1.500, -1.500);
        Point p2(1.500, 1.500);
        Point q1(-1.100, -1.100);
        Point q2(-1.700, -1.700);
        LineIntersectOperator::IntersectType intersectType =
            LineIntersectOperator::ComputeIntersect(p1, p2,
                    q1, q2, intersectVertics);
        ASSERT_EQ(LineIntersectOperator::COLLINEAR_INTERSECTION, intersectType);
        ASSERT_TRUE(intersectVertics.p1);
        ASSERT_FALSE(intersectVertics.p2);
        ASSERT_TRUE(intersectVertics.q1);
        ASSERT_FALSE(intersectVertics.q2);
    }
    // case2: q1 is between p1 and p2, q2 is the same with p1
    {
        LineIntersectOperator::IntersectVertics intersectVertics;
        Point p1(-1.500, -1.500);
        Point p2(1.500, 1.500);
        Point q1(-1.100, -1.100);
        Point q2(-1.500, -1.500);
        LineIntersectOperator::IntersectType intersectType =
            LineIntersectOperator::ComputeIntersect(p1, p2,
                    q1, q2, intersectVertics);
        ASSERT_EQ(LineIntersectOperator::COLLINEAR_INTERSECTION, intersectType);
        ASSERT_TRUE(intersectVertics.p1);
        ASSERT_FALSE(intersectVertics.p2);
        ASSERT_TRUE(intersectVertics.q1);
        ASSERT_TRUE(intersectVertics.q2);
    }
    // case3: q1 is between p1 and p2, q2 is between p1 and q1
    {
        LineIntersectOperator::IntersectVertics intersectVertics;
        Point p1(-1.500, -1.500);
        Point p2(1.500, 1.500);
        Point q1(-1.100, -1.100);
        Point q2(-1.300, -1.300);
        LineIntersectOperator::IntersectType intersectType =
            LineIntersectOperator::ComputeIntersect(p1, p2,
                    q1, q2, intersectVertics);
        ASSERT_EQ(LineIntersectOperator::COLLINEAR_INTERSECTION, intersectType);
        ASSERT_FALSE(intersectVertics.p1);
        ASSERT_FALSE(intersectVertics.p2);
        ASSERT_TRUE(intersectVertics.q1);
        ASSERT_TRUE(intersectVertics.q2);
    }
    // case4: q1 is between p1 and p2, q2 is between q1 and p2
    {
        LineIntersectOperator::IntersectVertics intersectVertics;
        Point p1(-1.500, -1.500);
        Point p2(1.500, 1.500);
        Point q1(-1.100, -1.100);
        Point q2(1.300, 1.300);
        LineIntersectOperator::IntersectType intersectType =
            LineIntersectOperator::ComputeIntersect(p1, p2,
                    q1, q2, intersectVertics);
        ASSERT_EQ(LineIntersectOperator::COLLINEAR_INTERSECTION, intersectType);
        ASSERT_FALSE(intersectVertics.p1);
        ASSERT_FALSE(intersectVertics.p2);
        ASSERT_TRUE(intersectVertics.q1);
        ASSERT_TRUE(intersectVertics.q2);
    }
    // case5: q1 is between p1 and p2, q2 is the same with p2
    {
        LineIntersectOperator::IntersectVertics intersectVertics;
        Point p1(-1.500, -1.500);
        Point p2(1.500, 1.500);
        Point q1(-1.100, -1.100);
        Point q2(1.500, 1.500);
        LineIntersectOperator::IntersectType intersectType =
            LineIntersectOperator::ComputeIntersect(p1, p2,
                    q1, q2, intersectVertics);
        ASSERT_EQ(LineIntersectOperator::COLLINEAR_INTERSECTION, intersectType);
        ASSERT_FALSE(intersectVertics.p1);
        ASSERT_TRUE(intersectVertics.p2);
        ASSERT_TRUE(intersectVertics.q1);
        ASSERT_TRUE(intersectVertics.q2);
    }
    // case6: q1 is between p1 and p2, q2 is on right of p2
    {
        LineIntersectOperator::IntersectVertics intersectVertics;
        Point p1(-1.500, -1.500);
        Point p2(1.500, 1.500);
        Point q1(-1.100, -1.100);
        Point q2(1.700, 1.700);
        LineIntersectOperator::IntersectType intersectType =
            LineIntersectOperator::ComputeIntersect(p1, p2,
                    q1, q2, intersectVertics);
        ASSERT_EQ(LineIntersectOperator::COLLINEAR_INTERSECTION, intersectType);
        ASSERT_FALSE(intersectVertics.p1);
        ASSERT_TRUE(intersectVertics.p2);
        ASSERT_TRUE(intersectVertics.q1);
        ASSERT_FALSE(intersectVertics.q2);
    }
}

void LineIntersectOperatorTest::TestColineIntersection_Q1IsSameWithP2()
{
    // case1: q1 is the same with p2, q2 on left of p1
    {
        LineIntersectOperator::IntersectVertics intersectVertics;
        Point p1(-1.500, -1.500);
        Point p2(1.500, 1.500);
        Point q1(1.500, 1.500);
        Point q2(-2.000, -2.000);
        LineIntersectOperator::IntersectType intersectType =
            LineIntersectOperator::ComputeIntersect(p1, p2,
                    q1, q2, intersectVertics);
        ASSERT_EQ(LineIntersectOperator::COLLINEAR_INTERSECTION, intersectType);
        ASSERT_TRUE(intersectVertics.p1);
        ASSERT_TRUE(intersectVertics.p2);
        ASSERT_TRUE(intersectVertics.q1);
        ASSERT_FALSE(intersectVertics.q2);
    }
    // case2: q1 is the same with p2, q2 is the same with p1
    {
        LineIntersectOperator::IntersectVertics intersectVertics;
        Point p1(-1.500, -1.500);
        Point p2(1.500, 1.500);
        Point q1(1.500, 1.500);
        Point q2(-1.500, -1.500);
        LineIntersectOperator::IntersectType intersectType =
            LineIntersectOperator::ComputeIntersect(p1, p2,
                    q1, q2, intersectVertics);
        ASSERT_EQ(LineIntersectOperator::COLLINEAR_INTERSECTION, intersectType);
        ASSERT_TRUE(intersectVertics.p1);
        ASSERT_TRUE(intersectVertics.p2);
        ASSERT_TRUE(intersectVertics.q1);
        ASSERT_TRUE(intersectVertics.q2);
    }
    // case3: q1 is the same with p2, q2 is between p1 and p2
    {
        LineIntersectOperator::IntersectVertics intersectVertics;
        Point p1(-1.500, -1.500);
        Point p2(1.500, 1.500);
        Point q1(1.500, 1.500);
        Point q2(0.500, 0.500);
        LineIntersectOperator::IntersectType intersectType =
            LineIntersectOperator::ComputeIntersect(p1, p2,
                    q1, q2, intersectVertics);
        ASSERT_EQ(LineIntersectOperator::COLLINEAR_INTERSECTION, intersectType);
        ASSERT_FALSE(intersectVertics.p1);
        ASSERT_TRUE(intersectVertics.p2);
        ASSERT_TRUE(intersectVertics.q1);
        ASSERT_TRUE(intersectVertics.q2);
    }
}

void LineIntersectOperatorTest::TestColineIntersection_Q1OnRightOfP2()
{
    // case1: q1 on right of p2, q2 on left of p1
    {
        LineIntersectOperator::IntersectVertics intersectVertics;
        Point p1(-1.500, -1.500);
        Point p2(1.500, 1.500);
        Point q1(1.600, 1.600);
        Point q2(-2.000, -2.000);
        LineIntersectOperator::IntersectType intersectType =
            LineIntersectOperator::ComputeIntersect(p1, p2,
                    q1, q2, intersectVertics);
        ASSERT_EQ(LineIntersectOperator::COLLINEAR_INTERSECTION, intersectType);
        ASSERT_TRUE(intersectVertics.p1);
        ASSERT_TRUE(intersectVertics.p2);
        ASSERT_FALSE(intersectVertics.q1);
        ASSERT_FALSE(intersectVertics.q2);
    }
    // case2: q1 on right of p2, q2 is the same with p1
    {
        LineIntersectOperator::IntersectVertics intersectVertics;
        Point p1(-1.500, -1.500);
        Point p2(1.500, 1.500);
        Point q1(1.600, 1.600);
        Point q2(-1.500, -1.500);
        LineIntersectOperator::IntersectType intersectType =
            LineIntersectOperator::ComputeIntersect(p1, p2,
                    q1, q2, intersectVertics);
        ASSERT_EQ(LineIntersectOperator::COLLINEAR_INTERSECTION, intersectType);
        ASSERT_TRUE(intersectVertics.p1);
        ASSERT_TRUE(intersectVertics.p2);
        ASSERT_FALSE(intersectVertics.q1);
        ASSERT_TRUE(intersectVertics.q2);
    }
    // case3: q1 on right of p2, q2 is between p1 and p2
    {
        LineIntersectOperator::IntersectVertics intersectVertics;
        Point p1(-1.500, -1.500);
        Point p2(1.500, 1.500);
        Point q1(1.600, 1.600);
        Point q2(0.500, 0.500);
        LineIntersectOperator::IntersectType intersectType =
            LineIntersectOperator::ComputeIntersect(p1, p2,
                    q1, q2, intersectVertics);
        ASSERT_EQ(LineIntersectOperator::COLLINEAR_INTERSECTION, intersectType);
        ASSERT_FALSE(intersectVertics.p1);
        ASSERT_TRUE(intersectVertics.p2);
        ASSERT_FALSE(intersectVertics.q1);
        ASSERT_TRUE(intersectVertics.q2);
    }
}

IE_NAMESPACE_END(common);

