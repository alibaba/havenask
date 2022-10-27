#include "indexlib/common/field_format/spatial/test/distance_util_unittest.h"
#include "indexlib/common/field_format/spatial/shape/point.h"

using namespace std;

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, DistanceUtilTest);

DistanceUtilTest::DistanceUtilTest()
{
}

DistanceUtilTest::~DistanceUtilTest()
{
}

void DistanceUtilTest::CaseSetUp()
{
}

void DistanceUtilTest::CaseTearDown()
{
}

void DistanceUtilTest::TestCalculateBoundingBoxFromPointDeg()
{
    //test normal case
    InnerTestCalculateBoundingBoxFromPointDeg("0 0", 1.0, "-1 -1, 1 1");
    //test case in date line
    InnerTestCalculateBoundingBoxFromPointDeg("180 0", 1.0, "179 -1, -179 1");
    InnerTestCalculateBoundingBoxFromPointDeg("-180 0", 1.0, "179 -1, -179 1");
    //test case touch north pole
    InnerTestCalculateBoundingBoxFromPointDeg("0 46", 45.0, "-180 1, 180 90");
    //test case touch sourth pole
    InnerTestCalculateBoundingBoxFromPointDeg("179 -46", 45.0, "-180 -90, 180 -1");
    //test case pole in center
    InnerTestCalculateBoundingBoxFromPointDeg("0 -90", 45.0, "-180 -90, 180 -45");
}


void DistanceUtilTest::InnerTestCalculateBoundingBoxFromPointDeg(
        const string& centerPointStr, double distDegree, const string& rectangleStr)
{
    PointPtr point = Point::FromString(centerPointStr);
    ASSERT_TRUE(point);
    RectanglePtr expectRectangle = Rectangle::FromString(rectangleStr);
    RectanglePtr rectangle = DistanceUtil::CalculateBoundingBoxFromPointDeg(
            point->GetY(), point->GetX(), distDegree);
    ASSERT_EQ(expectRectangle->GetMinX(), rectangle->GetMinX());
    ASSERT_EQ(expectRectangle->GetMinY(), rectangle->GetMinY());
    ASSERT_EQ(expectRectangle->GetMaxX(), rectangle->GetMaxX());
    ASSERT_EQ(expectRectangle->GetMaxY(), rectangle->GetMaxY());
}

IE_NAMESPACE_END(common);

