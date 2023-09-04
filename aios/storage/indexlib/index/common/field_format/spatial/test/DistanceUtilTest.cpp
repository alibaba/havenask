#include "indexlib/index/common/field_format/spatial/DistanceUtil.h"

#include "indexlib/index/common/field_format/spatial/shape/Point.h"
#include "indexlib/index/common/field_format/spatial/shape/Rectangle.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class DistanceUtilTest : public TESTBASE
{
public:
    DistanceUtilTest() = default;
    ~DistanceUtilTest() = default;

public:
    void setUp() override;
    void tearDown() override;
};

void DistanceUtilTest::setUp() {}

void DistanceUtilTest::tearDown() {}

TEST_F(DistanceUtilTest, testUsage)
{
    auto InnerTestCalculateBoundingBoxFromPointDeg = [](const std::string& centerPointStr, double distDegree,
                                                        const std::string& rectangleStr) {
        auto point = Point::FromString(centerPointStr);
        ASSERT_TRUE(point);
        auto expectRectangle = Rectangle::FromString(rectangleStr);
        auto rectangle = DistanceUtil::CalculateBoundingBoxFromPointDeg(point->GetY(), point->GetX(), distDegree);
        ASSERT_EQ(expectRectangle->GetMinX(), rectangle->GetMinX());
        ASSERT_EQ(expectRectangle->GetMinY(), rectangle->GetMinY());
        ASSERT_EQ(expectRectangle->GetMaxX(), rectangle->GetMaxX());
        ASSERT_EQ(expectRectangle->GetMaxY(), rectangle->GetMaxY());
    };

    // test normal case
    InnerTestCalculateBoundingBoxFromPointDeg("0 0", 1.0, "-1 -1, 1 1");
    // test case in date line
    InnerTestCalculateBoundingBoxFromPointDeg("180 0", 1.0, "179 -1, -179 1");
    InnerTestCalculateBoundingBoxFromPointDeg("-180 0", 1.0, "179 -1, -179 1");
    // test case touch north pole
    InnerTestCalculateBoundingBoxFromPointDeg("0 46", 45.0, "-180 1, 180 90");
    // test case touch sourth pole
    InnerTestCalculateBoundingBoxFromPointDeg("179 -46", 45.0, "-180 -90, 180 -1");
    // test case pole in center
    InnerTestCalculateBoundingBoxFromPointDeg("0 -90", 45.0, "-180 -90, 180 -45");
}

} // namespace indexlib::index
