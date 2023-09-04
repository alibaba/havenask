#include "indexlib/index/common/field_format/spatial/shape/Point.h"

#include "unittest/unittest.h"

namespace indexlib::index {
class PointTest : public TESTBASE
{
    PointTest() = default;
    ~PointTest() = default;
    void setUp() override {}
    void tearDown() override {}
};

TEST_F(PointTest, TestSimpleProcess)
{
    {
        auto point = Point::FromString("180.0 90.0");
        ASSERT_TRUE(point);
        ASSERT_EQ((double)90.0, point->GetY());
        ASSERT_EQ((double)180.0, point->GetX());
    }

    {
        auto point = Point::FromString("-180.0 -90.0");
        ASSERT_TRUE(point);
        ASSERT_EQ((double)-90.0, point->GetY());
        ASSERT_EQ((double)-180.0, point->GetX());
    }

    {
        auto point = Point::FromString(" 1.0");
        ASSERT_FALSE(point);
    }

    {
        auto point = Point::FromString("A 1.0");
        ASSERT_FALSE(point);
    }

    {
        auto point = Point::FromString("1.0 180.1");
        ASSERT_FALSE(point);
    }
}

TEST_F(PointTest, TestFromString)
{
    auto point = Point::FromString("-10.0 90.0");
    ASSERT_TRUE(point);
    ASSERT_EQ("point(-10 90)", point->ToString());
}

} // namespace indexlib::index
