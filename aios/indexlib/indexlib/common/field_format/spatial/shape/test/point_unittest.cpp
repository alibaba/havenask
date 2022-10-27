#include "indexlib/common/field_format/spatial/shape/test/point_unittest.h"

using namespace std;

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, PointTest);

PointTest::PointTest()
{
}

PointTest::~PointTest()
{
}

void PointTest::CaseSetUp()
{
}

void PointTest::CaseTearDown()
{
}

void PointTest::TestSimpleProcess()
{
    {
        PointPtr point = Point::FromString("180.0 90.0");
        ASSERT_TRUE(point);
        ASSERT_EQ((double)90.0, point->GetY());
        ASSERT_EQ((double)180.0, point->GetX());
    }

    {
        PointPtr point = Point::FromString("-180.0 -90.0");
        ASSERT_TRUE(point);
        ASSERT_EQ((double)-90.0, point->GetY());
        ASSERT_EQ((double)-180.0, point->GetX());
    }


    {
        PointPtr point = Point::FromString(" 1.0");
        ASSERT_FALSE(point);
    }

    {
        PointPtr point = Point::FromString("A 1.0");
        ASSERT_FALSE(point);
    }

    {
        PointPtr point = Point::FromString("1.0 180.1");
        ASSERT_FALSE(point);
    }
}

void PointTest::TestFromString()
{
    PointPtr point = Point::FromString("-10.0 90.0");
    ASSERT_TRUE(point);
    ASSERT_EQ("point(-10 90)", point->ToString());
}

IE_NAMESPACE_END(common);

