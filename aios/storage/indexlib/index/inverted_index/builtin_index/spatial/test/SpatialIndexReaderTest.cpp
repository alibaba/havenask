#include "indexlib/index/inverted_index/builtin_index/spatial/SpatialIndexReader.h"

#include "indexlib/index/common/field_format/spatial/geo_hash/GeoHashUtil.h"
#include "indexlib/index/common/field_format/spatial/shape/Shape.h"
#include "unittest/unittest.h"

using namespace std;

namespace indexlib { namespace index {

class SpatialIndexReaderTest : public TESTBASE
{
public:
    SpatialIndexReaderTest();
    ~SpatialIndexReaderTest();

public:
    void setUp() override {}
    void tearDown() override {}
    void TestParseShape();
    void TestParseShapeFail();

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.index, SpatialIndexReaderTest);

SpatialIndexReaderTest::SpatialIndexReaderTest() {}

SpatialIndexReaderTest::~SpatialIndexReaderTest() {}

TEST_F(SpatialIndexReaderTest, TestParseShape)
{
    SpatialIndexReader reader(nullptr);
    auto shape = reader.ParseShape("point(1 1)");
    ASSERT_EQ(Shape::ShapeType::POINT, shape->GetType());

    shape = reader.ParseShape("rectangle(1 1,2 2)");
    ASSERT_EQ(Shape::ShapeType::RECTANGLE, shape->GetType());

    shape = reader.ParseShape("circle(1 1,3)");
    ASSERT_EQ(Shape::ShapeType::CIRCLE, shape->GetType());
}

void SpatialIndexReaderTest::TestParseShapeFail()
{
    SpatialIndexReader reader(nullptr);
    ASSERT_FALSE(reader.ParseShape(""));
    ASSERT_FALSE(reader.ParseShape("()"));
    ASSERT_FALSE(reader.ParseShape(")("));
    ASSERT_FALSE(reader.ParseShape("point"));
    ASSERT_FALSE(reader.ParseShape("circle"));
    ASSERT_FALSE(reader.ParseShape("rectangle"));
    ASSERT_FALSE(reader.ParseShape("(1 1)"));
    ASSERT_FALSE(reader.ParseShape("(1 1,1)"));
    ASSERT_FALSE(reader.ParseShape("point()"));
    ASSERT_FALSE(reader.ParseShape("rectangle[1 1,2 2]"));
    ASSERT_FALSE(reader.ParseShape("rectangle(1 1,2 2"));
    ASSERT_FALSE(reader.ParseShape("rectangle1 1,2 2)"));
    ASSERT_FALSE(reader.ParseShape("XXX(2 2)"));
    ASSERT_FALSE(reader.ParseShape("point (1 1)"));
    ASSERT_FALSE(reader.ParseShape("point(1 1) "));
    ASSERT_FALSE(reader.ParseShape("point((1 1)"));
    ASSERT_FALSE(reader.ParseShape("point(1 1))"));
    ASSERT_FALSE(reader.ParseShape("point)1 1)"));
    ASSERT_FALSE(reader.ParseShape("point(1 1("));
    ASSERT_TRUE(reader.ParseShape("point( 1 1)"));
    ASSERT_TRUE(reader.ParseShape("point(1 1 )"));
}

}} // namespace indexlib::index
