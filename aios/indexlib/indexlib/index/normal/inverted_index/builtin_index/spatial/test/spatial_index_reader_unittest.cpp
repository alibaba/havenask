#include "indexlib/index/normal/inverted_index/builtin_index/spatial/test/spatial_index_reader_unittest.h"
#include "indexlib/common/field_format/spatial/shape/shape.h"
#include "indexlib/common/field_format/spatial/geo_hash/geo_hash_util.h"

using namespace std;
IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, SpatialIndexReaderTest);

SpatialIndexReaderTest::SpatialIndexReaderTest()
{
}

SpatialIndexReaderTest::~SpatialIndexReaderTest()
{
}

void SpatialIndexReaderTest::CaseSetUp()
{
}

void SpatialIndexReaderTest::CaseTearDown()
{
}

void SpatialIndexReaderTest::TestSimpleProcess()
{

}

void SpatialIndexReaderTest::TestParseShape()
{
    SpatialIndexReader reader;
    ShapePtr shape;

    shape = reader.ParseShape("point(1 1)");
    ASSERT_EQ(Shape::POINT, shape->GetType());

    shape = reader.ParseShape("rectangle(1 1,2 2)");
    ASSERT_EQ(Shape::RECTANGLE, shape->GetType());

    shape = reader.ParseShape("circle(1 1,3)");
    ASSERT_EQ(Shape::CIRCLE, shape->GetType());
}

void SpatialIndexReaderTest::TestParseShapeFail()
{
    SpatialIndexReader reader;
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

IE_NAMESPACE_END(index);

