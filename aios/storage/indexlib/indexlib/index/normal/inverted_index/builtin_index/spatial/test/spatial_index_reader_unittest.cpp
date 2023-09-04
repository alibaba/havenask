#include "indexlib/index/normal/inverted_index/builtin_index/spatial/test/spatial_index_reader_unittest.h"

#include "indexlib/index/common/field_format/spatial/shape/Shape.h"

using namespace std;

namespace indexlib { namespace index { namespace legacy {
IE_LOG_SETUP(index, SpatialIndexReaderTest);

SpatialIndexReaderTest::SpatialIndexReaderTest() {}

SpatialIndexReaderTest::~SpatialIndexReaderTest() {}

void SpatialIndexReaderTest::CaseSetUp() {}

void SpatialIndexReaderTest::CaseTearDown() {}

void SpatialIndexReaderTest::TestSimpleProcess() {}

void SpatialIndexReaderTest::TestParseShape()
{
    SpatialIndexReader reader;
    std::shared_ptr<Shape> shape;

    shape = reader.ParseShape("point(1 1)");
    ASSERT_EQ(Shape::ShapeType::POINT, shape->GetType());

    shape = reader.ParseShape("rectangle(1 1,2 2)");
    ASSERT_EQ(Shape::ShapeType::RECTANGLE, shape->GetType());

    shape = reader.ParseShape("circle(1 1,3)");
    ASSERT_EQ(Shape::ShapeType::CIRCLE, shape->GetType());
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
}}} // namespace indexlib::index::legacy
