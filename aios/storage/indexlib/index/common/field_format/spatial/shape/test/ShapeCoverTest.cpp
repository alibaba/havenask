#include "indexlib/index/common/Types.h"
#include "indexlib/index/common/field_format/spatial/Cell.h"
#include "indexlib/index/common/field_format/spatial/geo_hash/GeoHashUtil.h"
#include "indexlib/index/common/field_format/spatial/shape/Line.h"
#include "indexlib/index/common/field_format/spatial/shape/Polygon.h"
#include "indexlib/index/common/field_format/spatial/shape/RectangleIntersectOperator.h"
#include "indexlib/index/common/field_format/spatial/shape/ShapeCoverer.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class ShapeCovererTest : public TESTBASE
{
public:
    ShapeCovererTest() = default;
    ~ShapeCovererTest() = default;
    void setUp() override {}
    void tearDown() override {}

private:
    void CheckCells(const std::string& cellsStr, const std::vector<dictkey_t>& cells);

    template <typename T>
    void CheckSearchCells(std::queue<Cell> searchCells, const T& shape);
};

template <typename T>
void ShapeCovererTest::CheckSearchCells(std::queue<Cell> searchCells, const T& shape)
{
    while (searchCells.size() > 0) {
        Cell curSearchCell = searchCells.front();
        searchCells.pop();
        Rectangle* rectangle = curSearchCell.GetCellRectangle();
        DisjointEdges edges;
        Shape::Relation relation = RectangleIntersectOperator::GetRelation(*rectangle, shape, edges);
        ASSERT_TRUE(relation == Shape::Relation::CONTAINS || relation == Shape::Relation::INTERSECTS);
        DisjointEdges& disjointEdges = curSearchCell.GetDisjointEdges();
        if (disjointEdges.HasDisjointEdge()) {
            const std::vector<Point>& points = shape.GetPoints();
            for (size_t i = 0; i < points.size() - 1; i++) {
                if (disjointEdges.IsDisjointEdge(i)) {
                    Line line;
                    line.AppendPoint(points[i]);
                    line.AppendPoint(points[i + 1]);
                    DisjointEdges edge;
                    auto relation = RectangleIntersectOperator::GetRelation(*rectangle, line, edge);
                    ASSERT_EQ(Shape::Relation::DISJOINTS, relation);
                }
            }
        }
    }
}

void ShapeCovererTest::CheckCells(const std::string& cellsStr, const std::vector<dictkey_t>& cells)
{
    std::vector<std::string> actualCells;
    for (size_t i = 0; i < cells.size(); i++) {
        actualCells.push_back(GeoHashUtil::HashToGeoStr(cells[i]));
    }
    std::string actualStr = autil::StringUtil::toString(actualCells, ",");
    ASSERT_EQ(cellsStr, actualStr);
}

TEST_F(ShapeCovererTest, TestSearchSubCells)
{
    std::vector<dictkey_t> coverCells;
    ShapeCoverer coverer(1, 11, true);
    auto polygon = Polygon::FromString("-135 0, -135 45, -90 45, -90 0, -135 0");
    std::queue<Cell> searchCells;
    searchCells.push(Cell(GeoHashUtil::ZERO_LEVEL_HASH_ID));
    for (size_t i = 0; i < 9; i++) {
        Cell curSearchCell = searchCells.front();
        std::queue<Cell> tmpCells;
        searchCells.swap(tmpCells);
        coverer.SearchSubCells(polygon, curSearchCell, searchCells, coverCells, 11);
        CheckSearchCells(searchCells, *polygon);
    }
    auto line = Line::FromString("-135 0, -135 45, -90 45, -90 0, -135 0");
    std::queue<Cell> tmpCells;
    searchCells.swap(tmpCells);
    searchCells.push(Cell(GeoHashUtil::ZERO_LEVEL_HASH_ID));
    for (size_t i = 0; i < 9; i++) {
        Cell curSearchCell = searchCells.front();
        std::queue<Cell> tmpCells;
        searchCells.swap(tmpCells);
        coverer.SearchSubCells(line, curSearchCell, searchCells, coverCells, 11);
        CheckSearchCells(searchCells, *line);
    }
}

TEST_F(ShapeCovererTest, TestSearchLimitByMaxSearchTerms)
{
    std::vector<dictkey_t> coverCells;
    ShapeCoverer coverer(1, 11, true);
    {
        // test normal case
        auto rectangle = std::make_shared<Rectangle>(-135.0001, -0.0001, -89.9999, 45.0001);
        auto coverCells = coverer.GetShapeCoveredCells(rectangle, 9, 2);
        CheckCells("9,2,3,6,8,b,c,d,f", coverCells);
    }

    {
        // test exceed max search terms: with cell's sub cells
        auto rectangle = std::make_shared<Rectangle>(-135.0001, -0.0001, -89.9999, 45.0001);
        // test reach detail level
        auto coverCells = coverer.GetShapeCoveredCells(rectangle, 10, 2);
        CheckCells("9,2z,3p,3r,3x,3z,6,8,b,c,d,f", coverCells);
        // test not reach detail level
        coverCells = coverer.GetShapeCoveredCells(rectangle, 10, 3);
        CheckCells("9,6,8,b,c,d,f,2z,3p,3r,3x,3z", coverCells);
    }

    {
        // test exceed max search terms: exceed when not reach index top level
        ShapeCoverer coverer(2, 11, true);
        auto rectangle = std::make_shared<Rectangle>(-135.0001, -0.0001, -89.9999, 45.0001);
        auto coverCells = coverer.GetShapeCoveredCells(rectangle, 9, 3);
        CheckCells("90,91,92,93,94,95,96,97,98,99,9b,9c,9d,9e,9f,9g,9h,9j,9k,9m,"
                   "9n,9p,9q,9r,9s,9t,9u,9v,9w,9x,9y,9z,2z,3p,3r,3x,3z,6p,8b,8c,"
                   "8f,8g,8u,8v,8y,8z,bb,c0,c2,c8,cb,d0,d1,d4,d5,dh,dj,dn,dp,f0",
                   coverCells);
    }

    {
        // test exceed max search terms: exceed, use original top index level
        ShapeCoverer coverer(1, 11, true);
        auto rectangle = std::make_shared<Rectangle>(-135.0001, -0.0001, -89.9999, 45.0001);
        auto coverCells = coverer.GetShapeCoveredCells(rectangle, 9, 3);
        CheckCells("9,2,3,6,8,b,c,d,f", coverCells);
    }
}

TEST_F(ShapeCovererTest, TestGetShapeCoveredCellsForRectangle)
{
    std::vector<dictkey_t> coverCells;
    ShapeCoverer coverer(1, 11, true);
    {
        // test rectangle is cell
        auto rectangle = std::make_shared<Rectangle>(-135, 0, -90, 45);
        coverCells = coverer.GetShapeCoveredCells(rectangle, 100, 1);
        CheckCells("2,3,6,8,9,b,c,d,f", coverCells);
    }

    {
        // test rectangle is cell in sourth
        auto rectangle = std::make_shared<Rectangle>(-135, -45, -90, 0);
        coverCells = coverer.GetShapeCoveredCells(rectangle, 100, 1);
        CheckCells("0,1,2,3,4,6,8,9,d", coverCells);
    }

    {
        // test rectangle in cell
        auto rectangle = std::make_shared<Rectangle>(-133, 1, -91, 44);
        coverCells = coverer.GetShapeCoveredCells(rectangle, 100, 1);
        CheckCells("9", coverCells);
    }

    {
        // test rectangle tangent with two cells
        auto rectangle = std::make_shared<Rectangle>(-135, 1, -91, 44);
        coverCells = coverer.GetShapeCoveredCells(rectangle, 100, 1);
        CheckCells("8,9", coverCells);
    }

    {
        // test on side of the world
        auto rectangle = std::make_shared<Rectangle>(-180, 45, -135, 90);
        coverCells = coverer.GetShapeCoveredCells(rectangle, 100, 1);
        CheckCells("8,9,b,c,x,z", coverCells);
    }

    {
        // test cross 180
        auto rectangle = std::make_shared<Rectangle>(135, 0, -135, 45);
        coverCells = coverer.GetShapeCoveredCells(rectangle, 100, 1);
        CheckCells("2,3,8,9,b,c,q,r,w,x,y,z", coverCells);
    }
}

TEST_F(ShapeCovererTest, TestGetShapeCoveredCellsForRectangleMultiLevel)
{
    std::vector<dictkey_t> coverCells;
    ShapeCoverer coverer(1, 11, true);
    {
        // test in 2 level: contains intersect
        auto rectangle = std::make_shared<Rectangle>(-135.0001, -0.0001, -89.9999, 45.0001);
        coverCells = coverer.GetShapeCoveredCells(rectangle, 100, 2);
        CheckCells("9,2z,3p,3r,3x,3z,6p,8b,8c,8f,8g,8u,8v,"
                   "8y,8z,bb,c0,c2,c8,cb,d0,d1,d4,d5,dh,dj,dn,dp,f0",
                   coverCells);
    }

    {
        // test in 2 level: with in intersect
        auto rectangle = std::make_shared<Rectangle>(-134.9999, 0.9999, -90.0001, 44.9999);
        coverCells = coverer.GetShapeCoveredCells(rectangle, 100, 2);
        CheckCells("90,91,92,93,94,95,96,97,98,99,9b,9c,9d,9e,9f,9g,9h,9j,"
                   "9k,9m,9n,9p,9q,9r,9s,9t,9u,9v,9w,9x,9y,9z",
                   coverCells);
    }
}

TEST_F(ShapeCovererTest, TestGetCoveredCellsForPoint)
{
    std::vector<dictkey_t> coverCells;
    ShapeCoverer coverer(1, 1, true);
    {
        auto point = std::make_shared<Point>(-180, 90);
        coverCells = coverer.GetPointCoveredCells(point, true);
        CheckCells("b", coverCells);
    }

    {
        auto point = std::make_shared<Point>(-180, 0);
        coverCells = coverer.GetPointCoveredCells(point, true);
        CheckCells("8", coverCells);
    }

    {
        auto point = std::make_shared<Point>(0, 90);
        coverCells = coverer.GetPointCoveredCells(point, true);
        CheckCells("u", coverCells);
    }

    ShapeCoverer coverer2(1, 2, true);
    {
        auto point = std::make_shared<Point>(-180, 90);
        coverCells = coverer2.GetPointCoveredCells(point, false);
        CheckCells("b,bp", coverCells);
    }

    {
        auto point = std::make_shared<Point>(-180, 0);
        coverCells = coverer2.GetPointCoveredCells(point, false);
        CheckCells("8,80", coverCells);
    }

    {
        auto point = std::make_shared<Point>(0, 90);
        coverCells = coverer2.GetPointCoveredCells(point, false);
        CheckCells("u,up", coverCells);
    }
}

TEST_F(ShapeCovererTest, TestGetShapeCoveredCellsForPolygon)
{
    std::vector<dictkey_t> coverCells;
    ShapeCoverer coverer(1, 11, true);
    {
        // test polygon is cell
        // PolygonPtr polygon(new Polygon(-135, 0, -90, 45));
        auto polygon = Polygon::FromString("-135 0, -135 45, -90 45, -90 0, -135 0");
        coverCells = coverer.GetShapeCoveredCells(polygon, 100, 1);
        CheckCells("2,3,6,8,9,b,c,d,f", coverCells);
    }

    {
        // test polygon is cell in sourth
        // PolygonPtr polygon(new Polygon(-135, -45, -90, 0));
        auto polygon = Polygon::FromString("-135 -45, -135 0, -90 0, -90 -45, -135 -45");
        coverCells = coverer.GetShapeCoveredCells(polygon, 100, 1);
        CheckCells("0,1,2,3,4,6,8,9,d", coverCells);
    }

    {
        // test polygon in cell
        // PolygonPtr polygon(new Polygon(-133, 1, -91, 44));
        auto polygon = Polygon::FromString("-133 1, -133 44, -91 44, -91 1, -133 1");
        coverCells = coverer.GetShapeCoveredCells(polygon, 100, 1);
        CheckCells("9", coverCells);
    }

    {
        // test polygon tangent with two cells
        // PolygonPtr polygon(new Polygon(-135, 1, -91, 44));
        auto polygon = Polygon::FromString("-135 1, -135 44, -91 44, -91 1, -135 1");
        coverCells = coverer.GetShapeCoveredCells(polygon, 100, 1);
        CheckCells("8,9", coverCells);
    }

    {
        // test on side of the world - behaves differently with rectangle
        // PolygonPtr polygon(new Polygon(-180, 45, -135, 90));
        auto polygon = Polygon::FromString("-180 45, -180 90, -135 90, -135 45, -180 45");
        coverCells = coverer.GetShapeCoveredCells(polygon, 100, 1);
        CheckCells("8,9,b,c", coverCells);
    }

    {
        // test cross 180 -> change to inside 180
        // PolygonPtr polygon(new Polygon(135, 0, -135, 45));
        auto polygon = Polygon::FromString("135 0, -135 0, -135 45, 135 45, 135 0");
        coverCells = coverer.GetShapeCoveredCells(polygon, 100, 1);
        // CheckCells("2,3,8,9,b,c,q,r,w,x,y,z", coverCells);
        CheckCells("2,3,6,7,8,9,b,c,d,e,f,g,k,m,q,r,s,t,u,v,w,x,y,z", coverCells);
    }
}

TEST_F(ShapeCovererTest, TestGetShapeCoveredCellsForPolygonMultiLevel)
{
    std::vector<dictkey_t> coverCells;
    ShapeCoverer coverer(1, 11, true);
    {
        // test in 2 level: contains intersect
        // PolygonPtr polygon(new Polygon(-135.0001, -0.0001, -89.9999, 45.0001));
        auto polygon = Polygon::FromString("-135.0001 -0.0001,"
                                           "-135.0001 45.0001,"
                                           "-89.9999 45.0001,"
                                           "-89.9999 -0.0001,"
                                           "-135.0001 -0.0001");
        coverCells = coverer.GetShapeCoveredCells(polygon, 100, 2);
        CheckCells("9,2z,3p,3r,3x,3z,6p,8b,8c,8f,8g,8u,8v,"
                   "8y,8z,bb,c0,c2,c8,cb,d0,d1,d4,d5,dh,dj,dn,dp,f0",
                   coverCells);
    }

    {
        // test in 2 level: with in intersect
        // PolygonPtr polygon(new Polygon(-134.9999, 0.9999, -90.0001, 44.9999));
        auto polygon = Polygon::FromString("-134.9999 0.9999,"
                                           "-134.9999 44.9999,"
                                           "-90.0001 44.9999,"
                                           "-90.0001 0.9999,"
                                           "-134.9999 0.9999");
        coverCells = coverer.GetShapeCoveredCells(polygon, 100, 2);
        CheckCells("90,91,92,93,94,95,96,97,98,99,9b,9c,9d,9e,9f,9g,9h,9j,"
                   "9k,9m,9n,9p,9q,9r,9s,9t,9u,9v,9w,9x,9y,9z",
                   coverCells);
    }
}

TEST_F(ShapeCovererTest, TestGetShapeCoveredCellsForLine)
{
    std::vector<dictkey_t> coverCells;
    ShapeCoverer coverer(1, 11, true);

    {
        auto line = Line::FromString("-150 60, -60 60, "
                                     "-60 -30, -150 -30, -150 30, -90 30");
        coverCells = coverer.GetShapeCoveredCells(line, 100, 1);
        CheckCells("2,3,6,8,9,b,c,d,f", coverCells);
    }

    {
        auto line = Line::FromString("-135 0, -135 45, -90 45, -90 0,-135 0");
        coverCells = coverer.GetShapeCoveredCells(line, 100, 1);
        CheckCells("2,3,6,8,9,b,c,d,f", coverCells);
    }

    {
        auto line = Line::FromString("-135 0, -90 45, -135 45, -90 0");
        coverCells = coverer.GetShapeCoveredCells(line, 100, 1);
        CheckCells("2,3,6,8,9,b,c,d,f", coverCells);
    }
}

TEST_F(ShapeCovererTest, TestGetShapeCoveredCellsForLineMultiLevel)
{
    {
        ShapeCoverer coverer(1, 2, false);
        auto line = Line::FromString("-135.0001 -0.0001, -135.0001 45.0001,"
                                     "-89.9999 45.0001, -89.9999 -0.0001, -135.0001 -0.0001,"
                                     "-135 0, -130 45, -125 0, -120 45, -115 0, -110 45,"
                                     "-105 0, -100 45, -95 0, -90 45");
        std::vector<dictkey_t> coverCells = coverer.GetShapeCoveredCells(line, 100, 2);
        CheckCells("2,3,6,8,9,b,c,d,f,2z,3p,3r,3x,3z,6p,8b,8c,8f,8g,8u,8v,8y,8z,"
                   "90,91,92,93,94,95,96,97,98,99,9b,9c,9d,9e,9f,9g,9h,9j,9k,9m,"
                   "9n,9p,9q,9r,9s,9t,9u,9v,9w,9x,9y,9z,bb,c0,c2,c8,cb,d0,d1,d4,"
                   "d5,dh,dj,dn,dp,f0",
                   coverCells);
    }
}

} // namespace indexlib::index
