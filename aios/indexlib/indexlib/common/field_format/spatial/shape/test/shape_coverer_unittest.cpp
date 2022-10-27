#include <autil/StringUtil.h>
#include "indexlib/common/field_format/spatial/shape/test/shape_coverer_unittest.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, ShapeCovererTest);

ShapeCovererTest::ShapeCovererTest()
{
}

ShapeCovererTest::~ShapeCovererTest()
{
}

void ShapeCovererTest::CaseSetUp()
{
}

void ShapeCovererTest::CaseTearDown()
{
}

void ShapeCovererTest::TestSearchSubCells()
{
    vector<dictkey_t> coverCells;
    ShapeCoverer coverer(1, 11, true);
    PolygonPtr polygon = Polygon::FromString("-135 0, -135 45, -90 45, -90 0, -135 0");
    std::queue<Cell> searchCells;
    searchCells.push(Cell(GeoHashUtil::ZERO_LEVEL_HASH_ID));
    for (size_t i = 0; i < 9; i++)
    {
        Cell curSearchCell = searchCells.front();
        std::queue<Cell> tmpCells;
        searchCells.swap(tmpCells);
        coverer.SearchSubCells(
                polygon, curSearchCell,
                searchCells, coverCells, 11);
        CheckSearchCells(searchCells, *polygon);
    }
    LinePtr line = Line::FromString("-135 0, -135 45, -90 45, -90 0, -135 0");
    std::queue<Cell> tmpCells;
    searchCells.swap(tmpCells);
    searchCells.push(Cell(GeoHashUtil::ZERO_LEVEL_HASH_ID));
    for (size_t i = 0; i < 9; i++)
    {
        Cell curSearchCell = searchCells.front();
        std::queue<Cell> tmpCells;
        searchCells.swap(tmpCells);
        coverer.SearchSubCells(
                line, curSearchCell,
                searchCells, coverCells, 11);
        CheckSearchCells(searchCells, *line);
    }
}

void ShapeCovererTest::TestSearchLimitByMaxSearchTerms()
{
    vector<dictkey_t> coverCells;
    ShapeCoverer coverer(1, 11, true);
    {
        //test normal case
        RectanglePtr rectangle(new Rectangle(-135.0001, -0.0001, -89.9999, 45.0001));
        coverer.GetShapeCoveredCells(rectangle, 9, 2, coverCells);
        CheckCells("9,2,3,6,8,b,c,d,f", coverCells);
    }

    {
        //test exceed max search terms: with cell's sub cells
        RectanglePtr rectangle(new Rectangle(-135.0001, -0.0001, -89.9999, 45.0001));
        //test reach detail level
        coverer.GetShapeCoveredCells(rectangle, 10, 2, coverCells);
        CheckCells("9,2z,3p,3r,3x,3z,6,8,b,c,d,f", coverCells);
        //test not reach detail level
        coverer.GetShapeCoveredCells(rectangle, 10, 3, coverCells);
        CheckCells("9,6,8,b,c,d,f,2z,3p,3r,3x,3z", coverCells);
    }

    {
        //test exceed max search terms: exceed when not reach index top level
        ShapeCoverer coverer(2, 11, true);
        RectanglePtr rectangle(new Rectangle(-135.0001, -0.0001, -89.9999, 45.0001));
        coverer.GetShapeCoveredCells(rectangle, 9, 3, coverCells);
        CheckCells("90,91,92,93,94,95,96,97,98,99,9b,9c,9d,9e,9f,9g,9h,9j,9k,9m,"
                   "9n,9p,9q,9r,9s,9t,9u,9v,9w,9x,9y,9z,2z,3p,3r,3x,3z,6p,8b,8c,"
                   "8f,8g,8u,8v,8y,8z,bb,c0,c2,c8,cb,d0,d1,d4,d5,dh,dj,dn,dp,f0",
                   coverCells);
    }

    {
        //test exceed max search terms: exceed, use original top index level
        ShapeCoverer coverer(1, 11, true);
        RectanglePtr rectangle(new Rectangle(-135.0001, -0.0001, -89.9999, 45.0001));
        coverer.GetShapeCoveredCells(rectangle, 9, 3, coverCells);
        CheckCells("9,2,3,6,8,b,c,d,f", coverCells);
    }
}

void ShapeCovererTest::TestGetShapeCoveredCellsForRectangle()
{
    vector<dictkey_t> coverCells;
    ShapeCoverer coverer(1, 11, true);
    {
        //test rectangle is cell
        RectanglePtr rectangle(new Rectangle(-135, 0, -90, 45));
        coverer.GetShapeCoveredCells(rectangle, 100, 1, coverCells);
        CheckCells("2,3,6,8,9,b,c,d,f", coverCells);
    }

    {
        //test rectangle is cell in sourth
        RectanglePtr rectangle(new Rectangle(-135, -45, -90, 0));
        coverer.GetShapeCoveredCells(rectangle, 100, 1, coverCells);
        CheckCells("0,1,2,3,4,6,8,9,d", coverCells);
    }

    {
        //test rectangle in cell
        RectanglePtr rectangle(new Rectangle(-133, 1, -91, 44));
        coverer.GetShapeCoveredCells(rectangle, 100, 1, coverCells);
        CheckCells("9", coverCells);
    }

    {
        //test rectangle tangent with two cells
        RectanglePtr rectangle(new Rectangle(-135, 1, -91, 44));
        coverer.GetShapeCoveredCells(rectangle, 100, 1, coverCells);
        CheckCells("8,9", coverCells);
    }
    
    {
        //test on side of the world
        RectanglePtr rectangle(new Rectangle(-180, 45, -135, 90));
        coverer.GetShapeCoveredCells(rectangle, 100, 1, coverCells);
        CheckCells("8,9,b,c,x,z", coverCells);
    }

    {
        //test cross 180
        RectanglePtr rectangle(new Rectangle(135, 0, -135, 45));
        coverer.GetShapeCoveredCells(rectangle, 100, 1, coverCells);
        CheckCells("2,3,8,9,b,c,q,r,w,x,y,z", coverCells);
    }
}

void ShapeCovererTest::TestGetShapeCoveredCellsForRectangleMultiLevel()
{
    vector<dictkey_t> coverCells;
    ShapeCoverer coverer(1, 11, true);
    {
        //test in 2 level: contains intersect
        RectanglePtr rectangle(new Rectangle(-135.0001, -0.0001, -89.9999, 45.0001));
        coverer.GetShapeCoveredCells(rectangle, 100, 2, coverCells);
        CheckCells("9,2z,3p,3r,3x,3z,6p,8b,8c,8f,8g,8u,8v,"
                   "8y,8z,bb,c0,c2,c8,cb,d0,d1,d4,d5,dh,dj,dn,dp,f0", coverCells);
    }

    {
        //test in 2 level: with in intersect
        RectanglePtr rectangle(new Rectangle(-134.9999, 0.9999, -90.0001, 44.9999));
        coverer.GetShapeCoveredCells(rectangle, 100, 2, coverCells);
        CheckCells("90,91,92,93,94,95,96,97,98,99,9b,9c,9d,9e,9f,9g,9h,9j,"
                   "9k,9m,9n,9p,9q,9r,9s,9t,9u,9v,9w,9x,9y,9z" , coverCells);
    }
}

void ShapeCovererTest::TestGetCoveredCellsForPoint()
{
    vector<dictkey_t> coverCells;
    ShapeCoverer coverer(1, 1, true);
    {
        PointPtr point(new Point(-180,90));
        coverer.GetPointCoveredCells(point, coverCells, true);
        CheckCells("b", coverCells);
    }

    {
        PointPtr point(new Point(-180,0));
        coverer.GetPointCoveredCells(point, coverCells, true);
        CheckCells("8", coverCells);
    }

    {
        PointPtr point(new Point(0,90));
        coverer.GetPointCoveredCells(point, coverCells, true);
        CheckCells("u", coverCells);
    }

    ShapeCoverer coverer2(1, 2, true);
    {
        PointPtr point(new Point(-180,90));
        coverer2.GetPointCoveredCells(point, coverCells, false);
        CheckCells("b,bp", coverCells);
    }

    {
        PointPtr point(new Point(-180,0));
        coverer2.GetPointCoveredCells(point, coverCells, false);
        CheckCells("8,80", coverCells);
    }

    {
        PointPtr point(new Point(0,90));
        coverer2.GetPointCoveredCells(point, coverCells, false);
        CheckCells("u,up", coverCells);
    }

}

void ShapeCovererTest::CheckCells(const string& cellsStr, const vector<dictkey_t>& cells)
{
    vector<string> actualCells;
    for (size_t i = 0; i < cells.size(); i++)
    {
        actualCells.push_back(GeoHashUtil::HashToGeoStr(cells[i]));
    }
    string actualStr = StringUtil::toString(actualCells, ",");
    ASSERT_EQ(cellsStr, actualStr);
}

void ShapeCovererTest::TestGetShapeCoveredCellsForPolygon()
{
    vector<dictkey_t> coverCells;
    ShapeCoverer coverer(1, 11, true);
    {
        //test polygon is cell
        //PolygonPtr polygon(new Polygon(-135, 0, -90, 45));
        PolygonPtr polygon = Polygon::FromString("-135 0, -135 45, -90 45, -90 0, -135 0");
        coverer.GetShapeCoveredCells(polygon, 100, 1, coverCells);
        CheckCells("2,3,6,8,9,b,c,d,f", coverCells);
    }

    {
        //test polygon is cell in sourth
        //PolygonPtr polygon(new Polygon(-135, -45, -90, 0));
        PolygonPtr polygon = Polygon::FromString("-135 -45, -135 0, -90 0, -90 -45, -135 -45");
        coverer.GetShapeCoveredCells(polygon, 100, 1, coverCells);
        CheckCells("0,1,2,3,4,6,8,9,d", coverCells);
    }

    {
        //test polygon in cell
        //PolygonPtr polygon(new Polygon(-133, 1, -91, 44));
        PolygonPtr polygon = Polygon::FromString("-133 1, -133 44, -91 44, -91 1, -133 1");
        coverer.GetShapeCoveredCells(polygon, 100, 1, coverCells);
        CheckCells("9", coverCells);
    }

    {
        //test polygon tangent with two cells
        //PolygonPtr polygon(new Polygon(-135, 1, -91, 44));
        PolygonPtr polygon = Polygon::FromString("-135 1, -135 44, -91 44, -91 1, -135 1");
        coverer.GetShapeCoveredCells(polygon, 100, 1, coverCells);
        CheckCells("8,9", coverCells);
    }
    
    {
        //test on side of the world - behaves differently with rectangle
        //PolygonPtr polygon(new Polygon(-180, 45, -135, 90));
        PolygonPtr polygon = Polygon::FromString("-180 45, -180 90, -135 90, -135 45, -180 45");
        coverer.GetShapeCoveredCells(polygon, 100, 1, coverCells);
        CheckCells("8,9,b,c", coverCells);
    }

    {
        //test cross 180 -> change to inside 180
        //PolygonPtr polygon(new Polygon(135, 0, -135, 45));
        PolygonPtr polygon = Polygon::FromString("135 0, -135 0, -135 45, 135 45, 135 0");
        coverer.GetShapeCoveredCells(polygon, 100, 1, coverCells);
        // CheckCells("2,3,8,9,b,c,q,r,w,x,y,z", coverCells);
        CheckCells("2,3,6,7,8,9,b,c,d,e,f,g,k,m,q,r,s,t,u,v,w,x,y,z", coverCells);
    }
}

void ShapeCovererTest::TestGetShapeCoveredCellsForPolygonMultiLevel()
{
    vector<dictkey_t> coverCells;
    ShapeCoverer coverer(1, 11, true);
    {
        //test in 2 level: contains intersect
        //PolygonPtr polygon(new Polygon(-135.0001, -0.0001, -89.9999, 45.0001));
        PolygonPtr polygon = Polygon::FromString("-135.0001 -0.0001,"
                                                 "-135.0001 45.0001,"
                                                 "-89.9999 45.0001,"
                                                 "-89.9999 -0.0001,"
                                                 "-135.0001 -0.0001");
        coverer.GetShapeCoveredCells(polygon, 100, 2, coverCells);
        CheckCells("9,2z,3p,3r,3x,3z,6p,8b,8c,8f,8g,8u,8v,"
                   "8y,8z,bb,c0,c2,c8,cb,d0,d1,d4,d5,dh,dj,dn,dp,f0", coverCells);
    }

    {
        //test in 2 level: with in intersect
        //PolygonPtr polygon(new Polygon(-134.9999, 0.9999, -90.0001, 44.9999));
        PolygonPtr polygon = Polygon::FromString("-134.9999 0.9999,"
                                                 "-134.9999 44.9999,"
                                                 "-90.0001 44.9999,"
                                                 "-90.0001 0.9999,"
                                                 "-134.9999 0.9999");
        coverer.GetShapeCoveredCells(polygon, 100, 2, coverCells);
        CheckCells("90,91,92,93,94,95,96,97,98,99,9b,9c,9d,9e,9f,9g,9h,9j,"
                   "9k,9m,9n,9p,9q,9r,9s,9t,9u,9v,9w,9x,9y,9z" , coverCells);
    }
}

void ShapeCovererTest::TestGetShapeCoveredCellsForLine()
{
    vector<dictkey_t> coverCells;
    ShapeCoverer coverer(1, 11, true);

    {
        LinePtr line = Line::FromString("-150 60, -60 60, "
                "-60 -30, -150 -30, -150 30, -90 30");
        coverer.GetShapeCoveredCells(line, 100, 1, coverCells);
        CheckCells("2,3,6,8,9,b,c,d,f", coverCells);
    }

    {
        LinePtr line = Line::FromString("-135 0, -135 45, -90 45, -90 0,-135 0");
        coverer.GetShapeCoveredCells(line, 100, 1, coverCells);
        CheckCells("2,3,6,8,9,b,c,d,f", coverCells);
    }

    {
        LinePtr line = Line::FromString("-135 0, -90 45, -135 45, -90 0");
        coverer.GetShapeCoveredCells(line, 100, 1, coverCells);
        CheckCells("2,3,6,8,9,b,c,d,f", coverCells);
    }
}

void ShapeCovererTest::TestGetShapeCoveredCellsForLineMultiLevel()
{
    {
        vector<dictkey_t> coverCells;
        ShapeCoverer coverer(1, 2, false);
        LinePtr line = Line::FromString("-135.0001 -0.0001, -135.0001 45.0001,"
                "-89.9999 45.0001, -89.9999 -0.0001, -135.0001 -0.0001,"
                "-135 0, -130 45, -125 0, -120 45, -115 0, -110 45,"
                "-105 0, -100 45, -95 0, -90 45");
        coverer.GetShapeCoveredCells(line, 100, 2, coverCells);
        CheckCells("2,3,6,8,9,b,c,d,f,2z,3p,3r,3x,3z,6p,8b,8c,8f,8g,8u,8v,8y,8z,"
                   "90,91,92,93,94,95,96,97,98,99,9b,9c,9d,9e,9f,9g,9h,9j,9k,9m,"
                   "9n,9p,9q,9r,9s,9t,9u,9v,9w,9x,9y,9z,bb,c0,c2,c8,cb,d0,d1,d4,"
                   "d5,dh,dj,dn,dp,f0", coverCells);
    }
}

IE_NAMESPACE_END(common);

