#ifndef __INDEXLIB_SHAPECOVERERTEST_H
#define __INDEXLIB_SHAPECOVERERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/field_format/spatial/shape/shape_coverer.h"
#include "indexlib/common/field_format/spatial/shape/polygon.h"
#include "indexlib/common/field_format/spatial/shape/rectangle_intersect_operator.h"
#include "indexlib/common/field_format/spatial/shape/line.h"

IE_NAMESPACE_BEGIN(common);

class ShapeCovererTest : public INDEXLIB_TESTBASE
{
public:
    ShapeCovererTest();
    ~ShapeCovererTest();

    DECLARE_CLASS_NAME(ShapeCovererTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestGetShapeCoveredCellsForRectangle();
    void TestGetShapeCoveredCellsForRectangleMultiLevel();
    void TestGetCoveredCellsForPoint();
    void TestSearchLimitByMaxSearchTerms();
    void TestGetShapeCoveredCellsForPolygon();
    void TestGetShapeCoveredCellsForPolygonMultiLevel();
    void TestGetShapeCoveredCellsForLine();
    void TestGetShapeCoveredCellsForLineMultiLevel();
    void TestSearchSubCells();
private:
    void CheckCells(const std::string& cellsStr, const std::vector<dictkey_t>& cells);
    template<typename T>
    void CheckSearchCells(std::queue<Cell> searchCells, const T& shape);
private:
    IE_LOG_DECLARE();
};
/////////////////////////////////////////////
template<typename T>
void ShapeCovererTest::CheckSearchCells(
        std::queue<Cell> searchCells, const T& shape)
{
    while(searchCells.size() > 0)
    {
        Cell curSearchCell = searchCells.front();
        searchCells.pop();
        Rectangle* rectangle = curSearchCell.GetCellRectangle();
        DisjointEdges edges;
        Shape::Relation relation =
            RectangleIntersectOperator::GetRelation(
                    *rectangle, shape, edges);
        ASSERT_TRUE(relation == Shape::CONTAINS || relation == Shape::INTERSECTS);
        DisjointEdges& disjointEdges = curSearchCell.GetDisjointEdges();
        if (disjointEdges.hasDisjointEdge())
        {
            const std::vector<Point>& points = shape.GetPoints();
            for (size_t i = 0; i < points.size() - 1; i++)
            {
                if (disjointEdges.isDisjointEdge(i))
                {
                    Line line;
                    line.AppendPoint(points[i]);
                    line.AppendPoint(points[i + 1]);
                    DisjointEdges edge;
                    auto relation = RectangleIntersectOperator::GetRelation(
                            *rectangle, line, edge);
                    ASSERT_EQ(Shape::DISJOINTS, relation);
                }
            }
        }
    }
}

INDEXLIB_UNIT_TEST_CASE(ShapeCovererTest, TestGetShapeCoveredCellsForRectangle);
INDEXLIB_UNIT_TEST_CASE(ShapeCovererTest, TestGetShapeCoveredCellsForRectangleMultiLevel);
INDEXLIB_UNIT_TEST_CASE(ShapeCovererTest, TestGetCoveredCellsForPoint);
INDEXLIB_UNIT_TEST_CASE(ShapeCovererTest, TestSearchLimitByMaxSearchTerms);
INDEXLIB_UNIT_TEST_CASE(ShapeCovererTest, TestGetShapeCoveredCellsForPolygon);
INDEXLIB_UNIT_TEST_CASE(ShapeCovererTest, TestGetShapeCoveredCellsForPolygonMultiLevel);
INDEXLIB_UNIT_TEST_CASE(ShapeCovererTest, TestGetShapeCoveredCellsForLine);
INDEXLIB_UNIT_TEST_CASE(ShapeCovererTest, TestGetShapeCoveredCellsForLineMultiLevel);
INDEXLIB_UNIT_TEST_CASE(ShapeCovererTest, TestSearchSubCells);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_SHAPECOVERERTEST_H
