#include "indexlib/common/field_format/spatial/test/cell_unittest.h"
#include "indexlib/common/field_format/spatial/geo_hash/geo_hash_util.h"
#include "indexlib/common/field_format/spatial/geo_hash/geohash.h"
using namespace std;

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, CellTest);

CellTest::CellTest()
{
}

CellTest::~CellTest()
{
}

void CellTest::CaseSetUp()
{
}

void CellTest::CaseTearDown()
{
}

void CellTest::TestGetSubCells()
{
    Cell cell(GeoHashUtil::GeoStrToHash("bc"));
    cell.mDisjointEdges.setDisjointEdge(1);
    vector<Cell> subCells;
    cell.GetSubCells(subCells);
    ASSERT_EQ((size_t)32, subCells.size());

    const char* nodes = "0123456789bcdefghjkmnpqrstuvwxyz";
    for (size_t i = 0; i < strlen(nodes); i++)
    {
        uint64_t cellId = subCells[i].GetCellId();
        ASSERT_TRUE(subCells[i].mDisjointEdges.isDisjointEdge(1u));
        double minLat = 0;
        double minLon = 0;
        double maxLat = 0;
        double maxLon = 0;
        GeoHashUtil::GetGeoHashArea(cellId, minLon, minLat, maxLon, maxLat);
        ASSERT_EQ(subCells[i].mRectangle.GetMinX(), minLon);
        ASSERT_EQ(subCells[i].mRectangle.GetMinY(), minLat);
        ASSERT_EQ(subCells[i].mRectangle.GetMaxX(), maxLon);
        ASSERT_EQ(subCells[i].mRectangle.GetMaxY(), maxLat);
        string cellStr = GeoHashUtil::HashToGeoStr(cellId);
        string expectCellStr = string("bc").append(1, nodes[i]);
        ASSERT_EQ(expectCellStr, cellStr);
    }
}

void CellTest::TestGetCellRectangle()
{
    InnerTestGetCellRectangle("b",1);
    InnerTestGetCellRectangle("bc",2);
    InnerTestGetCellRectangle("0",1);
    InnerTestGetCellRectangle("02bcuvz",7);
}

void CellTest::InnerTestGetCellRectangle(const std::string& cellStr, int8_t level)
{
    Cell cell(GeoHashUtil::GeoStrToHash(cellStr));
    ASSERT_EQ(cellStr, GeoHashUtil::HashToGeoStr(cell.GetCellId()));
    Rectangle* rectangle = cell.GetCellRectangle();
    uint64_t cellId = cell.GetCellId();

    //the smallest point and side is in cell
    ASSERT_EQ(cellId, GeoHashUtil::Encode(
                    rectangle->GetMinX(), rectangle->GetMinY(), level));
    ASSERT_EQ(cellId, GeoHashUtil::Encode(
                    rectangle->GetMinX(),
                    rectangle->GetMinY() + 0.0001, level));
    ASSERT_EQ(cellId, GeoHashUtil::Encode(
                    rectangle->GetMinX() + 0.0001,
                    rectangle->GetMinY(), level));

    //test in cell
    ASSERT_EQ(cellId, GeoHashUtil::Encode(
                    rectangle->GetMaxX() - 0.0001,
                    rectangle->GetMaxY() - 0.0001, level));
    if (rectangle->GetMaxX() != 90.0 && rectangle->GetMaxY() != 180.0)
    {
        ASSERT_FALSE(cellId == GeoHashUtil::Encode(
                        rectangle->GetMaxX() + 0.0001,
                        rectangle->GetMaxY() + 0.0001, level));
    }

    if (rectangle->GetMinX() != -90.0 && rectangle->GetMinY() != -180.0)
    {
        ASSERT_FALSE(cellId == GeoHashUtil::Encode(
                        rectangle->GetMinX() - 0.0001,
                        rectangle->GetMinY() - 0.0001, level));   
    }

    //test max point and side not in cell
    ASSERT_FALSE(cellId == GeoHashUtil::Encode(
                    rectangle->GetMaxX(), rectangle->GetMaxY(), level));
}

IE_NAMESPACE_END(common);

